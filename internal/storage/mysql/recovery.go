package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type RecoveryBatch struct {
	ID    string `json:"id"`
	Items []Data `json:"items"`
}

type RecoveryStore struct {
	path string
	mu   sync.Mutex
}

func NewRecoveryStore(path string) *RecoveryStore {
	if path == "" {
		return nil
	}
	return &RecoveryStore{path: path}
}

// Pending reports whether the spool holds batches awaiting replay.
func (s *RecoveryStore) Pending() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Stat(s.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return info.Size() > 0, nil
}

// Rejected returns the store, next to this spool, for items MySQL rejects
// permanently. It is never replayed automatically; inspect it by hand.
func (s *RecoveryStore) Rejected() *RecoveryStore {
	return NewRecoveryStore(strings.TrimSuffix(s.path, ".jsonl") + ".rejected.jsonl")
}

func (s *RecoveryStore) Append(batch RecoveryBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("open recovery spool: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	rollback := func(cause error) error {
		return errors.Join(cause, file.Truncate(info.Size()), file.Sync())
	}
	if err := json.NewEncoder(file).Encode(batch); err != nil {
		return rollback(fmt.Errorf("write recovery spool: %w", err))
	}
	if err := file.Sync(); err != nil {
		return rollback(fmt.Errorf("sync recovery spool: %w", err))
	}
	return nil
}

// Replay writes spooled batches in order. It stops writing at the first
// failure, because the database is most likely still unavailable and every
// further attempt would only add another timeout to the writer's stall. That
// batch and all later ones are streamed, not buffered, into the replacement
// spool. A truncated tail is preserved in a backup and reported as an error.
func (s *RecoveryStore) Replay(write func(string, []Data) error) (result error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open recovery spool: %w", err)
	}
	defer file.Close()

	var temp *os.File
	var encoder *json.Encoder
	defer func() {
		if temp != nil {
			_ = temp.Close()
			_ = os.Remove(temp.Name())
		}
	}()
	var damaged error
	decoder := json.NewDecoder(file)
	for {
		var batch RecoveryBatch
		err := decoder.Decode(&batch)
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			name, backupErr := s.backupDamaged(file)
			if backupErr != nil {
				return backupErr
			}
			damaged = fmt.Errorf("incomplete recovery tail preserved in %s", name)
			break
		}
		if err != nil {
			return fmt.Errorf("decode recovery spool: %w", err)
		}
		if encoder == nil {
			if err := write(batch.ID, batch.Items); err == nil {
				continue
			}
			if temp, err = os.CreateTemp(filepath.Dir(s.path), ".dht-recovery-*"); err != nil {
				return fmt.Errorf("create recovery spool replacement: %w", err)
			}
			encoder = json.NewEncoder(temp)
		}
		if err := encoder.Encode(batch); err != nil {
			return fmt.Errorf("write recovery spool replacement: %w", err)
		}
	}
	if encoder == nil {
		if err := os.Remove(s.path); err != nil {
			return err
		}
		return damaged
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync recovery spool replacement: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close recovery spool replacement: %w", err)
	}
	name := temp.Name()
	temp = nil
	if err := os.Rename(name, s.path); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("replace recovery spool: %w", err)
	}
	return damaged
}

// backupDamaged copies the whole spool aside before its tail is dropped.
func (s *RecoveryStore) backupDamaged(file *os.File) (string, error) {
	backup, err := os.CreateTemp(filepath.Dir(s.path), ".dht-recovery-damaged-*")
	if err != nil {
		return "", err
	}
	if _, err = file.Seek(0, io.SeekStart); err == nil {
		_, err = io.Copy(backup, file)
	}
	if err = errors.Join(err, backup.Sync(), backup.Close()); err != nil {
		return "", err
	}
	return backup.Name(), nil
}

func (s *RecoveryStore) Remove(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	remaining := make([]RecoveryBatch, 0)
	decoder := json.NewDecoder(file)
	for {
		var batch RecoveryBatch
		err := decoder.Decode(&batch)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if batch.ID != id {
			remaining = append(remaining, batch)
		}
	}
	if len(remaining) == 0 {
		return os.Remove(s.path)
	}
	return s.rewrite(remaining)
}

func (s *RecoveryStore) rewrite(batches []RecoveryBatch) error {
	directory := filepath.Dir(s.path)
	temp, err := os.CreateTemp(directory, ".dht-recovery-*")
	if err != nil {
		return fmt.Errorf("create recovery spool replacement: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	encoder := json.NewEncoder(temp)
	for _, batch := range batches {
		if err := encoder.Encode(batch); err != nil {
			_ = temp.Close()
			return fmt.Errorf("write recovery spool replacement: %w", err)
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync recovery spool replacement: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close recovery spool replacement: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace recovery spool: %w", err)
	}
	return nil
}
