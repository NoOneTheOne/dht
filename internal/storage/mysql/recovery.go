package mysql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func (s *RecoveryStore) Replay(write func(string, []Data) error) error {
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

	remaining := make([]RecoveryBatch, 0)
	decoder := json.NewDecoder(file)
	for {
		var batch RecoveryBatch
		err := decoder.Decode(&batch)
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			backup, backupErr := os.CreateTemp(filepath.Dir(s.path), ".dht-recovery-damaged-*")
			if backupErr != nil {
				return backupErr
			}
			if _, backupErr = file.Seek(0, io.SeekStart); backupErr == nil {
				_, backupErr = io.Copy(backup, file)
			}
			backupErr = errors.Join(backupErr, backup.Sync(), backup.Close())
			if backupErr != nil {
				return backupErr
			}
			if err := s.rewrite(remaining); err != nil {
				return err
			}
			return fmt.Errorf("incomplete recovery tail preserved in %s", backup.Name())
		}
		if err != nil {
			return fmt.Errorf("decode recovery spool: %w", err)
		}
		if err := write(batch.ID, batch.Items); err != nil {
			remaining = append(remaining, batch)
		}
	}
	if len(remaining) == 0 {
		return os.Remove(s.path)
	}
	return s.rewrite(remaining)
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
