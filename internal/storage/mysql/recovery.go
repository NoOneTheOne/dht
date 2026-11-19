package mysql

import (
	"bufio"
	"bytes"
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
// spool. The spool holds one JSON batch per line: a line that does not decode
// (a torn tail, or damage mid-file) is moved to a damaged-lines file and
// reported, and replay continues with the next line, so one bad line can
// never block the spool forever.
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

	var temp, damagedFile *os.File
	var encoder *json.Encoder
	defer func() {
		if temp != nil {
			_ = temp.Close()
			_ = os.Remove(temp.Name())
		}
		if damagedFile != nil {
			_ = damagedFile.Close()
		}
	}()
	quarantine := func(line []byte) error {
		if damagedFile == nil {
			var err error
			if damagedFile, err = os.CreateTemp(filepath.Dir(s.path), ".dht-recovery-damaged-*"); err != nil {
				return fmt.Errorf("create damaged recovery lines file: %w", err)
			}
		}
		if _, err := damagedFile.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("preserve damaged recovery line: %w", err)
		}
		return nil
	}
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("read recovery spool: %w", readErr)
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			var batch RecoveryBatch
			if err := json.Unmarshal(line, &batch); err != nil {
				if err := quarantine(line); err != nil {
					return err
				}
			} else {
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
		}
		if readErr == io.EOF {
			break
		}
	}
	var damaged error
	if damagedFile != nil {
		if err := errors.Join(damagedFile.Sync(), damagedFile.Close()); err != nil {
			damagedFile = nil
			return fmt.Errorf("preserve damaged recovery lines: %w", err)
		}
		damaged = fmt.Errorf("undecodable recovery lines preserved in %s", damagedFile.Name())
		damagedFile = nil
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
