package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestMySQLInitializationFailure(t *testing.T) {
	getDB := func() (*gorm.DB, error) {
		return nil, errors.New("synthetic private backend details")
	}
	health := gin.H{"status": "ok"}
	checkMySQLHealth(context.Background(), health, getDB)
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("unexpected health response: %v", health)
	}
	if _, ok := health["mysql_pool"]; ok {
		t.Fatal("failed initialization exposed pool stats")
	}
	if open, inUse, idle := mysqlStats(getDB); open != 0 || inUse != 0 || idle != 0 {
		t.Fatalf("failed initialization stats = %d, %d, %d", open, inUse, idle)
	}
}

func TestMySQLPoolAccessFailure(t *testing.T) {
	getDB := func() (*gorm.DB, error) {
		return &gorm.DB{Config: &gorm.Config{}}, nil
	}
	health := gin.H{"status": "ok"}
	checkMySQLHealth(context.Background(), health, getDB)
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("unexpected health response: %v", health)
	}
	if open, inUse, idle := mysqlStats(getDB); open != 0 || inUse != 0 || idle != 0 {
		t.Fatalf("pool access failure stats = %d, %d, %d", open, inUse, idle)
	}
}

func TestMySQLHealthPing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pingErr error
		status  string
		mysql   string
	}{
		{"success", nil, "degraded", "connected"},
		{"failure", errors.New("synthetic private ping details"), "unhealthy", "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &healthConn{pingErr: tc.pingErr}
			pool := sql.OpenDB(healthConnector{conn})
			t.Cleanup(func() { _ = pool.Close() })
			getDB := func() (*gorm.DB, error) {
				return &gorm.DB{Config: &gorm.Config{ConnPool: pool}}, nil
			}
			health := gin.H{"status": "degraded"}
			before := time.Now()
			checkMySQLHealth(context.Background(), health, getDB)
			if health["status"] != tc.status || health["mysql"] != tc.mysql {
				t.Fatalf("unexpected health response: %v", health)
			}
			if conn.pingContext == nil {
				t.Fatal("health did not call PingContext")
			}
			deadline, ok := conn.pingContext.Deadline()
			if !ok || deadline.Before(before.Add(time.Second)) || deadline.After(time.Now().Add(2*time.Second)) {
				t.Fatal("health ping must have a two-second deadline")
			}
			if !errors.Is(conn.pingContext.Err(), context.Canceled) {
				t.Fatal("health ping context was not canceled after use")
			}
			if tc.pingErr == nil {
				if _, ok := health["mysql_pool"]; !ok {
					t.Fatal("healthy connection omitted pool stats")
				}
				if open, inUse, idle := mysqlStats(getDB); open != 1 || inUse != 0 || idle != 1 {
					t.Fatalf("healthy pool stats = %d, %d, %d", open, inUse, idle)
				}
			} else if _, ok := health["mysql_pool"]; ok {
				t.Fatal("failed ping exposed pool stats")
			}
		})
	}
}

func TestMySQLHealthHonorsRequestCancellation(t *testing.T) {
	conn := &healthConn{}
	pool := sql.OpenDB(healthConnector{conn})
	t.Cleanup(func() { _ = pool.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	health := gin.H{"status": "ok"}
	checkMySQLHealth(ctx, health, func() (*gorm.DB, error) {
		return &gorm.DB{Config: &gorm.Config{ConnPool: pool}}, nil
	})
	if health["status"] != "unhealthy" || health["mysql"] != "unavailable" {
		t.Fatalf("canceled health response: %v", health)
	}
}

// Minimal database/sql fake: no external database or driver dependency.
type healthConnector struct{ conn *healthConn }

func (c healthConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c healthConnector) Driver() driver.Driver                        { return healthDriver{} }

type healthDriver struct{}

func (healthDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type healthConn struct {
	pingErr     error
	pingContext context.Context
}

func (c *healthConn) Ping(ctx context.Context) error {
	c.pingContext = ctx
	return c.pingErr
}
func (*healthConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (*healthConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func (*healthConn) Close() error                        { return nil }
