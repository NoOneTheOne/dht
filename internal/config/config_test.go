package config

import (
	"reflect"
	"testing"
)

func TestLoadFromEnvironment(t *testing.T) {
	env := map[string]string{
		EnvMySQLAddress:   " 10.0.0.201:3306 ",
		EnvMySQLUser:      "dht",
		EnvMySQLPassword:  " pass with spaces ",
		EnvMySQLDatabase:  "DHT",
		EnvMySQLTable:     "info",
		EnvBootstrapNodes: "router.bittorrent.com:6881, dht.transmissionbt.com:6881\n[2001:db8::1]:6881",
	}
	c := load(func(k string) string { return env[k] })
	if c.MySQL.Addresses != "10.0.0.201:3306" || c.MySQL.Username != "dht" || c.MySQL.Database != "DHT" || c.MySQL.Table != "info" {
		t.Fatalf("mysql settings: %+v", c.MySQL)
	}
	if c.MySQL.Password != " pass with spaces " {
		t.Fatalf("password must be kept verbatim, got %q", c.MySQL.Password)
	}
	want := []string{"router.bittorrent.com:6881", "dht.transmissionbt.com:6881", "[2001:db8::1]:6881"}
	if !reflect.DeepEqual(c.BootstrapNodes, want) {
		t.Fatalf("bootstrap nodes = %q, want %q", c.BootstrapNodes, want)
	}
	if missing := c.MissingMySQL(); len(missing) != 0 {
		t.Fatalf("unexpected missing variables %q", missing)
	}
}

func TestMissingMySQL(t *testing.T) {
	c := load(func(k string) string {
		if k == EnvMySQLUser {
			return "dht"
		}
		return ""
	})
	want := []string{EnvMySQLAddress, EnvMySQLDatabase, EnvMySQLTable} // Password is optional.
	if got := c.MissingMySQL(); !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %q, want %q", got, want)
	}
	if len(c.BootstrapNodes) != 0 {
		t.Fatal("empty list must parse to no nodes")
	}
}
