// Package config reads the service configuration from environment
// variables. Docker Compose passes them from .env; see .env.example.
package config

import (
	"os"
	"strings"
	"sync"

	"github.com/NoOneTheOne/dht/internal/logging"
)

// Environment variable names.
const (
	EnvMySQLAddress   = "DHT_MYSQL_ADDRESS"
	EnvMySQLUser      = "DHT_MYSQL_USER"
	EnvMySQLPassword  = "DHT_MYSQL_PASSWORD"
	EnvMySQLDatabase  = "DHT_MYSQL_DATABASE"
	EnvMySQLTable     = "DHT_MYSQL_TABLE"
	EnvBootstrapNodes = "DHT_BOOTSTRAP_NODES"
)

type Config struct {
	MySQL struct {
		Addresses string // host:port
		Username  string
		Password  string
		Database  string
		Table     string // created automatically
	}
	// BootstrapNodes are DHT routers as host:port, merged with built-in ones.
	BootstrapNodes []string
}

var (
	cachedConfig *Config
	configOnce   sync.Once
)

// GetConfig returns the configuration read from the environment once.
func GetConfig() *Config {
	configOnce.Do(func() {
		cachedConfig = load(os.Getenv)
	})
	return cachedConfig
}

func load(getenv func(string) string) *Config {
	c := &Config{}
	c.MySQL.Addresses = strings.TrimSpace(getenv(EnvMySQLAddress))
	c.MySQL.Username = strings.TrimSpace(getenv(EnvMySQLUser))
	c.MySQL.Password = getenv(EnvMySQLPassword) // Passwords may contain spaces.
	c.MySQL.Database = strings.TrimSpace(getenv(EnvMySQLDatabase))
	c.MySQL.Table = strings.TrimSpace(getenv(EnvMySQLTable))
	c.BootstrapNodes = splitList(getenv(EnvBootstrapNodes))
	return c
}

// splitList parses a comma- or whitespace-separated list.
func splitList(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}

// MissingMySQL returns the names of required MySQL variables that are empty.
// The password is optional.
func (c *Config) MissingMySQL() []string {
	var missing []string
	for _, v := range []struct{ name, value string }{
		{EnvMySQLAddress, c.MySQL.Addresses},
		{EnvMySQLUser, c.MySQL.Username},
		{EnvMySQLDatabase, c.MySQL.Database},
		{EnvMySQLTable, c.MySQL.Table},
	} {
		if v.value == "" {
			missing = append(missing, v.name)
		}
	}
	return missing
}

// RequireMySQL exits when a service that stores data starts without its
// MySQL settings, naming every missing variable.
func RequireMySQL(c *Config) {
	if missing := c.MissingMySQL(); len(missing) > 0 {
		logging.Fatalf("missing required environment variables: %s (see .env.example)", strings.Join(missing, ", "))
	}
}
