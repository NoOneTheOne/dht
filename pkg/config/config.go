package config

import (
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage struct {
		Type string `yaml:"type"`
	}
	MySQL struct {
		Addresses string `yaml:"addresses"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Database  string `yaml:"database"`
		Table     string `yaml:"table"`
	}
	Elasticsearch struct {
		Addresses []string `yaml:"addresses"`
		Username  string   `yaml:"username"`
		Password  string   `yaml:"password"`
		Index     string   `yaml:"index"`
	}
	Tracker struct {
		List []string `yaml:"list"`
	}
}

var (
	cachedConfig *Config
	configOnce   sync.Once
)

func GetConfig() *Config {
	configOnce.Do(func() {
		c := Config{}

		configPath := "./config.yml"
		if p, ok := os.LookupEnv("CONFIG_FILE_PATH"); ok {
			configPath = p
		}

		configFile, err := os.ReadFile(configPath)
		if err != nil {
			log.Fatalf("failed to read config file %s: %v", configPath, err)
		}
		err = yaml.Unmarshal(configFile, &c)
		if err != nil {
			log.Fatalf("failed to parse config file: %v", err)
		}
		cachedConfig = &c
	})
	return cachedConfig
}
