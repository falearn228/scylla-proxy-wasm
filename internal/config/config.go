package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type TLSConfig struct {
	Enabled           bool     `yaml:"enabled"`
	CertFile          string   `yaml:"cert_file"`
	KeyFile           string   `yaml:"key_file"`
	ClientCAFile      string   `yaml:"client_ca_file"`
	RequireClientCert bool     `yaml:"require_client_cert"`
	MinVersion        string   `yaml:"min_version"`
	CipherSuites      []string `yaml:"cipher_suites"`
}

type Config struct {
	ListenAddr           string    `yaml:"listen_addr"`
	TargetAddr           string    `yaml:"target_addr"`
	WASMDir              string    `yaml:"wasm_dir"`
	LogLevel             string    `yaml:"log_level"`
	WASMInstancePoolSize int       `yaml:"wasm_instance_pool_size"`
	TLS                  TLSConfig `yaml:"tls"`
}

func Load(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
