package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ListenAddr string `yaml:"listen_addr"`
	TargetAddr string `yaml:"target_addr"`
	WASMDir    string `yaml:"wasm_dir"`
	LogLevel   string `yaml:"log_level"`
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