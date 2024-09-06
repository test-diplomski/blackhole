package model

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

var retry = [...]string{"linear", "exp"}

type Config struct {
	Content BlackHole `yaml:"blackhole"`
}

type BlackHole struct {
	Address        string            `yaml:"address"`
	Celestial      string            `yaml:"celestial"`
	Apollo         string            `yaml:"apollo"`
	Meridian       string            `yaml:"meridian"`
	DB             []string          `yaml:"db"`
	Queues         map[string]Queue  `yaml:"queue"`
	InstrumentConf map[string]string `yaml:"instrument"`
}

type FillInterval struct {
	Interval int    `yaml:"interval"`
	Rate     string `yaml:"rate"`
}

type Retry struct {
	Delay    string `yaml:"delay"`
	Doubling int    `yaml:"doubling"`
	Limit    int    `yaml:"limit"`
}

type Queue struct {
	Namespace  string       `yaml:"namespace"`
	TRetry     Retry        `yaml:"retry"`
	MaxWorkers int          `yaml:"maxWorkers"`
	Capacity   int64        `yaml:"capacity"`
	Tokens     int64        `yaml:"tokens"`
	Rate       FillInterval `yaml:"fillInterval"`
}

type BlackHoleConfig struct {
	Address        string
	Celestial      string
	Apollo         string
	Meridian       string
	DB             []string
	Opts           []*TaskOption
	InstrumentConf map[string]string
}

type TaskOption struct {
	TRetry     *Retry        `yaml:"retry"`
	Namespace  string        `yaml:"namespace"`
	Name       string        `yaml:"name"`
	MaxWorkers int           `yaml:"maxWorkers"`
	MaxQueued  int           `yaml:"maxQueued"`
	Capacity   int64         `yaml:"capacity"`
	Tokens     int64         `yaml:"tokens"`
	FillRate   *FillInterval `yaml:"fillInterval"`
}

func DetermineInterval(ft *FillInterval) time.Duration {
	if ft.Interval <= 0 { //TODO: for now, this should be implemented beter!!!!
		return time.Second
	}

	tm := time.Duration(ft.Interval)
	switch ft.Rate {
	case "s", "second":
		return tm * time.Second
	case "ms", "millisecond":
		return tm * time.Millisecond
	case "h", "hour":
		return tm * time.Second
	default:
		return time.Second
	}
}

func checkRetry(strategy string) string {
	for _, value := range retry {
		if strategy == value {
			return strategy
		}
	}
	return "linear" //TODO: This should be checked nad return some error
}

func configToOption(bcf *Config) *BlackHoleConfig {
	opts := []*TaskOption{}
	for name, q := range bcf.Content.Queues {
		to := &TaskOption{
			TRetry:     &q.TRetry,
			Name:       name,
			Namespace:  q.Namespace,
			MaxWorkers: q.MaxWorkers,
			MaxQueued:  q.MaxWorkers,
			Capacity:   q.Capacity,
			Tokens:     q.Tokens,
			FillRate:   &q.Rate,
		}
		opts = append(opts, to)
	}

	tretry := &Retry{
		Delay:    "linear",
		Doubling: 1,
		Limit:    500,
	}

	fr := &FillInterval{
		Interval: 1,
		Rate:     "s",
	}

	// add defaiult queue
	dtk := &TaskOption{
		TRetry:     tretry,
		Name:       "default",
		Namespace:  "default",
		MaxWorkers: 5,
		MaxQueued:  5,
		Capacity:   5,
		Tokens:     0,
		FillRate:   fr,
	}
	opts = append(opts, dtk)
	return &BlackHoleConfig{
		Address:        bcf.Content.Address,
		Celestial:      bcf.Content.Celestial,
		Apollo:         bcf.Content.Apollo,
		Meridian:       bcf.Content.Meridian,
		DB:             bcf.Content.DB,
		Opts:           opts,
		InstrumentConf: bcf.Content.InstrumentConf,
	}
}

func LoadConfig(n ...string) (*BlackHoleConfig, error) {
	path := ""
	if len(n) > 0 {
		path = n[0]
	}

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var f Config
	err = yaml.Unmarshal(yamlFile, &f)
	if err != nil {
		return nil, err
	}

	return configToOption(&f), nil
}
