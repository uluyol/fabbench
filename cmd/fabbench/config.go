package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/uluyol/fabbench/bench"
	"github.com/uluyol/fabbench/db"

	_ "github.com/uluyol/fabbench/db/cassandra"
)

type cmdConfig struct {
	DB struct {
		Name    string          `json:"name"`
		Options json.RawMessage `json:"options"`
	} `json:"db"`
	Workload bench.Config `json:"workload"`
}

func loadConfig(hosts []string, path string) (db.DB, *bench.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to open config: %v", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var allCfg cmdConfig
	if err := dec.Decode(&allCfg); err != nil {
		return nil, nil, fmt.Errorf("unable to decode config: %v", err)
	}

	db, err := db.Dial(allCfg.DB.Name, hosts, []byte(allCfg.DB.Options))
	if err != nil {
		return nil, nil, fmt.Errorf("error connecting to db: %v", err)
	}
	return db, &allCfg.Workload, nil
}
