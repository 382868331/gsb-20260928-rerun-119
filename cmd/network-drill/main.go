package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"network-drill/scenario"
)

func main() {
	file := flag.String("file", "", "path to a JSON replay scenario")
	flag.Parse()
	if *file == "" {
		fmt.Fprintln(os.Stderr, "missing -file")
		os.Exit(2)
	}
	s, err := scenario.Load(*file)
	if err != nil {
		fatal(err)
	}
	result, err := s.Run()
	if err != nil {
		fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
