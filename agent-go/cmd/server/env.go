package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// loadDotEnv loads the repository .env file when it is present. Values already
// provided by the process environment win, matching the usual dotenv behavior.
func loadDotEnv() error {
	workingDir, err := os.Getwd()
	if err != nil {
		return err
	}
	// Tests run with cmd/server as their working directory, while the service is
	// usually started from agent-go. Walk to the repository root so both find
	// the same root .env file.
	for dir := workingDir; ; dir = filepath.Dir(dir) {
		path := filepath.Join(dir, ".env")
		if _, err := os.Stat(path); err == nil {
			return loadDotEnvFile(path)
		} else if !os.IsNotExist(err) {
			return err
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
	}
}

func loadDotEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, found := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNumber)
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			if value[0] == '"' {
				value, err = strconv.Unquote(value)
				if err != nil {
					return fmt.Errorf("%s:%d: invalid quoted value: %w", path, lineNumber, err)
				}
			} else {
				value = value[1 : len(value)-1]
			}
		}
		if key == "MAIN_MODEL" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("%s:%d: set %s: %w", path, lineNumber, key, err)
			}
		} else if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("%s:%d: set %s: %w", path, lineNumber, key, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}
