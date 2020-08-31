package utils

import "os"

func EnvOrDefault(env, d string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return d
}
