package database

import (
	"log"

	"github.com/spf13/viper"
)

func GetEnvironmentVariableOrPanic(key string) string {
	viper.SetConfigFile("environment.env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error while reading config file %s", err)
	}

	value, ok := viper.Get(key).(string)
	if !ok {
		log.Fatalf("FAILED LOADING: %s, wrong type", key)
	} else if value == "" {
		log.Fatalf("FAILED LOADING: %s, missing/empty value", key)
	}

	log.Println("SETTING LOADED -", key)
	return value
}

func GetEnvironmentVariable(key string) string {
	viper.SetConfigFile("environment.env")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error while reading config file %s", err)
	}

	value, ok := viper.Get(key).(string)
	if !ok || value == "" {
		value = ""
	}

	log.Println("SETTING LOADED -", key)
	return value
}

