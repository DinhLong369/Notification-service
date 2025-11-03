package app

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config is a application configuration structure
type (
	AppConfig struct {
		Database   DatabaseConfig
		Logging    LoggingConfig
		ConfigFile string
	}

	KafkaConfig struct {
		Brokers []string
		GroupID string
	}
)

var (
	Logging  *LoggingConfig
	Database *DatabaseConfig
	Kafka    *KafkaConfig
)

func Setup() {

	if err := godotenv.Load(".env"); err != nil {
		fmt.Println("Error loading .env file:", err)
	}

	Http := &AppConfig{
		Database: DatabaseConfig{
			Driver:   os.Getenv("DB_DRIVER"),
			Host:     os.Getenv("DB_HOST"),
			Username: os.Getenv("DB_USER"),
			Password: os.Getenv("DB_PASSWORD"),
			DBName:   os.Getenv("DB_NAME"),
			Port:     getEnvAsInt("DB_PORT", 3306),
			Debug:    os.Getenv("DB_DEBUG") == "true",
		},
		Logging: LoggingConfig{
			Type:       os.Getenv("LOG_TYPE"),
			ServerName: os.Getenv("SERVER_NAME"),
		},
	}

	Http.Database.Setup()
	Http.Logging.Setup()

	Database = &Http.Database
	Logging = &Http.Logging
}

func Config(key string) string {
	return os.Getenv(key)
}

// Helper convert env -> int
func getEnvAsInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return i
}
