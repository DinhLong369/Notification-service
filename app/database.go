package app

import (
	"fmt"
	"io"
	"log"
	"time"

	"core/internal/model"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const DBTimeout = 10 * time.Second

type DatabaseConfig struct {
	*gorm.DB
	Driver      string `env:"DB_DRIVER"`
	Host        string `env:"DB_HOST"`
	Username    string `env:"DB_USER"`
	Password    string `env:"DB_PASSWORD"`
	DBName      string `env:"DB_NAME"`
	Port        int    `env:"DB_PORT"`
	MaxIdleConn int    `env:"MAX_IDLE_CONN"`
	MaxOpenConn int    `env:"MAX_OPEN_CONN"`
	MaxLifetime int    `env:"MAX_LIFE_TIME_PER_CONN"`
	Debug       bool
}

// Setup kết nối DB và AutoMigrate model CMS
func (dbConf *DatabaseConfig) Setup() {

	// Force GORM logger to silent and discard output to prevent SQL logs
	// being printed to stdout. If you later need SQL logs, enable DB_DEBUG
	// and rework this section.
	newLogger := logger.New(
		log.New(io.Discard, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Silent,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbConf.Username, dbConf.Password, dbConf.Host, dbConf.Port, dbConf.DBName)

	db, err := gorm.Open(
		mysql.New(mysql.Config{
			DSN:               dsn,
			DefaultStringSize: 256,
		}),
		&gorm.Config{
			PrepareStmt: true,
			Logger:      newLogger,
		},
	)

	if err != nil {
		logrus.Fatal("Failed to connect to database:", err)
	}

	// db.Debug() would enable verbose logging; we control logging via the
	// logger configuration above. Avoid calling db.Debug() to prevent
	// accidentally printing SQL to stdout when not desired.

	sqlDB, err := db.DB()
	if err != nil {
		logrus.Fatal("Failed to get sql.DB from gorm:", err)
	}

	// Set pool từ env
	if dbConf.MaxOpenConn > 0 {
		sqlDB.SetMaxOpenConns(dbConf.MaxOpenConn)
	} else {
		sqlDB.SetMaxOpenConns(20) // default
	}

	if dbConf.MaxIdleConn > 0 {
		sqlDB.SetMaxIdleConns(dbConf.MaxIdleConn)
	} else {
		sqlDB.SetMaxIdleConns(10) // default
	}

	if dbConf.MaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(dbConf.MaxLifetime) * time.Minute)
	} else {
		sqlDB.SetConnMaxLifetime(time.Hour)
	}

	dbConf.DB = db

	models := []interface{}{
		&model.Notification{},
		&model.OutboxEvent{},
	}

	for _, m := range models {
		if err := db.AutoMigrate(m); err != nil {
			logrus.Warn("AutoMigrate error:", err)
		}
	}

	logrus.Info("Database connection established & migration completed")
}
