package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultHeartBeat = 5 * time.Second
)

func GetDBConnString() string {
	var missing []string

	checkEnvVar := func(val, key string) {
		if val == "" {
			missing = append(missing, key)
		}
	}
	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")
	dbHost := os.Getenv(("POSTGRES_HOST"))
	checkEnvVar(dbHost, "POSTGRES_HOST")
	if len(missing) > 0 {
		log.Fatalf("following vars are missing : %s", strings.Join(missing, ", "))
	}
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
}

func ConnectToDatabase(ctx context.Context, dbConnString string) (*pgxpool.Pool, error) {
	config := dbConfig(dbConnString)
	count := 0
	var pool *pgxpool.Pool
	var err error
	for count < 5 {
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			break
		}
		log.Printf("retry connecting to database in 5 sec...")
		time.Sleep(5 * time.Second)
		count++
	}
	log.Println("connected to database")
	return pool, nil
}

func dbConfig(dbConnString string) *pgxpool.Config {
	dbConfig, err := pgxpool.ParseConfig(dbConnString)
	if err != nil {
		log.Fatalf("failed to create db config: %s", err)
	}
	return dbConfig
}
