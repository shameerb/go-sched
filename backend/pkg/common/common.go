package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	DefaultHeartBeat = 5 * time.Second
)

func GetConnString() string {
	var missing []string
	checkEnvVar := func(val, key string) {
		if val == "" {
			missing = append(missing, key)
		}
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(password, "POSTGRES_PASSWORD")

	user := os.Getenv("POSTGRES_USER")
	checkEnvVar(user, "POSTGRES_USER")

	host := os.Getenv("POSTGRES_HOST")
	checkEnvVar(host, "POSTGRES_HOST")

	db := os.Getenv("POSTGRES_DB")
	checkEnvVar(db, "POSTGRES_DB")

	if len(missing) > 0 {
		log.Fatalf("following env vars are missing: %s", strings.Join(missing, ", "))
	}
	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, password, host, db)
}

func ConnectToDatabase(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	count := 0
	var pool *pgxpool.Pool
	var err error
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalf("failed to create db config: %s\n", err)
	}
	for count < 5 {
		// try to connect
		pool, err = pgxpool.ConnectConfig(ctx, config)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
		count++
	}
	log.Printf("connected to database")
	return pool, nil
}
