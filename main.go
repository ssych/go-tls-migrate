package main

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	clickhouse "github.com/ClickHouse/clickhouse-go"
	migrate "github.com/golang-migrate/migrate/v4"
	clickhousemigrate "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file" // get clickhouse driver file
)

const (
	tlsName = "default_tls_name"
)

type flags struct {
	*flag.FlagSet

	ConnectionString string
	PemPath          string
	Path             string
}

func newFlagSet() *flags {
	f := &flags{}
	f.FlagSet = flag.NewFlagSet("migration_ch", flag.ContinueOnError)
	f.FlagSet.SetOutput(io.Discard)
	f.StringVar(&f.ConnectionString, "database", "", "clickhouse connection string")
	f.StringVar(&f.PemPath, "pem_path", "", "path to ssl root certificate")
	f.StringVar(&f.Path, "path", "migrations", "migrations path")
	return f
}

func (f *flags) Parse() error {
	return f.FlagSet.Parse(os.Args[1:])
}

func NewConn(
	connectString string,
	pemPath string,
) (*sql.DB, error) {
	if len(connectString) == 0 {
		return nil, errors.New("empty connection string")
	}

	if len(pemPath) > 0 {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(pemPath)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, errors.New("failed to append pem file")
		}

		tlsConfig := &tls.Config{
			RootCAs:    rootCertPool,
			MinVersion: tls.VersionTLS13,
		}

		err = clickhouse.RegisterTLSConfig(tlsName, tlsConfig)
		if err != nil {
			return nil, err
		}

		connectString = fmt.Sprintf("%s&tls_config=%s", connectString, tlsName)
	}

	return sql.Open("clickhouse", connectString)
}

func Migrate(connect *sql.DB, path string) error {
	driver, err := clickhousemigrate.WithInstance(connect, &clickhousemigrate.Config{MultiStatementEnabled: true})
	if err != nil {
		return fmt.Errorf("failed to init migration db driver: clickhouse.WithInstance(): %v", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", path),
		"clickhouse",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to init migration: migrate.NewWithDatabaseInstance(): %v", err)
	}

	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migration: %v", err)
	}
	return nil
}

func main() {
	fs := newFlagSet()
	if err := fs.Parse(); err != nil {
		log.Fatalf("command line error %s", err)
	}

	connect, err := NewConn(fs.ConnectionString, fs.PemPath)
	if err != nil {
		log.Fatalf("failed to create clickhouse storage-> %v", err)
	}

	if err = Migrate(connect, fs.Path); err != nil {
		log.Fatalf("failed to migrate -> %v", err)
	}
	log.Println("migration done")
}
