// kbeauty-customer-mdm : golden record client pour l'ecosysteme kbeauty.
// Exercice de montee en competence (prepa Master MIAGE) - voir
// docs/montee-competence dans kbeauty-shop pour le cadrage complet.
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "modernc.org/sqlite"

	"github.com/NF31/kbeauty-customer-mdm/internal/httpapi"
	"github.com/NF31/kbeauty-customer-mdm/internal/mdm"
	"github.com/NF31/kbeauty-customer-mdm/internal/rabbitmq"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func main() {
	rabbitURL := getenv("RABBITMQ_URL", "amqp://mdm:mdm@localhost:5672")
	dbPath := getenv("MDM_DB_PATH", "./data/customers.sqlite")
	port := getenv("PORT", "3001")
	schemaPath := getenv("CUSTOMER_UPDATED_SCHEMA_PATH", "./contracts/rabbitmq/customer-updated.schema.json")

	if err := os.MkdirAll("./data", 0o755); err != nil {
		slog.Error("creation du dossier data", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		slog.Error("ouverture de la base", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	store, err := mdm.NewSQLiteStore(db)
	if err != nil {
		slog.Error("initialisation du store", "error", err)
		os.Exit(1)
	}

	validator, err := mdm.NewContractValidator(schemaPath)
	if err != nil {
		slog.Error("chargement du contrat", "error", err)
		os.Exit(1)
	}

	consumer, err := rabbitmq.NewConsumer(rabbitURL, store, validator)
	if err != nil {
		slog.Error("connexion a RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("consumer arrete de maniere inattendue", "error", err)
		}
	}()

	server := &http.Server{Addr: ":" + port, Handler: httpapi.NewServer(store, consumer)}
	go func() {
		slog.Info("en ecoute", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("serveur HTTP", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("arret demande, fermeture propre")
	_ = server.Shutdown(context.Background())
}
