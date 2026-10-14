// Package httpapi expose le seul point de consultation synchrone du MDM :
// un GET par email (en plus du flux evenementiel qui fait le gros du
// travail), et un /health pour un hebergeur (meme principe que
// kbeauty-notifications-service).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/NF31/kbeauty-customer-mdm/internal/mdm"
)

type RabbitStatus interface {
	IsConnected() bool
}

type customerResponse struct {
	SourceUserID int64  `json:"sourceUserId"`
	Name         string `json:"name"`
	Email        string `json:"email"`
	UpdatedAt    string `json:"updatedAt"`
}

func NewServer(store mdm.Store, rabbit RabbitStatus) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		connected := rabbit.IsConnected()
		status := "ok"
		if !connected {
			status = "degraded"
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		rabbitmqStatus := "connected"
		if !connected {
			rabbitmqStatus = "disconnected"
		}

		writeJSON(w, map[string]string{"status": status, "rabbitmq": rabbitmqStatus})
	})

	mux.HandleFunc("GET /customers/{email}", func(w http.ResponseWriter, r *http.Request) {
		email := r.PathValue("email")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		record, err := store.FindByEmail(ctx, email)
		if errors.Is(err, mdm.ErrNotFound) {
			http.Error(w, `{"error":"client introuvable"}`, http.StatusNotFound)

			return
		}
		if err != nil {
			http.Error(w, `{"error":"erreur interne"}`, http.StatusInternalServerError)

			return
		}

		writeJSON(w, customerResponse{
			SourceUserID: record.SourceUserID,
			Name:         record.Name,
			Email:        record.Email,
			UpdatedAt:    record.UpdatedAt.UTC().Format(time.RFC3339),
		})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
