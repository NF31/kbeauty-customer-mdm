// Package mdm tient le golden record client : aujourd'hui un simple miroir
// (un seul producteur, kbeauty-shop), mais l'architecture (Upsert idempotent,
// horodatage de fraicheur) est celle qu'il faudrait pour fusionner plusieurs
// sources plus tard - voir Limites et suite dans le README.
package mdm

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Record est le golden record d'un client, tel qu'exporte par l'API et
// republie dans customer.golden_record.updated.
type Record struct {
	SourceUserID int64
	Name         string
	Email        string
	UpdatedAt    time.Time
}

var ErrNotFound = errors.New("client introuvable")

// Store conserve les golden records. Implemente par SQLiteStore ; une
// interface pour que les tests puissent la doubler sans base reelle.
type Store interface {
	// Upsert applique la mise a jour si elle est plus recente que ce qui est
	// deja enregistre (ou si le client est inconnu). Renvoie true si le
	// golden record a reellement change (et doit donc etre republie).
	Upsert(ctx context.Context, r Record) (applied bool, err error)
	FindByEmail(ctx context.Context, email string) (Record, error)
	FindBySourceUserID(ctx context.Context, id int64) (Record, error)
}

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(db *sql.DB) (*SQLiteStore, error) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS customers (
			source_user_id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_customers_email ON customers (email);
	`)
	if err != nil {
		return nil, err
	}

	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Upsert(ctx context.Context, r Record) (bool, error) {
	existing, err := s.FindBySourceUserID(ctx, r.SourceUserID)
	if err == nil && !r.UpdatedAt.After(existing.UpdatedAt) {
		// Message en retard ou deja applique (livraison "au moins une
		// fois") : on ne regresse jamais vers une version plus ancienne.
		return false, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return false, err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO customers (source_user_id, name, email, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (source_user_id) DO UPDATE SET
			name = excluded.name,
			email = excluded.email,
			updated_at = excluded.updated_at
	`, r.SourceUserID, r.Name, r.Email, r.UpdatedAt.UTC().Format(time.RFC3339))

	return err == nil, err
}

func (s *SQLiteStore) FindByEmail(ctx context.Context, email string) (Record, error) {
	return s.scanOne(s.db.QueryRowContext(ctx,
		`SELECT source_user_id, name, email, updated_at FROM customers WHERE email = ?`, email))
}

func (s *SQLiteStore) FindBySourceUserID(ctx context.Context, id int64) (Record, error) {
	return s.scanOne(s.db.QueryRowContext(ctx,
		`SELECT source_user_id, name, email, updated_at FROM customers WHERE source_user_id = ?`, id))
}

func (s *SQLiteStore) scanOne(row *sql.Row) (Record, error) {
	var r Record
	var updatedAt string

	if err := row.Scan(&r.SourceUserID, &r.Name, &r.Email, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, ErrNotFound
		}

		return Record{}, err
	}

	parsed, err := time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return Record{}, err
	}
	r.UpdatedAt = parsed

	return r, nil
}
