package mdm

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatalf("ouverture de la base : %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := NewSQLiteStore(db)
	if err != nil {
		t.Fatalf("initialisation du store : %v", err)
	}

	return store
}

func TestUpsertCreatesNewRecord(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	applied, err := store.Upsert(ctx, Record{SourceUserID: 1, Name: "Jeanne Martin", Email: "jeanne@example.com", UpdatedAt: now})
	if err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}
	if !applied {
		t.Fatal("applied devrait etre true pour un nouveau client")
	}

	record, err := store.FindBySourceUserID(ctx, 1)
	if err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}
	if record.Name != "Jeanne Martin" || record.Email != "jeanne@example.com" {
		t.Fatalf("record inattendu : %+v", record)
	}
}

func TestUpsertIgnoresOlderUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	recent := time.Now().UTC()
	older := recent.Add(-24 * time.Hour)

	if _, err := store.Upsert(ctx, Record{SourceUserID: 1, Name: "Nom recent", Email: "recent@example.com", UpdatedAt: recent}); err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}

	applied, err := store.Upsert(ctx, Record{SourceUserID: 1, Name: "Vieux nom", Email: "ancien@example.com", UpdatedAt: older})
	if err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}
	if applied {
		t.Fatal("un message plus ancien ne doit jamais etre applique")
	}

	record, _ := store.FindBySourceUserID(ctx, 1)
	if record.Name != "Nom recent" {
		t.Fatalf("le golden record a regresse : %+v", record)
	}
}

func TestUpsertAppliesNewerUpdate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	first := time.Now().UTC().Add(-time.Hour)
	second := time.Now().UTC()

	if _, err := store.Upsert(ctx, Record{SourceUserID: 1, Name: "Ancien nom", Email: "ancien@example.com", UpdatedAt: first}); err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}

	applied, err := store.Upsert(ctx, Record{SourceUserID: 1, Name: "Nouveau nom", Email: "nouveau@example.com", UpdatedAt: second})
	if err != nil {
		t.Fatalf("erreur inattendue : %v", err)
	}
	if !applied {
		t.Fatal("une mise a jour plus recente doit etre appliquee")
	}

	record, _ := store.FindBySourceUserID(ctx, 1)
	if record.Name != "Nouveau nom" {
		t.Fatalf("mise a jour non appliquee : %+v", record)
	}
}

func TestFindByEmailNotFound(t *testing.T) {
	store := newTestStore(t)

	_, err := store.FindByEmail(context.Background(), "inconnu@example.com")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("attendu ErrNotFound, obtenu : %v", err)
	}
}
