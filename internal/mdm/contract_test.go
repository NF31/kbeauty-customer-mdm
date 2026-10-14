package mdm

import (
	"path/filepath"
	"testing"
)

func newTestValidator(t *testing.T) *ContractValidator {
	t.Helper()

	schemaPath, err := filepath.Abs("../../contracts/rabbitmq/customer-updated.schema.json")
	if err != nil {
		t.Fatalf("resolution du chemin du contrat : %v", err)
	}

	validator, err := NewContractValidator(schemaPath)
	if err != nil {
		t.Fatalf("chargement du contrat : %v", err)
	}

	return validator
}

func TestValidateAcceptsConformMessage(t *testing.T) {
	validator := newTestValidator(t)
	body := []byte(`{"eventType":"customer.updated","sourceUserId":1,"name":"Jeanne Martin","email":"jeanne@example.com","occurredAt":"2026-10-14T16:45:00Z"}`)

	var out map[string]any
	if err := validator.Validate(body, &out); err != nil {
		t.Fatalf("message conforme rejete : %v", err)
	}
}

func TestValidateRejectsMissingField(t *testing.T) {
	validator := newTestValidator(t)
	body := []byte(`{"eventType":"customer.updated","sourceUserId":1,"name":"Jeanne Martin","occurredAt":"2026-10-14T16:45:00Z"}`)

	var out map[string]any
	if err := validator.Validate(body, &out); err == nil {
		t.Fatal("message sans email aurait du etre rejete")
	}
}

func TestValidateRejectsExtraField(t *testing.T) {
	validator := newTestValidator(t)
	body := []byte(`{"eventType":"customer.updated","sourceUserId":1,"name":"Jeanne Martin","email":"jeanne@example.com","occurredAt":"2026-10-14T16:45:00Z","motDePasse":"secret"}`)

	var out map[string]any
	if err := validator.Validate(body, &out); err == nil {
		t.Fatal("message avec un champ en plus aurait du etre rejete (additionalProperties: false)")
	}
}

func TestValidateRejectsInvalidEmail(t *testing.T) {
	validator := newTestValidator(t)
	body := []byte(`{"eventType":"customer.updated","sourceUserId":1,"name":"Jeanne Martin","email":"pas-un-email","occurredAt":"2026-10-14T16:45:00Z"}`)

	var out map[string]any
	if err := validator.Validate(body, &out); err == nil {
		t.Fatal("email invalide aurait du etre rejete")
	}
}
