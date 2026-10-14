package mdm

import (
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ErrMessageHorsContrat : meme principe que MessageHorsContrat cote
// kbeauty-notifications-service - un message est une donnee non fiable
// tant qu'il n'est pas verifie contre son contrat.
type ErrMessageHorsContrat struct {
	cause error
}

func (e *ErrMessageHorsContrat) Error() string {
	return fmt.Sprintf("message hors contrat : %s", e.cause)
}

func (e *ErrMessageHorsContrat) Unwrap() error { return e.cause }

type ContractValidator struct {
	schema *jsonschema.Schema
}

func NewContractValidator(schemaPath string) (*ContractValidator, error) {
	compiler := jsonschema.NewCompiler()
	// Les formats ("email", "date-time"...) ne sont que des annotations par
	// defaut a partir de draft 2019-09 (meme situation que Ajv/ajv-formats
	// cote kbeauty-notifications-service) - il faut explicitement demander
	// qu'ils soient verifies.
	compiler.AssertFormat = true

	schema, err := compiler.Compile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("compilation du schema %s : %w", schemaPath, err)
	}

	return &ContractValidator{schema: schema}, nil
}

// Validate verifie body contre le contrat et le decode dans out (pointeur).
func (v *ContractValidator) Validate(body []byte, out any) error {
	var asMap map[string]any
	if err := json.Unmarshal(body, &asMap); err != nil {
		return &ErrMessageHorsContrat{cause: err}
	}

	if err := v.schema.Validate(asMap); err != nil {
		return &ErrMessageHorsContrat{cause: err}
	}

	if err := json.Unmarshal(body, out); err != nil {
		return &ErrMessageHorsContrat{cause: err}
	}

	return nil
}
