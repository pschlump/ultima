package httpapi

// JsonBody decodes a JSON request body into a generated DTO and validates
// it (design doc §10.1, the exsms lib/utils/jsonbody.go pattern, D7):
// decode (empty body → zero value, so optional-field endpoints stay
// lenient) → config.SetDefaults (the `default:"..."` struct-tag machinery
// shared with lib/config) → go-playground/validator over the generated
// `validate:` tags (x-oapi-codegen-extra-tags in api/openapi.yaml).

import (
	"encoding/json"
	"net/http"

	"github.com/go-playground/validator/v10"

	"github.com/pschlump/ultima/lib/config"
)

// dtoValidator is the package-level validator singleton (validator.New is
// safe for concurrent use after registration).
var dtoValidator = validator.New(validator.WithRequiredStructEnabled())

// JsonBody decodes the request body into v (a pointer to a generated
// request DTO), applies defaults and validates. On any failure it writes
// the 400 Error reply and returns false; the handler must just return.
//
//nolint:revive // the exsms-pattern name JsonBody is deliberate (§10.1)
func JsonBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(v); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return false
		}
	}
	if err := config.SetDefaults(v); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	if err := dtoValidator.Struct(v); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}
