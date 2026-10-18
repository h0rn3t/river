package riverdriver

import (
	"bytes"
	"encoding/json"
)

// MarshalJSONNoEscapeHTML is json.Marshal without escaping HTML characters.
// Drivers use it to encode JSON that's stored as text, like job args and
// metadata in SQLite and SQL Server, so that it's kept as it's given rather
// than with `<`, `>`, and `&` rewritten as `<`, `>`, and `&`.
func MarshalJSONNoEscapeHTML(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
