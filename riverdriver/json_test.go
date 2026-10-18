package riverdriver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalJSONNoEscapeHTML(t *testing.T) {
	t.Parallel()

	t.Run("KeepsHTMLCharacters", func(t *testing.T) {
		t.Parallel()

		data, err := MarshalJSONNoEscapeHTML(map[string]any{
			"args": json.RawMessage(`{"html": "<b>&</b>"}`),
			"tags": []string{"<tag>"},
		})
		require.NoError(t, err)

		// Raw JSON is compacted like json.Marshal would, but HTML characters
		// aren't escaped, and the encoder's trailing newline is removed.
		require.Equal(t, `{"args":{"html":"<b>&</b>"},"tags":["<tag>"]}`, string(data)) //nolint:testifylint // exact bytes matter; JSONEq treats < and < as equal
	})

	t.Run("UnsupportedValue", func(t *testing.T) {
		t.Parallel()

		_, err := MarshalJSONNoEscapeHTML(func() {})
		require.Error(t, err)
	})
}
