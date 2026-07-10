package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseURLLabelRedactsUserinfo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			want: "none",
		},
		{
			name: "plain url",
			in:   "http://localhost:11434/v1",
			want: "http://localhost:11434/v1",
		},
		{
			name: "userinfo",
			in:   "https://user:pass@example.test/v1",
			want: "https://redacted@example.test/v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, baseURLLabel(tt.in))
		})
	}
}

func TestServerLogWriters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		output     string
		wantWriter *os.File
		wantErr    string
	}{
		{
			name:   "file only",
			output: "file",
		},
		{
			name:       "stderr",
			output:     "stderr",
			wantWriter: os.Stderr,
		},
		{
			name:       "stdout",
			output:     "stdout",
			wantWriter: os.Stdout,
		},
		{
			name:    "invalid",
			output:  "bogus",
			wantErr: `invalid --log-output "bogus"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			writers, err := serverLogWriters(tt.output)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, writers)
				return
			}

			require.NoError(t, err)
			if tt.wantWriter == nil {
				require.Nil(t, writers)
				return
			}
			require.Len(t, writers, 1)
			require.Same(t, tt.wantWriter, writers[0])
		})
	}
}
