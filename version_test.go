package incite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	n := 10
	versions := make(chan string, n)

	// Test the lazy initialization several times in parallel.
	for i := 0; i < n; i++ {
		t.Run(fmt.Sprintf("Lazy[%d]", i), func(t *testing.T) {
			t.Parallel()

			v := version()

			require.GreaterOrEqual(t, len(v), len(modulePath))
			assert.Equal(t, v[0:len(modulePath)], modulePath)
			versions <- v
		})
	}

	// Verify that all the lazy initialized versions are the same.
	t.Run("Reference version", func(t *testing.T) {
		t.Cleanup(func() {
			close(versions)
		})
		t.Parallel()
		refVersion := <-versions
		for i := 1; i < n; i++ {
			t.Run(fmt.Sprintf("Verify[%d]", i), func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, refVersion, <-versions)
			})
		}
	})
}
