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
	t.Cleanup(func() {
		close(versions)
	})

	// Test the lazy initialization several times in parallel.
	for i := 0; i < n; i++ {
		go func() {
			j := i

			v := version()

			require.GreaterOrEqual(t, len(v), len(modulePath), "version string must be at least as long as modulePath (iteration: %d)", j)
			assert.Equal(t, v[0:len(modulePath)], modulePath, "version string must have modulePath as a prefix (iteration: %d)", j)
			versions <- v
		}()
	}

	// Use the first version string as a reference value to compare the
	// other ones against.
	refVersion := <-versions

	// Verify that all the lazy initialized versions are the same.
	for i := 1; i < n; i++ {
		t.Run(fmt.Sprintf("Verify[%d]", i), func(t *testing.T) {
			v := <-versions

			assert.Equal(t, refVersion, v)
		})
	}
}
