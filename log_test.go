package incite

import "testing"

func TestNopLogger_Printf(t *testing.T) {
	t.Run("Empty Format", func(t *testing.T) {
		NopLogger.Printf("foo")
	})

	t.Run("One Argument", func(t *testing.T) {
		NopLogger.Printf("hello, %q", "world")
	})

	t.Run("Two Arguments", func(t *testing.T) {
		NopLogger.Printf("the two numbers are %d and %d", 1, 2)
	})
}
