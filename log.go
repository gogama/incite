package incite

// A Logger represents a logging object that which can receive log messages
// from a QueryManager and send them to an output sink.
//
// Logger is compatible with *log.Logger. Therefore you may, for example, set
// log.DefaultLogger(), or any other *log.Logger, as the logger field in a
// Config.
type Logger interface {
	Printf(format string, v ...interface{})
}

type nopLogger int

func (nop nopLogger) Printf(_ string, _ ...interface{}) {}

// NopLogger is a Logger that ignores any messages sent to it.
var NopLogger = nopLogger(0)
