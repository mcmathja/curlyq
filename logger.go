package curlyq

import (
	"log"
)

// Logger exposes an interface for a leveled logger.
// You can provide a Logger to a Consumer and a Producer
// to modify CurlyQ's default logging behavior.
type Logger interface {
	// Debug logs fine-grained information,
	// such as when a given process starts and ends.
	Debug(...interface{})

	// Info logs useful information,
	// such as which job is currently being processed.
	Info(...interface{})

	// Warn logs non-critical errors,
	// such as network issues that are treated as transient errors.
	Warn(...interface{})

	// Error logs critical errors,
	// such as redis issues which might affect the consistency of the queue.
	Error(...interface{})
}

// DefaultLogger is a Logger that sends all logs to stdout.
type DefaultLogger struct{}

// Debug logs debug level information to stdout.
func (l *DefaultLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

// Info logs info level information to stdout.
func (l *DefaultLogger) Info(args ...interface{}) {
	log.Println(args...)
}

// Warn logs warn level information to stdout.
func (l *DefaultLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

// Error logs error level information to stdout.
func (l *DefaultLogger) Error(args ...interface{}) {
	log.Println(args...)
}
