package curlyq

import (
	"log"
)

type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
}

type DefaultLogger struct{}

func (l *DefaultLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

func (l *DefaultLogger) Info(args ...interface{}) {
	log.Println(args...)
}

func (l *DefaultLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

func (l *DefaultLogger) Error(args ...interface{}) {
	log.Println(args...)
}
