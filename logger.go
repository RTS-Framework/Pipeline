package pipeline

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sync"
)

// control the logger output minimum level.
const (
	LevelDebug = iota
	LevelInfo
	LevelWarning
	LevelError
	LevelFatal
	LevelDiscard = math.MaxInt8
)

// DefaultLevel is the default logger level.
const DefaultLevel = LevelInfo

// Logger is a simple logger interface, it can skip log less than level.
// Methods about Fatal will not call panic in method, only record the
// log from the code like "r:= recover()"
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warning(v ...interface{})
	Warningf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(fn string, v ...interface{})
	Fatalf(fn, format string, v ...interface{})
	SetLevel(level int8)
	GetLevel() int8
}

type logger struct {
	logger *log.Logger

	level    int8
	levelRWM sync.RWMutex
}

// NewLogger is used to create a standard logger.
func NewLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stdout
	}
	lg := log.New(w, "", log.LstdFlags)
	return &logger{logger: lg, level: DefaultLevel}
}

func (l *logger) Debug(v ...interface{}) {
	if l.GetLevel() > LevelDebug {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[debug] ")
	_, _ = fmt.Fprintln(buf, v...)
	l.logger.Print(buf)
}

func (l *logger) Debugf(format string, v ...interface{}) {
	if l.GetLevel() > LevelDebug {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[debug] ")
	_, _ = fmt.Fprintf(buf, format, v...)
	l.logger.Println(buf)
}

func (l *logger) Info(v ...interface{}) {
	if l.GetLevel() > LevelInfo {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[info] ")
	_, _ = fmt.Fprintln(buf, v...)
	l.logger.Print(buf)
}

func (l *logger) Infof(format string, v ...interface{}) {
	if l.GetLevel() > LevelInfo {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[info] ")
	_, _ = fmt.Fprintf(buf, format, v...)
	l.logger.Println(buf)
}

func (l *logger) Warning(v ...interface{}) {
	if l.GetLevel() > LevelWarning {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[warning] ")
	_, _ = fmt.Fprintln(buf, v...)
	l.logger.Print(buf)
}

func (l *logger) Warningf(format string, v ...interface{}) {
	if l.GetLevel() > LevelWarning {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[warning] ")
	_, _ = fmt.Fprintf(buf, format, v...)
	l.logger.Println(buf)
}

func (l *logger) Error(v ...interface{}) {
	if l.GetLevel() > LevelError {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[error] ")
	_, _ = fmt.Fprintln(buf, v...)
	l.logger.Print(buf)
}

func (l *logger) Errorf(format string, v ...interface{}) {
	if l.GetLevel() > LevelError {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[error] ")
	_, _ = fmt.Fprintf(buf, format, v...)
	l.logger.Println(buf)
}

func (l *logger) Fatal(fn string, v ...interface{}) {
	if l.GetLevel() > LevelFatal {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[fatal] ")
	buf.WriteString("panic in " + fn + "\n")
	_, _ = fmt.Fprintln(buf, v...)
	l.logger.Print(buf)
}

func (l *logger) Fatalf(fn, format string, v ...interface{}) {
	if l.GetLevel() > LevelFatal {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("[fatal] ")
	buf.WriteString("panic in " + fn + "\n")
	_, _ = fmt.Fprintf(buf, format, v...)
	l.logger.Println(buf)
}

func (l *logger) SetLevel(level int8) {
	l.levelRWM.Lock()
	defer l.levelRWM.Unlock()
	l.level = level
}

func (l *logger) GetLevel() int8 {
	l.levelRWM.RLock()
	defer l.levelRWM.RUnlock()
	return l.level
}
