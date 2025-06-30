// Copyright 2025 Chen Chunchieh <junjie725@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
)

func NewZapLogger(level zapcore.Level, logOut []string, logDir string) *zap.Logger {
	writeSyncer := getLogWriter(logOut, logDir)
	encoder := getEncoder(level == zap.DebugLevel)
	core := zapcore.NewCore(encoder, zapcore.NewMultiWriteSyncer(writeSyncer...), level)
	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
}

func getEncoder(isDebug bool) zapcore.Encoder {

	if isDebug {
		encoderConfig := zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		return zapcore.NewConsoleEncoder(encoderConfig)
	}
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(logOut []string, logDir string) []zapcore.WriteSyncer {
	var writes []zapcore.WriteSyncer
	for _, k := range logOut {
		switch k {
		case "stdout":
			writes = append(writes, zapcore.AddSync(os.Stdout))
		case "stderr":
			writes = append(writes, zapcore.AddSync(os.Stderr))
		case "journal":
			file, _ := os.Create(path.Join(logDir, "logger.log"))
			writes = append(writes, zapcore.AddSync(file))
		}
	}
	return writes
}

func ConvertToZapLevel(lvl string) zapcore.Level {
	switch lvl {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "dpanic":
		return zap.DPanicLevel
	case "panic":
		return zap.PanicLevel
	case "fatal":
		return zap.FatalLevel
	default:
		panic(fmt.Sprintf("unknown level %q", lvl))
	}
}

type RaftLogger struct {
	lg *zap.SugaredLogger
}

func NewRaftLogger(lg *zap.SugaredLogger) *RaftLogger {
	return &RaftLogger{
		lg: lg,
	}
}
func (l *RaftLogger) Desugar() *zap.Logger {
	return l.lg.Desugar()
}

func (l *RaftLogger) Debug(v ...interface{}) {
	l.lg.Debug(v...)
}
func (l *RaftLogger) Debugf(format string, v ...interface{}) {
	l.lg.Debugf(format, v...)
}
func (l *RaftLogger) Error(v ...interface{}) {
	l.lg.Error(v...)
}
func (l *RaftLogger) Errorf(format string, v ...interface{}) {
	l.lg.Errorf(format, v...)
}
func (l *RaftLogger) Info(v ...interface{}) {
	l.lg.Info(v...)
}
func (l *RaftLogger) Infof(format string, v ...interface{}) {
	l.lg.Infof(format, v...)
}
func (l *RaftLogger) Warning(v ...interface{}) {
	l.lg.Warn(v...)
}
func (l *RaftLogger) Warningf(format string, v ...interface{}) {
	l.lg.Warnf(format, v...)
}
func (l *RaftLogger) Fatal(v ...interface{}) {
	l.lg.Fatal(v...)
}
func (l *RaftLogger) Fatalf(format string, v ...interface{}) {
	l.lg.Fatalf(format, v...)
}
func (l *RaftLogger) Panic(v ...interface{}) {
	l.lg.Panic(v...)
}

func (l *RaftLogger) Panicf(format string, v ...interface{}) {
	l.lg.Panicf(format, v...)
}
