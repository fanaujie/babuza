package logger

type Mock struct {
}

func (l *Mock) Debug(v ...interface{})                   {}
func (l *Mock) Debugf(format string, v ...interface{})   {}
func (l *Mock) Error(v ...interface{})                   {}
func (l *Mock) Errorf(format string, v ...interface{})   {}
func (l *Mock) Info(v ...interface{})                    {}
func (l *Mock) Infof(format string, v ...interface{})    {}
func (l *Mock) Warning(v ...interface{})                 {}
func (l *Mock) Warningf(format string, v ...interface{}) {}
func (l *Mock) Fatal(v ...interface{})                   {}
func (l *Mock) Fatalf(format string, v ...interface{})   {}
func (l *Mock) Panic(v ...interface{})                   {}
func (l *Mock) Panicf(format string, v ...interface{})   {}
