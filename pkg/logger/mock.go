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
