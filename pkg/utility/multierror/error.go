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


package multierror

import (
	"fmt"
)

type MultiError struct {
	errors []error
}

func New() *MultiError {
	return &MultiError{}
}

func (m *MultiError) Append(err error) {
	if err != nil {
		m.errors = append(m.errors, err)
	}
}

func (m *MultiError) Get() error {
	if len(m.errors) == 0 {
		return nil
	}
	return m
}

func (m *MultiError) Error() string {
	if len(m.errors) == 0 {
		return ""
	}
	buf := getBuffer()
	defer releaseBuffer(buf)
	buf.Reset()
	if len(m.errors) == 1 {
		buf.WriteString("1 error: ")
	} else {
		buf.WriteString(fmt.Sprintf("%d errors: ", len(m.errors)))
	}
	for i, err := range m.errors {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}
