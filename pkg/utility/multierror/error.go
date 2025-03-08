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
