package multierror

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAppend(t *testing.T) {
	me := New()
	me.Append(errors.New("1"))
	me.Append(errors.New("2"))
	me.Append(errors.New("3"))
	me.Append(nil)
	assert.Equal(t, 3, len(me.errors))
}

func TestGet(t *testing.T) {
	me := New()
	assert.Nil(t, me.Get())
	me.Append(errors.New("1"))
	assert.Equal(t, "1 error: 1", me.Get().Error())
	me.Append(errors.New("2"))
	assert.Equal(t, "2 errors: 1; 2", me.Get().Error())
	me.Append(errors.New("3"))
	assert.Equal(t, "3 errors: 1; 2; 3", me.Get().Error())

}
