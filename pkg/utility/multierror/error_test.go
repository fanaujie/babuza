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
