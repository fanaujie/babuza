package kvstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOrderMap_Set(t *testing.T) {
	o := NewKvOperationOrderMap()
	o.Set("foo", "bar")
	_, ok := o.oMap["foo"]
	assert.Equal(t, true, ok)

	o.Set("foofoo", "barbar")
	_, ok = o.oMap["foofoo"]
	assert.Equal(t, true, ok)
	assert.Equal(t, 2, o.oList.Len())

}

func TestOrderMap_Get(t *testing.T) {
	o := NewKvOperationOrderMap()
	o.Set("foo", "bar")
	_, ok := o.oMap["foo"]
	assert.Equal(t, true, ok)

	v, exist := o.Get("foo")
	assert.Equal(t, true, exist)
	assert.Equal(t, "bar", v)
	_, exist = o.Get("bar")
	assert.Equal(t, false, exist)
}

func TestOrderMap_Delete(t *testing.T) {
	o := NewKvOperationOrderMap()
	o.Set("foo", "bar")
	assert.Equal(t, true, o.Delete("foo"))
	_, ok := o.oMap["foo"]
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, o.oList.Len())
	assert.Equal(t, false, o.Delete("foo"))
}
func TestOrderMap_Append(t *testing.T) {
	o := NewKvOperationOrderMap()
	o.Set("foo", "bar")
	result := o.Append("foo", "bar")
	assert.Equal(t, "barbar", result)

	result = o.Append("bar", "foo")
	assert.Equal(t, "foo", result)

}
func TestOrderMap_Iterator(t *testing.T) {
	o := NewKvOperationOrderMap()

	o.Set("foo1", "bar1")
	o.Set("foo2", "bar2")
	o.Set("foo3", "bar3")

	it := o.Iterator()
	expectKey := "foo"
	expectValue := "bar"
	count := 1
	for pair := it.First(); pair != nil; pair = it.Next() {
		assert.Equal(t, fmt.Sprintf("%s%d", expectKey, count), pair.Key)
		assert.Equal(t, fmt.Sprintf("%s%d", expectValue, count), pair.Value)
		count++
	}
}
