package cloudstorage

import (
	"github.com/minio/minio-go/v7"
	"io"
)

type ObjectReader struct {
	*minio.Object
}

func NewObjectReader(object *minio.Object) *ObjectReader {
	return &ObjectReader{
		Object: object,
	}
}

func (o *ObjectReader) Read(p []byte) (int, error) {
	n, err := o.Object.Read(p)
	if err == io.EOF && n == len(p) {
		return n, nil
	}
	return n, err
}

func (o *ObjectReader) Close() error {
	return o.Object.Close()
}
