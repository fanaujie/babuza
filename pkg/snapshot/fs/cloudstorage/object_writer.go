package cloudstorage

import (
	"github.com/fanaujie/babuza/pkg/utility/multierror"
	"io"
)

type ObjectWriter struct {
	*io.PipeWriter
	writeDoneCh chan error
}

func NewObjectWriter(p *io.PipeWriter, writeDoneCh chan error) *ObjectWriter {
	return &ObjectWriter{
		PipeWriter:  p,
		writeDoneCh: writeDoneCh,
	}
}

func (o *ObjectWriter) Write(p []byte) (int, error) {
	return o.PipeWriter.Write(p)
}

func (o *ObjectWriter) Close() error {
	me := multierror.New()
	me.Append(o.PipeWriter.Close())
	me.Append(<-o.writeDoneCh)
	return me.Get()
}
