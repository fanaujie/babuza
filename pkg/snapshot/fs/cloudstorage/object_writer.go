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
