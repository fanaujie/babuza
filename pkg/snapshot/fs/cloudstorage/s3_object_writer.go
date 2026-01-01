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
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ObjectWriter struct {
	client *s3.Client
	bucket string
	key    string
	buf    *bytes.Buffer
}

func NewS3ObjectWriter(client *s3.Client, bucket, key string) *S3ObjectWriter {
	return &S3ObjectWriter{
		client: client,
		bucket: bucket,
		key:    key,
		buf:    &bytes.Buffer{},
	}
}

func (w *S3ObjectWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *S3ObjectWriter) Close() error {
	_, err := w.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buf.Bytes()),
	})
	return err
}
