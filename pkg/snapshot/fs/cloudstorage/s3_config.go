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

type S3Config struct {
	// Endpoint is the S3-compatible endpoint URL (e.g., "s3.amazonaws.com" or custom endpoint).
	// For S3-compatible services like RustFS or MinIO, use the service's endpoint.
	Endpoint string

	// Region is the AWS region (required for AWS S3 signature calculation).
	// For S3-compatible services, this can be any non-empty string (e.g., "us-east-1").
	Region string

	// AccessKeyID is the access key for authentication.
	AccessKeyID string

	// SecretAccessKey is the secret key for authentication.
	SecretAccessKey string

	// UsePathStyle enables path-style addressing for S3-compatible services.
	// Set to true for services like RustFS, MinIO, or other S3-compatible storage
	// that don't support virtual-hosted style URLs.
	UsePathStyle bool

	// Bucket is the name of the S3 bucket to use for snapshots.
	Bucket string

	// Prefix is the object key prefix for all snapshot objects.
	// This allows organizing snapshots under a specific path within the bucket.
	Prefix string
}
