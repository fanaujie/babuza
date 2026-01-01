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
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
)

const (
	S3TouchFile        = "touch"
	S3TouchMetadataKey = "folder-type"
)

type S3SnapshotFS struct {
	client        *s3.Client
	bucket        string
	prefix        string
	ph            api.PathHelper
	appendMetaKey string
}

var _ api.SnapshotFileSystem = (*S3SnapshotFS)(nil)

func NewS3SnapshotFS(config S3Config) (*S3SnapshotFS, error) {
	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = config.Region
			o.Credentials = credentials.NewStaticCredentialsProvider(
				config.AccessKeyID,
				config.SecretAccessKey,
				"",
			)
			if config.Endpoint != "" {
				o.BaseEndpoint = aws.String(config.Endpoint)
			}
			if config.UsePathStyle {
				o.UsePathStyle = true
			}
		},
	}

	client := s3.New(s3.Options{}, opts...)

	// Check if bucket exists, create if not
	ctx := context.Background()
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(config.Bucket),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(config.Bucket),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create bucket: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to check bucket: %w", err)
		}
	}

	prefix := config.Prefix
	prefix = strings.TrimPrefix(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	return &S3SnapshotFS{
		client:        client,
		bucket:        config.Bucket,
		prefix:        prefix,
		ph:            api.NewPathHelper("snapshot", "snapshot", "snapshot"),
		appendMetaKey: "append-meta.json",
	}, nil
}

func (fs *S3SnapshotFS) toObjectKey(path string) string {
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	return fs.prefix + path
}

func (fs *S3SnapshotFS) fromObjectKey(key string) string {
	return strings.TrimPrefix(key, fs.prefix)
}

func (fs *S3SnapshotFS) FileRead(path string) (io.ReadCloser, error) {
	objKey := fs.toObjectKey(path)
	output, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(objKey),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (fs *S3SnapshotFS) FileWrite(path string) (io.WriteCloser, error) {
	objKey := fs.toObjectKey(path)
	return NewS3ObjectWriter(fs.client, fs.bucket, objKey), nil
}

func (fs *S3SnapshotFS) CrcFileRead(path string) (api.CrcFileReader, error) {
	reader, err := fs.FileRead(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateReader(reader), nil
}

func (fs *S3SnapshotFS) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	writer, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateWriter(writer), nil
}

func (fs *S3SnapshotFS) CreateDirAndTouch(snapshotDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	installDir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	if err != nil {
		return "", err
	}
	targetFile := filepath.Join(installDir, S3TouchFile)
	objKey := fs.toObjectKey(targetFile)

	// Check if touch file already exists
	_, err = fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(objKey),
	})
	if err == nil {
		return "", fmt.Errorf("%s already exists", targetFile)
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		return "", err
	}

	// Create touch file with folder type metadata
	_, err = fs.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(objKey),
		Body:   strings.NewReader(time.Now().String()),
		Metadata: map[string]string{
			S3TouchMetadataKey: strconv.Itoa(int(folderType)),
		},
	})
	if err != nil {
		return "", err
	}
	return installDir, nil
}

func (fs *S3SnapshotFS) FileAppendData(path string, chunkId int64, data []byte) error {
	objectKey := fs.toObjectKey(fmt.Sprintf("%s-%d", path, chunkId))
	_, err := fs.client.PutObject(
		context.Background(),
		&s3.PutObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(data),
		},
	)
	return err
}

func (fs *S3SnapshotFS) FileAppendFinalize(path string, totalChunks int64) error {
	// For S3, we need to use multipart upload completion or copy objects
	// Since we're uploading chunks as separate objects, we need to:
	// 1. Create a multipart upload
	// 2. Copy each chunk as a part
	// 3. Complete the multipart upload
	// 4. Delete the chunk objects

	ctx := context.Background()
	finalKey := fs.toObjectKey(path)

	// Start multipart upload
	createResp, err := fs.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(finalKey),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	uploadId := createResp.UploadId
	var completedParts []types.CompletedPart

	// Upload each chunk as a part using UploadPartCopy
	for i := int64(1); i <= totalChunks; i++ {
		chunkKey := fs.toObjectKey(fmt.Sprintf("%s-%d", path, i))
		copySource := fmt.Sprintf("%s/%s", fs.bucket, chunkKey)

		uploadResp, err := fs.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(fs.bucket),
			Key:        aws.String(finalKey),
			UploadId:   uploadId,
			PartNumber: aws.Int32(int32(i)),
			CopySource: aws.String(copySource),
		})
		if err != nil {
			// Abort the multipart upload on failure
			_, _ = fs.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(fs.bucket),
				Key:      aws.String(finalKey),
				UploadId: uploadId,
			})
			return fmt.Errorf("failed to copy part %d: %w", i, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.CopyPartResult.ETag,
			PartNumber: aws.Int32(int32(i)),
		})
	}

	// Complete multipart upload
	_, err = fs.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(fs.bucket),
		Key:      aws.String(finalKey),
		UploadId: uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	// Delete chunk objects
	for i := int64(1); i <= totalChunks; i++ {
		chunkKey := fs.toObjectKey(fmt.Sprintf("%s-%d", path, i))
		_, err = fs.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(chunkKey),
		})
		if err != nil {
			return fmt.Errorf("failed to delete chunk %d: %w", i, err)
		}
	}

	return nil
}

func (fs *S3SnapshotFS) FindMetadataFile(dirPath string) ([]uint64, error) {
	var indices []uint64

	dirKey := fs.toObjectKey(dirPath)

	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(dirKey),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			filename := filepath.Base(aws.ToString(obj.Key))
			index, err := fs.ph.ParseMetadataFileName(filename)
			if err == nil {
				indices = append(indices, index)
			}
		}
	}

	return indices, nil
}

func (fs *S3SnapshotFS) ScanInstalledSnapshot(snapshotDirPath string) ([]uint64, error) {
	var indices []uint64
	dirKey := fs.toObjectKey(snapshotDirPath)

	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(dirKey),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			if strings.Compare(S3TouchFile, filepath.Base(aws.ToString(obj.Key))) == 0 {
				index, err := fs.ph.ParseSnapshotFolderName(filepath.Base(filepath.Dir(aws.ToString(obj.Key))))
				if err == nil {
					// Get object metadata to check folder type
					headResp, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
						Bucket: aws.String(fs.bucket),
						Key:    obj.Key,
					})
					if err != nil {
						return nil, err
					}

					v, ok := headResp.Metadata[S3TouchMetadataKey]
					if !ok {
						continue
					}
					folderType, err := strconv.Atoi(v)
					if err != nil {
						return nil, err
					}
					if folderType != int(babuzapb.SnapshotFolderType_InstallSnapshot) {
						continue
					}
					indices = append(indices, index)
				}
			}
		}
	}

	return indices, nil
}

func (fs *S3SnapshotFS) ScanTempSnapshotFolder(snapshotDirPath string) ([]string, []string, error) {
	var tmpWriter, tmpReceiver []string
	dirKey := fs.toObjectKey(snapshotDirPath)

	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(dirKey),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, nil, err
		}

		for _, obj := range page.Contents {
			if strings.Compare(S3TouchFile, filepath.Base(aws.ToString(obj.Key))) == 0 {
				headResp, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String(fs.bucket),
					Key:    obj.Key,
				})
				if err != nil {
					return nil, nil, err
				}

				v, ok := headResp.Metadata[S3TouchMetadataKey]
				if !ok {
					continue
				}
				folderType, err := strconv.Atoi(v)
				if err != nil {
					return nil, nil, err
				}
				if folderType == int(babuzapb.SnapshotFolderType_TempWrite) {
					tmpWriter = append(tmpWriter, fs.fromObjectKey(filepath.Dir(aws.ToString(obj.Key))))
				} else if folderType == int(babuzapb.SnapshotFolderType_TempReceive) {
					tmpReceiver = append(tmpReceiver, fs.fromObjectKey(filepath.Dir(aws.ToString(obj.Key))))
				}
			}
		}
	}

	return tmpWriter, tmpReceiver, nil
}

func (fs *S3SnapshotFS) InstallSnapshotFromTempFolder(snapshotDirPath string, folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	installDir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDirPath, folderType, snapshotIndex)
	if err != nil {
		return err
	}
	touchObjectKey := fs.toObjectKey(filepath.Join(installDir, S3TouchFile))

	switch folderType {
	case babuzapb.SnapshotFolderType_TempWrite:
	case babuzapb.SnapshotFolderType_TempReceive:
		break
	default:
		return fmt.Errorf("invalid folder type: %v", folderType)
	}

	// Copy object with updated metadata
	copySource := fmt.Sprintf("%s/%s", fs.bucket, touchObjectKey)
	_, err = fs.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:            aws.String(fs.bucket),
		Key:               aws.String(touchObjectKey),
		CopySource:        aws.String(copySource),
		MetadataDirective: types.MetadataDirectiveReplace,
		Metadata: map[string]string{
			S3TouchMetadataKey: strconv.Itoa(int(babuzapb.SnapshotFolderType_InstallSnapshot)),
		},
	})

	return err
}

func (fs *S3SnapshotFS) ExistFilePath(path string) bool {
	objKey := fs.toObjectKey(path)
	_, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(objKey),
	})
	return err == nil
}

func (fs *S3SnapshotFS) ExistDir(path string) bool {
	dirKey := fs.toObjectKey(path)

	result, err := fs.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(dirKey),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return false
	}

	return len(result.Contents) > 0
}

func (fs *S3SnapshotFS) FileSize(path string) (int64, error) {
	objKey := fs.toObjectKey(path)
	headResp, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(objKey),
	})
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(headResp.ContentLength), nil
}

func (fs *S3SnapshotFS) SyncDir(path string) error {
	// Not implemented for S3 - objects are durable once written
	return nil
}

func (fs *S3SnapshotFS) SyncFile(path string) error {
	// Not implemented for S3 - objects are durable once written
	return nil
}

func (fs *S3SnapshotFS) RenameDir(oldPath string, newPath string) error {
	// Not implemented for S3 - would require copying all objects
	return nil
}

func (fs *S3SnapshotFS) RemoveDir(path string) error {
	dirKey := fs.toObjectKey(path)
	ctx := context.Background()

	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(dirKey),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, obj := range page.Contents {
			_, err = fs.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    obj.Key,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (fs *S3SnapshotFS) RemoveFilePath(path string) error {
	objKey := fs.toObjectKey(path)
	_, err := fs.client.DeleteObject(
		context.Background(),
		&s3.DeleteObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(objKey),
		},
	)
	return err
}

func (fs *S3SnapshotFS) PathHelper() api.PathHelper {
	return fs.ph
}

func (fs *S3SnapshotFS) Close() error {
	return nil
}
