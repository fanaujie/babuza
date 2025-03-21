package cloudstorage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/crcfile"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	MinioTouchFile        = "touch"
	MinioTouchMetadataKey = "X-Amz-Meta-Folder-Type"
)

type MinIOSnapshotFS struct {
	client        *minio.Client
	bucket        string
	prefix        string
	ph            api.PathHelper
	appendMetaKey string
}

type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	Bucket          string
	Prefix          string
}

func NewMinioSnapshotFS(config Config) (api.SnapshotFileSystem, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(context.Background(), config.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		err = client.MakeBucket(context.Background(), config.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	prefix := config.Prefix
	prefix = strings.TrimPrefix(prefix, "/") // Remove leading slash
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	return &MinIOSnapshotFS{
		client:        client,
		bucket:        config.Bucket,
		prefix:        prefix,
		ph:            api.NewPathHelper("snapshot", "snapshot", "snapshot"),
		appendMetaKey: "append-meta.json",
	}, nil
}

func (fs *MinIOSnapshotFS) toObjectKey(path string) string {
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	return fs.prefix + path
}

func (fs *MinIOSnapshotFS) fromObjectKey(key string) string {
	return strings.TrimPrefix(key, fs.prefix)
}

func (fs *MinIOSnapshotFS) FileRead(path string) (io.ReadCloser, error) {
	objKey := fs.toObjectKey(path)
	obj, err := fs.client.GetObject(context.Background(), fs.bucket, objKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return NewObjectReader(obj), nil
}

func (fs *MinIOSnapshotFS) FileWrite(path string) (io.WriteCloser, error) {
	objKey := fs.toObjectKey(path)

	pr, pw := io.Pipe()
	doneCh := make(chan error)
	go func() {
		// Since objectSize is -1 (unknown file size), the default PartSize needs to be configured manually.
		// Here, a 5MB PartSize is recommended, which is the size suggested by MinIO.
		_, err := fs.client.PutObject(context.Background(), fs.bucket, objKey, pr, -1, minio.PutObjectOptions{
			PartSize: 5 * 1024 * 1024, // 5MB part size
		})
		doneCh <- err
	}()

	return NewObjectWriter(pw, doneCh), nil
}

func (fs *MinIOSnapshotFS) CrcFileRead(path string) (api.CrcFileReader, error) {
	reader, err := fs.FileRead(path)
	if err != nil {
		return nil, err
	}
	return crcfile.CreateReader(reader), nil
}

func (fs *MinIOSnapshotFS) CrcFileWrite(path string) (api.CrcFileWriter, error) {
	writer, err := fs.FileWrite(path)
	if err != nil {
		return nil, err
	}

	return crcfile.CreateWriter(writer), nil
}

func (fs *MinIOSnapshotFS) CreateDirAndTouch(snapshotDir string, folderType babuzapb.SnapshotFolderType, snapIndex uint64) (string, error) {
	installDir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	if err != nil {
		return "", err
	}
	targetFile := filepath.Join(installDir, MinioTouchFile)
	objKey := fs.toObjectKey(targetFile)

	_, err = fs.client.StatObject(context.Background(), fs.bucket, objKey, minio.StatObjectOptions{})
	if err == nil {
		return "", fmt.Errorf("%s already exists", targetFile)
	}

	errResponse := minio.ToErrorResponse(err)
	if errResponse.Code != "NoSuchKey" {
		return "", err
	}

	_, err = fs.client.PutObject(context.Background(), fs.bucket, objKey, strings.NewReader(time.Now().String()),
		-1, minio.PutObjectOptions{
			UserMetadata: map[string]string{
				MinioTouchMetadataKey: strconv.Itoa(int(folderType)),
			},
		})
	return installDir, nil
}

func (fs *MinIOSnapshotFS) FileAppendData(path string, chunkId int64, data []byte) error {
	objectKey := fs.toObjectKey(fmt.Sprintf("%s-%d", path, chunkId))
	_, err := fs.client.PutObject(
		context.Background(),
		fs.bucket,
		objectKey,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{},
	)
	return err
}

func (fs *MinIOSnapshotFS) FileAppendFinalize(path string, totalChunks int64) error {
	// use compose object to merge all chunks
	chunkKeys := make([]minio.CopySrcOptions, totalChunks)
	for i := int64(1); i <= totalChunks; i++ {
		chunkKeys[i-1] = minio.CopySrcOptions{
			Bucket: fs.bucket,
			Object: fs.toObjectKey(fmt.Sprintf("%s-%d", path, i))}
	}
	_, err := fs.client.ComposeObject(context.Background(), minio.CopyDestOptions{
		Bucket: fs.bucket,
		Object: fs.toObjectKey(path)}, chunkKeys...)
	if err != nil {
		return err
	}
	// remove all chunks
	for i := int64(1); i <= totalChunks; i++ {
		err = fs.client.RemoveObject(context.Background(), fs.bucket, chunkKeys[i-1].Object, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}
	}

	return err
}

func (fs *MinIOSnapshotFS) FindMetadataFile(dirPath string) ([]uint64, error) {
	var indices []uint64

	dirKey := fs.toObjectKey(dirPath)

	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: true,
	}

	for obj := range fs.client.ListObjects(context.Background(), fs.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		filename := filepath.Base(obj.Key)
		index, err := fs.ph.ParseMetadataFileName(filename)
		if err == nil {
			indices = append(indices, index)
		}
	}

	return indices, nil
}

func (fs *MinIOSnapshotFS) ScanInstalledSnapshot(snapshotDirPath string) ([]uint64, error) {
	var indices []uint64
	dirKey := fs.toObjectKey(snapshotDirPath)
	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: true,
	}
	for obj := range fs.client.ListObjects(context.Background(), fs.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}

		if strings.Compare(MinioTouchFile, filepath.Base(obj.Key)) == 0 {
			index, err := fs.ph.ParseSnapshotFolderName(filepath.Base(filepath.Dir(obj.Key)))
			if err == nil {
				objectInfo, err := fs.client.StatObject(context.Background(), fs.bucket, obj.Key, minio.StatObjectOptions{})
				if err != nil {
					return nil, err
				}
				v, ok := objectInfo.Metadata[MinioTouchMetadataKey]
				if !ok {
					continue
				}
				folderType, err := strconv.Atoi(v[0])
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
	return indices, nil
}

func (fs *MinIOSnapshotFS) ScanTempSnapshotFolder(snapshotDirPath string) ([]string, []string, error) {
	var tmpWriter, tmpReceiver []string
	dirKey := fs.toObjectKey(snapshotDirPath)
	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: true,
	}
	for obj := range fs.client.ListObjects(context.Background(), fs.bucket, opts) {
		if obj.Err != nil {
			return nil, nil, obj.Err
		}
		if strings.Compare(MinioTouchFile, filepath.Base(obj.Key)) == 0 {
			objectInfo, err := fs.client.StatObject(context.Background(), fs.bucket, obj.Key, minio.StatObjectOptions{})
			if err != nil {
				return nil, nil, err
			}
			v, ok := objectInfo.Metadata[MinioTouchMetadataKey]
			if !ok {
				continue
			}
			folderType, err := strconv.Atoi(v[0])
			if err != nil {
				return nil, nil, err
			}
			if folderType == int(babuzapb.SnapshotFolderType_TempWrite) {
				tmpWriter = append(tmpWriter, fs.fromObjectKey(filepath.Dir(obj.Key)))
			} else if folderType == int(babuzapb.SnapshotFolderType_TempReceive) {
				tmpReceiver = append(tmpReceiver, fs.fromObjectKey(filepath.Dir(obj.Key)))
			}
		}
	}
	return tmpWriter, tmpReceiver, nil
}

func (fs *MinIOSnapshotFS) InstallSnapshotFromTempFolder(snapshotDirPath string, folderType babuzapb.SnapshotFolderType, snapshotIndex uint64) error {
	installDir, err := fs.ph.GenerateSnapshotFolderPath(snapshotDirPath, folderType, snapshotIndex)
	if err != nil {
		return err
	}
	touchObjectKey := fs.toObjectKey(filepath.Join(installDir, MinioTouchFile))
	switch folderType {
	case babuzapb.SnapshotFolderType_TempWrite:
	case babuzapb.SnapshotFolderType_TempReceive:
		break
	default:
		return fmt.Errorf("invalid folder type: %v", folderType)
	}
	_, err = fs.client.CopyObject(context.Background(), minio.CopyDestOptions{
		Bucket: fs.bucket,
		Object: touchObjectKey,
		UserMetadata: map[string]string{
			MinioTouchMetadataKey: strconv.Itoa(int(babuzapb.SnapshotFolderType_InstallSnapshot)),
		},
		ReplaceMetadata: true,
	}, minio.CopySrcOptions{
		Bucket: fs.bucket,
		Object: touchObjectKey,
	})
	return err
}

func (fs *MinIOSnapshotFS) ExistFilePath(path string) bool {
	objKey := fs.toObjectKey(path)
	_, err := fs.client.StatObject(context.Background(), fs.bucket, objKey, minio.StatObjectOptions{})
	return err == nil
}

func (fs *MinIOSnapshotFS) ExistDir(path string) bool {
	dirKey := fs.toObjectKey(path)

	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: false,
		MaxKeys:   1,
	}

	for obj := range fs.client.ListObjects(context.Background(), fs.bucket, opts) {
		if obj.Err != nil {
			return false
		}
		return true
	}
	return false
}

func (fs *MinIOSnapshotFS) FileSize(path string) (int64, error) {
	objKey := fs.toObjectKey(path)
	info, err := fs.client.StatObject(context.Background(), fs.bucket, objKey, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

func (fs *MinIOSnapshotFS) SyncDir(path string) error {
	// not implemented in MinIO
	return nil
}

func (fs *MinIOSnapshotFS) SyncFile(path string) error {
	// not implemented in MinIO
	return nil
}

func (fs *MinIOSnapshotFS) RenameDir(oldPath string, newPath string) error {
	// not implemented in MinIO
	return nil
}

func (fs *MinIOSnapshotFS) RemoveDir(path string) error {
	dirKey := fs.toObjectKey(path)
	opts := minio.ListObjectsOptions{
		Prefix:    dirKey,
		Recursive: true,
	}
	ctx := context.Background()
	for obj := range fs.client.ListObjects(ctx, fs.bucket, opts) {
		if obj.Err != nil {
			return obj.Err
		}
		err := fs.client.RemoveObject(
			ctx,
			fs.bucket,
			obj.Key,
			minio.RemoveObjectOptions{},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs *MinIOSnapshotFS) RemoveFilePath(path string) error {
	objKey := fs.toObjectKey(path)
	return fs.client.RemoveObject(
		context.Background(),
		fs.bucket,
		objKey,
		minio.RemoveObjectOptions{},
	)
}

func (fs *MinIOSnapshotFS) PathHelper() api.PathHelper {
	return fs.ph
}
