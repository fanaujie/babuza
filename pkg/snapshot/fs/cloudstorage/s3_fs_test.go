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
	"io"
	"path/filepath"
	"testing"

	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rustfsImage    = "rustfs/rustfs:latest"
	rustfsUsername = "rustfsadmin"
	rustfsPassword = "rustfsadmin"
)

func setupRustFSContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        rustfsImage,
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": rustfsUsername,
			"RUSTFS_SECRET_KEY": rustfsPassword,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "9000")
	require.NoError(t, err)

	endpoint := "http://" + host + ":" + mappedPort.Port()
	return container, endpoint
}

func createS3FS(t *testing.T, endpoint string) api.SnapshotFileSystem {
	config := S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     rustfsUsername,
		SecretAccessKey: rustfsPassword,
		UsePathStyle:    true,
		Bucket:          "test-bucket",
		Prefix:          "test-prefix",
	}

	fs, err := NewS3SnapshotFS(config)
	require.NoError(t, err)
	return fs
}

func generateS3TestData(size int, fillChar byte) []byte {
	return bytes.Repeat([]byte{fillChar}, size)
}

func TestNewS3SnapshotFS(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)
	assert.NotNil(t, fs)
}

func TestS3SnapshotFS_FileReadWrite(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	testContent := "Hello, S3!"
	testPath := "test-file.txt"

	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte(testContent))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := fs.FileRead(testPath)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)

	assert.Equal(t, testContent, string(content))
}

func TestS3SnapshotFS_CrcFileReadWrite(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	testContent := "Hello, CRC!"
	testPath := "test-crc-file.txt"

	writer, err := fs.CrcFileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte(testContent))
	require.NoError(t, err)
	writerCrc := writer.Crc()
	writerSize := writer.FileSize()
	err = writer.Close()
	require.NoError(t, err)

	reader, err := fs.CrcFileRead(testPath)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	readerCrc := reader.Crc()
	readerSize := reader.FileSize()
	err = reader.Close()
	require.NoError(t, err)

	assert.Equal(t, testContent, string(content))
	assert.Equal(t, writerCrc, readerCrc)
	assert.Equal(t, writerSize, readerSize)
}

func TestS3SnapshotFS_CreateDirAndTouch(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	snapshotDir := "snapshot-dir"
	folderType := babuzapb.SnapshotFolderType_InstallSnapshot
	snapIndex := uint64(123)

	_, err := fs.CreateDirAndTouch(snapshotDir, folderType, snapIndex)
	require.NoError(t, err)

	path, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	require.NoError(t, err)
	touchFilePath := filepath.Join(path, S3TouchFile)

	exists := fs.ExistFilePath(touchFilePath)
	assert.True(t, exists)

	// Try to create it again, should fail with "already exists" error
	_, err = fs.CreateDirAndTouch(snapshotDir, folderType, snapIndex)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestS3SnapshotFS_FileAppendOperations(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	testPath := "appended-file.txt"

	// Create data chunks of at least 5MB each (S3 multipart requirement)
	chunkSize := 5 * 1024 * 1024

	chunk1 := generateS3TestData(chunkSize, 'A')
	chunk2 := generateS3TestData(chunkSize, 'B')
	chunk3 := generateS3TestData(chunkSize, 'C')

	err := fs.FileAppendData(testPath, 1, chunk1)
	require.NoError(t, err)
	err = fs.FileAppendData(testPath, 2, chunk2)
	require.NoError(t, err)
	err = fs.FileAppendData(testPath, 3, chunk3)
	require.NoError(t, err)

	err = fs.FileAppendFinalize(testPath, 3)
	require.NoError(t, err)

	size, err := fs.FileSize(testPath)
	require.NoError(t, err)
	expectedSize := int64(3 * chunkSize)
	assert.Equal(t, expectedSize, size, "File size should be 15MB (3 chunks of 5MB each)")

	reader, err := fs.FileRead(testPath)
	require.NoError(t, err)
	defer reader.Close()

	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	for i := 0; i < 1024; i++ {
		assert.Equal(t, byte('A'), buffer[i], "First chunk should contain 'A' characters")
	}
}

func TestS3SnapshotFS_ScanInstalledSnapshot(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	snapshotDir := "snapshot-scan-dir"
	snapIndex1 := uint64(100)
	snapIndex2 := uint64(200)

	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex1)
	require.NoError(t, err)
	_, err = fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex2)
	require.NoError(t, err)

	indices, err := fs.ScanInstalledSnapshot(snapshotDir)
	require.NoError(t, err)
	assert.Len(t, indices, 2)
	assert.Contains(t, indices, snapIndex1)
	assert.Contains(t, indices, snapIndex2)
}

func TestS3SnapshotFS_ScanTempSnapshotFolder(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	snapshotDir := "snapshot-temp-dir"
	tempWriteIndex := uint64(300)
	tempReceiveIndex := uint64(400)

	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempWriteIndex)
	require.NoError(t, err)

	_, err = fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempReceive, tempReceiveIndex)
	require.NoError(t, err)

	tmpWriter, tmpReceiver, err := fs.ScanTempSnapshotFolder(snapshotDir)
	require.NoError(t, err)

	writePath, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempWriteIndex)
	require.NoError(t, err)
	receivePath, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, babuzapb.SnapshotFolderType_TempReceive, tempReceiveIndex)
	require.NoError(t, err)

	assert.Len(t, tmpWriter, 1)
	assert.Len(t, tmpReceiver, 1)
	assert.Contains(t, tmpWriter, writePath)
	assert.Contains(t, tmpReceiver, receivePath)
}

func TestS3SnapshotFS_InstallSnapshotFromTempFolder(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	snapshotDir := "snapshot-install-dir"
	tempIndex := uint64(500)

	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempIndex)
	require.NoError(t, err)

	err = fs.InstallSnapshotFromTempFolder(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempIndex)
	require.NoError(t, err)

	indices, err := fs.ScanInstalledSnapshot(snapshotDir)
	require.NoError(t, err)
	assert.Contains(t, indices, tempIndex)
}

func TestS3SnapshotFS_ExistFilePathAndDir(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	exists := fs.ExistFilePath("non-existent-file.txt")
	assert.False(t, exists)

	testPath := "exist-test-file.txt"
	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte("Test content"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	exists = fs.ExistFilePath(testPath)
	assert.True(t, exists)

	dirPath := "exist-test-dir"

	filePath := filepath.Join(dirPath, "test.txt")
	writer, err = fs.FileWrite(filePath)
	require.NoError(t, err)
	_, err = writer.Write([]byte("Test content"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	exists = fs.ExistDir(dirPath)
	assert.True(t, exists)

	exists = fs.ExistDir("non-existent-dir")
	assert.False(t, exists)
}

func TestS3SnapshotFS_FileSizeAndRemoveFile(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	testPath := "filesize-test.txt"
	testContent := "File size test content"

	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte(testContent))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	size, err := fs.FileSize(testPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testContent)), size)

	err = fs.RemoveFilePath(testPath)
	require.NoError(t, err)

	exists := fs.ExistFilePath(testPath)
	assert.False(t, exists)

	_, err = fs.FileSize(testPath)
	assert.Error(t, err)
}

func TestS3SnapshotFS_RemoveDir(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	dirPath := "remove-dir-test"
	file1Path := filepath.Join(dirPath, "file1.txt")
	file2Path := filepath.Join(dirPath, "file2.txt")
	subDirPath := filepath.Join(dirPath, "subdir")
	file3Path := filepath.Join(subDirPath, "file3.txt")

	for _, path := range []string{file1Path, file2Path, file3Path} {
		writer, err := fs.FileWrite(path)
		require.NoError(t, err)
		_, err = writer.Write([]byte("Test content"))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)
	}

	exists := fs.ExistDir(dirPath)
	assert.True(t, exists)

	err := fs.RemoveDir(dirPath)
	require.NoError(t, err)

	exists = fs.ExistDir(dirPath)
	assert.False(t, exists)

	for _, path := range []string{file1Path, file2Path, file3Path} {
		exists = fs.ExistFilePath(path)
		assert.False(t, exists)
	}
}

func TestS3SnapshotFS_FindMetadataFile(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	dirPath := "metadata-dir"
	index1 := uint64(1000)
	index2 := uint64(2000)

	file1, err := fs.PathHelper().GenerateSnapshotFilePath(dirPath, babuzapb.SnapshotFileType_Metadata, index1, "")
	require.NoError(t, err)
	file2, err := fs.PathHelper().GenerateSnapshotFilePath(dirPath, babuzapb.SnapshotFileType_Metadata, index2, "")
	require.NoError(t, err)

	for _, path := range []string{file1, file2} {
		writer, err := fs.FileWrite(path)
		require.NoError(t, err)
		_, err = writer.Write([]byte("Metadata content"))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)
	}

	indices, err := fs.FindMetadataFile(dirPath)
	require.NoError(t, err)

	assert.Len(t, indices, 2)
	assert.Contains(t, indices, index1)
	assert.Contains(t, indices, index2)
}

func TestS3SnapshotFS_PathHelper(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	ph := fs.PathHelper()
	assert.NotNil(t, ph)

	assert.Equal(t, "snapshot", ph.SnapshotFolderPrefix())
	assert.Equal(t, "snapshot", ph.TempWriterFolderPrefix())
	assert.Equal(t, "snapshot", ph.TempReceiverFolderPrefix())
}

func TestS3SnapshotFS_NotImplementedFunctions(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createS3FS(t, endpoint)

	err := fs.SyncDir("any-path")
	assert.NoError(t, err)

	err = fs.SyncFile("any-path")
	assert.NoError(t, err)

	err = fs.RenameDir("old-path", "new-path")
	assert.NoError(t, err)
}

func TestS3SnapshotFS_ObjectKeyConversion(t *testing.T) {
	container, endpoint := setupRustFSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	config := S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     rustfsUsername,
		SecretAccessKey: rustfsPassword,
		UsePathStyle:    true,
		Bucket:          "test-bucket",
		Prefix:          "custom-prefix",
	}

	s3fs, err := NewS3SnapshotFS(config)
	require.NoError(t, err)

	path := "test/path.txt"
	objectKey := s3fs.toObjectKey(path)
	assert.Equal(t, "custom-prefix/test/path.txt", objectKey)

	originalPath := s3fs.fromObjectKey(objectKey)
	assert.Equal(t, path, originalPath)
}
