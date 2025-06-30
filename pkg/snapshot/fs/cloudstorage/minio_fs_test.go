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
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/snapshot/fs/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"io"
	"path/filepath"
	"testing"
)

func setupMinioContainer(t *testing.T) (*minio.MinioContainer, string, string, string) {

	// Start MinIO container
	minioContainer, err := minio.Run(context.Background(), "minio/minio:latest",
		minio.WithUsername("minioroot"), minio.WithPassword("miniopassword"))
	require.NoError(t, err)

	// Get connection details
	endpoint, err := minioContainer.ConnectionString(context.Background())
	require.NoError(t, err)

	return minioContainer, endpoint, minioContainer.Username, minioContainer.Password
}

func createMinIOFS(t *testing.T, endpoint, accessKey, secretKey string) api.SnapshotFileSystem {
	config := Config{
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		UseSSL:          false,
		Bucket:          "test-bucket",
		Prefix:          "test-prefix",
	}

	fs, err := NewMinioSnapshotFS(config)
	require.NoError(t, err)
	return fs
}

// generateTestData creates a byte slice of the specified size filled with the given character
func generateTestData(size int, fillChar byte) []byte {
	data := bytes.Repeat([]byte{fillChar}, size)
	return data
}

func TestNewMinioSnapshotFS(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	// Test that we can create a new MinIO FS
	fs := createMinIOFS(t, endpoint, accessKey, secretKey)
	assert.NotNil(t, fs)
}

func TestMinIOSnapshotFS_FileReadWrite(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test file write
	testContent := "Hello, MinIO!"
	testPath := "test-file.txt"

	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte(testContent))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Test file read
	reader, err := fs.FileRead(testPath)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)

	assert.Equal(t, testContent, string(content))
}

func TestMinIOSnapshotFS_CrcFileReadWrite(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test CRC file write
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

	// Test CRC file read
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

func TestMinIOSnapshotFS_CreateDirAndTouch(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test creating a directory and touch file
	snapshotDir := "snapshot-dir"
	folderType := babuzapb.SnapshotFolderType_InstallSnapshot
	snapIndex := uint64(123)

	_, err := fs.CreateDirAndTouch(snapshotDir, folderType, snapIndex)
	require.NoError(t, err)

	// Verify the directory exists by checking for the touch file
	path, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, folderType, snapIndex)
	require.NoError(t, err)
	touchFilePath := filepath.Join(path, MinioTouchFile)

	exists := fs.ExistFilePath(touchFilePath)
	assert.True(t, exists)

	// Try to create it again, should fail with "already exists" error
	_, err = fs.CreateDirAndTouch(snapshotDir, folderType, snapIndex)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestMinIOSnapshotFS_FileAppendOperations(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test file append operations
	testPath := "appended-file.txt"

	// Create data chunks of at least 5MB each
	chunkSize := 5 * 1024 * 1024 // 5MB

	// Generate 3 different chunks with unique patterns
	chunk1 := generateTestData(chunkSize, 'A')
	chunk2 := generateTestData(chunkSize, 'B')
	chunk3 := generateTestData(chunkSize, 'C')

	// Append data in chunks
	err := fs.FileAppendData(testPath, 1, chunk1)
	require.NoError(t, err)
	err = fs.FileAppendData(testPath, 2, chunk2)
	require.NoError(t, err)
	err = fs.FileAppendData(testPath, 3, chunk3)
	require.NoError(t, err)

	// Finalize the append operation
	err = fs.FileAppendFinalize(testPath, 3)
	require.NoError(t, err)

	// Verify the total file size
	size, err := fs.FileSize(testPath)
	require.NoError(t, err)
	expectedSize := int64(3 * chunkSize)
	assert.Equal(t, expectedSize, size, "File size should be 15MB (3 chunks of 5MB each)")

	// Read the beginning of the file to verify it contains the expected pattern
	reader, err := fs.FileRead(testPath)
	require.NoError(t, err)
	deferFunc := func() {
		reader.Close()
	}
	defer deferFunc()

	// Sample the beginning of the file to verify it starts with 'A's (first chunk)
	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	// Check that the first 1024 bytes are all 'A'
	for i := 0; i < 1024; i++ {
		assert.Equal(t, byte('A'), buffer[i], "First chunk should contain 'A' characters")
	}
}

func TestMinIOSnapshotFS_ScanInstalledSnapshot(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create multiple snapshot directories
	snapshotDir := "snapshot-scan-dir"
	snapIndex1 := uint64(100)
	snapIndex2 := uint64(200)

	// Create installed snapshots
	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex1)
	require.NoError(t, err)
	_, err = fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_InstallSnapshot, snapIndex2)
	require.NoError(t, err)

	// Test scanning for installed snapshots
	indices, err := fs.ScanInstalledSnapshot(snapshotDir)
	require.NoError(t, err)
	assert.Len(t, indices, 2)
	assert.Contains(t, indices, snapIndex1)
	assert.Contains(t, indices, snapIndex2)
}

func TestMinIOSnapshotFS_ScanTempSnapshotFolder(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create temp snapshot directories
	snapshotDir := "snapshot-temp-dir"
	tempWriteIndex := uint64(300)
	tempReceiveIndex := uint64(400)

	// Create temp writer snapshot
	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempWriteIndex)
	require.NoError(t, err)

	// Create temp receiver snapshot
	_, err = fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempReceive, tempReceiveIndex)
	require.NoError(t, err)

	// Test scanning for temp snapshots
	tmpWriter, tmpReceiver, err := fs.ScanTempSnapshotFolder(snapshotDir)
	require.NoError(t, err)

	// Get the expected folder paths
	writePath, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempWriteIndex)
	require.NoError(t, err)
	receivePath, err := fs.PathHelper().GenerateSnapshotFolderPath(snapshotDir, babuzapb.SnapshotFolderType_TempReceive, tempReceiveIndex)
	require.NoError(t, err)

	assert.Len(t, tmpWriter, 1)
	assert.Len(t, tmpReceiver, 1)
	assert.Contains(t, tmpWriter, writePath)
	assert.Contains(t, tmpReceiver, receivePath)
}

func TestMinIOSnapshotFS_InstallSnapshotFromTempFolder(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create a temp write snapshot
	snapshotDir := "snapshot-install-dir"
	tempIndex := uint64(500)

	_, err := fs.CreateDirAndTouch(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempIndex)
	require.NoError(t, err)

	// Install the snapshot from temp folder
	err = fs.InstallSnapshotFromTempFolder(snapshotDir, babuzapb.SnapshotFolderType_TempWrite, tempIndex)
	require.NoError(t, err)

	// Verify the snapshot is now installed
	indices, err := fs.ScanInstalledSnapshot(snapshotDir)
	require.NoError(t, err)
	assert.Contains(t, indices, tempIndex)
}

func TestMinIOSnapshotFS_ExistFilePathAndDir(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test ExistFilePath for non-existent file
	exists := fs.ExistFilePath("non-existent-file.txt")
	assert.False(t, exists)

	// Create a file
	testPath := "exist-test-file.txt"
	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte("Test content"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Test ExistFilePath for existing file
	exists = fs.ExistFilePath(testPath)
	assert.True(t, exists)

	// Test ExistDir for a directory with files
	dirPath := "exist-test-dir"

	// Create a file in the directory
	filePath := filepath.Join(dirPath, "test.txt")
	writer, err = fs.FileWrite(filePath)
	require.NoError(t, err)
	_, err = writer.Write([]byte("Test content"))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Check if directory exists
	exists = fs.ExistDir(dirPath)
	assert.True(t, exists)

	// Check a non-existent directory
	exists = fs.ExistDir("non-existent-dir")
	assert.False(t, exists)
}

func TestMinIOSnapshotFS_FileSizeAndRemoveFile(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create a test file
	testPath := "filesize-test.txt"
	testContent := "File size test content"

	writer, err := fs.FileWrite(testPath)
	require.NoError(t, err)
	_, err = writer.Write([]byte(testContent))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Check file size
	size, err := fs.FileSize(testPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testContent)), size)

	// Remove the file
	err = fs.RemoveFilePath(testPath)
	require.NoError(t, err)

	// Verify file no longer exists
	exists := fs.ExistFilePath(testPath)
	assert.False(t, exists)

	// Try to get file size for non-existent file
	_, err = fs.FileSize(testPath)
	assert.Error(t, err)
}

func TestMinIOSnapshotFS_RemoveDir(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create a directory with multiple files
	dirPath := "remove-dir-test"
	file1Path := filepath.Join(dirPath, "file1.txt")
	file2Path := filepath.Join(dirPath, "file2.txt")
	subDirPath := filepath.Join(dirPath, "subdir")
	file3Path := filepath.Join(subDirPath, "file3.txt")

	// Create the files
	for _, path := range []string{file1Path, file2Path, file3Path} {
		writer, err := fs.FileWrite(path)
		require.NoError(t, err)
		_, err = writer.Write([]byte("Test content"))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)
	}

	// Verify directory exists
	exists := fs.ExistDir(dirPath)
	assert.True(t, exists)

	// Remove the directory
	err := fs.RemoveDir(dirPath)
	require.NoError(t, err)

	// Verify directory no longer exists
	exists = fs.ExistDir(dirPath)
	assert.False(t, exists)

	// Verify files no longer exist
	for _, path := range []string{file1Path, file2Path, file3Path} {
		exists = fs.ExistFilePath(path)
		assert.False(t, exists)
	}
}

func TestMinIOSnapshotFS_FindMetadataFile(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Create metadata files
	dirPath := "metadata-dir"
	index1 := uint64(1000)
	index2 := uint64(2000)

	// Generate metadata file paths
	file1, err := fs.PathHelper().GenerateSnapshotFilePath(dirPath, babuzapb.SnapshotFileType_Metadata, index1, "")
	require.NoError(t, err)
	file2, err := fs.PathHelper().GenerateSnapshotFilePath(dirPath, babuzapb.SnapshotFileType_Metadata, index2, "")
	require.NoError(t, err)

	// Create the files
	for _, path := range []string{file1, file2} {
		writer, err := fs.FileWrite(path)
		require.NoError(t, err)
		_, err = writer.Write([]byte("Metadata content"))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)
	}

	// Test finding metadata files
	indices, err := fs.FindMetadataFile(dirPath)
	require.NoError(t, err)

	assert.Len(t, indices, 2)
	assert.Contains(t, indices, index1)
	assert.Contains(t, indices, index2)
}

func TestMinIOSnapshotFS_PathHelper(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Verify the PathHelper is properly initialized
	ph := fs.PathHelper()
	assert.NotNil(t, ph)

	// Check PathHelper methods
	assert.Equal(t, "snapshot", ph.SnapshotFolderPrefix())
	assert.Equal(t, "snapshot", ph.TempWriterFolderPrefix())
	assert.Equal(t, "snapshot", ph.TempReceiverFolderPrefix())
}

func TestMinIOSnapshotFS_NotImplementedFunctions(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	fs := createMinIOFS(t, endpoint, accessKey, secretKey)

	// Test functions that are not implemented in MinIO
	err := fs.SyncDir("any-path")
	assert.NoError(t, err)

	err = fs.SyncFile("any-path")
	assert.NoError(t, err)

	err = fs.RenameDir("old-path", "new-path")
	assert.NoError(t, err)
}

func TestMinIOSnapshotFS_ObjectKeyConversion(t *testing.T) {
	minioContainer, endpoint, accessKey, secretKey := setupMinioContainer(t)
	defer func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	// Create a MinIO FS with a specific prefix
	config := Config{
		Endpoint:        endpoint,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		UseSSL:          false,
		Bucket:          "test-bucket",
		Prefix:          "custom-prefix",
	}

	fs, err := NewMinioSnapshotFS(config)
	require.NoError(t, err)
	minioFS := fs.(*MinIOSnapshotFS)

	// Test toObjectKey
	path := "test/path.txt"
	objectKey := minioFS.toObjectKey(path)
	assert.Equal(t, "custom-prefix/test/path.txt", objectKey)

	// Test fromObjectKey
	originalPath := minioFS.fromObjectKey(objectKey)
	assert.Equal(t, path, originalPath)
}
