package fileutil

import (
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestExist(t *testing.T) {

	d, _ := os.MkdirTemp("", "test")
	if Exist(d) == false {
		t.Fatal()
	}
	defer os.RemoveAll(d)

	f, _ := os.CreateTemp(d, "test")
	if Exist(f.Name()) == false {
		t.Fatal()
	}
	h := fnv.New32a()
	h.Write([]byte("HelloWorld"))
	if Exist(filepath.Join(d, strconv.Itoa(int(h.Sum32())))) {
		t.Fatal()
	}

}

func TestCanDirWriteable(t *testing.T) {
	tmpD, _ := os.MkdirTemp("", "test1")
	defer os.RemoveAll(tmpD)

	type testDirPermission struct {
		fileMode os.FileMode
		success  bool
	}

	testCase := []testDirPermission{
		{0500, false}, {0300, true}, {0700, true},
	}
	for i, p := range testCase {
		testDir := filepath.Join(tmpD, "test"+strconv.Itoa(i))
		if err := os.Mkdir(testDir, p.fileMode); err != nil {
			t.Fatal(err)
		}
		err := IsDirWriteable(testDir)
		if p.success && err != nil {
			t.Fatalf("test case = %d, expected success but fial. err=%s", i, err)
		} else if p.success == false && err == nil {
			t.Fatalf("test case = %d, expected fail but success", i)
		}
	}
}

func TestCreateDirAndTouch(t *testing.T) {
	d1, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(d1)

	p := filepath.Join(d1, "hello")
	_ = os.WriteFile(p, []byte("hello"), FileMode)
	if err := CreateDirAndTouch(d1); err == nil {
		t.Fatal()
	}

	d2, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(d1)

	if err := CreateDirAndTouch(filepath.Join(d2, "hello")); err != nil {
		t.Fatal(err)
	}
}

func TestPreAllocateFileSpace(t *testing.T) {
	d, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(d)

	path := filepath.Join(d, "hello.tmp")
	var allocateSize int64 = 1024 * 1024
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, FileMode)
	if err != nil {
		t.Fatal(err)
	}

	if err = AllocateFileSpace(f, allocateSize, 0); err != nil {
		t.Fatal(err)
	}

	//err = unix.Fdatasync(int(f.Fd()))
	//if err != nil {
	//	t.Fatal("unable to Fdatasync file:", err)
	//}
	f.Close()
}
