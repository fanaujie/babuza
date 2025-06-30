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


package fileutil

import (
	"os"
	"syscall"
	"unsafe"
)

func allocate(f *os.File, offset, size int64) error {
	var fst syscall.Fstore_t

	fst.Flags = syscall.F_ALLOCATECONTIG
	fst.Posmode = syscall.F_PREALLOCATE
	fst.Offset = 0
	fst.Length = offset + size
	fst.Bytesalloc = 0

	// Check https://lists.apple.com/archives/darwin-dev/2007/Dec/msg00040.html
	_, _, err := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_PREALLOCATE, uintptr(unsafe.Pointer(&fst)))
	if err != syscall.Errno(0x0) {
		fst.Flags = syscall.F_ALLOCATEALL
		// Ignore the return value
		_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_PREALLOCATE, uintptr(unsafe.Pointer(&fst)))
	}
	return syscall.Ftruncate(int(f.Fd()), fst.Length)
}
