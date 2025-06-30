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


package connpool

type Connection interface {
	Close() error
}

type ComparableConnection interface {
	Connection
	comparable
}

type ConnectionDialer[T ComparableConnection] interface {
	Dial(address string) (T, error)
}

type Pool[T ComparableConnection] interface {
	Get(address string) (T, error)
	Put(conn T) error
	Remove(conn T) error
	Close() error
	GetActiveConnectionCount(address string) int
	GetIdleConnectionCount(address string) int
}
