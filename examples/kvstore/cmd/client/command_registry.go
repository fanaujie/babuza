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

package client

import (
	"github.com/fanaujie/babuza/examples/kvstore/client"
)

type CommandRegistry interface {
	RegisterCommands(processor *CommandProcessor, kvClient *client.KvStoreClient)
}

type DefaultCommandRegistry struct{}

func NewDefaultCommandRegistry() CommandRegistry {
	return &DefaultCommandRegistry{}
}

func (r *DefaultCommandRegistry) RegisterCommands(processor *CommandProcessor, kvClient *client.KvStoreClient) {
	processor.AddCommand("exit", NewExitCommand())
	processor.AddCommand("join", NewJoinCommand(kvClient))
	processor.AddCommand("set", NewSetCommand(kvClient))
	processor.AddCommand("get", NewGetCommand(kvClient))
	processor.AddCommand("delete", NewDeleteCommand(kvClient))
	processor.AddCommand("append", NewAppendCommand(kvClient))
	processor.AddCommand("remove", NewRemoveCommand(kvClient))
	processor.AddCommand("cluster", NewClusterCommand(kvClient))
	processor.AddCommand("help", NewHelpCommand(processor))
}