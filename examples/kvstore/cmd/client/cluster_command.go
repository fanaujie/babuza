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
	"context"
	"fmt"
	"github.com/fanaujie/babuza/examples/kvstore/client"
	"github.com/fanaujie/babuza/examples/kvstore/server/response"
	"strings"
)

type ClusterCommand struct {
	kvClient *client.KvStoreClient
}

func NewClusterCommand(kvClient *client.KvStoreClient) *ClusterCommand {
	return &ClusterCommand{
		kvClient: kvClient,
	}
}

func (cc *ClusterCommand) Execute(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("cluster command accepts no arguments, got %d", len(args))
	}

	clusterConfig, err := cc.kvClient.GetClusterConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get cluster configuration: %w", err)
	}

	// Get current client session information
	session, err := cc.kvClient.Session()
	if err != nil {
		return fmt.Errorf("failed to get client session: %w", err)
	}

	cc.printClusterInfo(clusterConfig, session)
	return nil
}

func (cc *ClusterCommand) printClusterInfo(config *response.ClusterConfigurationResponse, session client.Session) {
	fmt.Println("Cluster Configuration:")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Leader ID:        %d\n", config.LeaderID)
	fmt.Printf("Session ID:       %d\n", session.SessionID)
	fmt.Printf("Sequence Number:  %d\n", session.SequenceNumber)
	fmt.Printf("Total Peers:      %d\n", len(config.Peers))
	fmt.Println()

	if len(config.Peers) == 0 {
		fmt.Println("No peers found in cluster.")
		return
	}

	fmt.Println("Cluster Members:")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-8s %-20s %-10s %-30s\n", "ID", "Raft Address", "Role", "App Service Address")
	fmt.Println(strings.Repeat("-", 80))

	for _, peer := range config.Peers {
		role := "Voter"
		if peer.IsLearner {
			role = "Learner"
		}
		
		leaderMark := ""
		if peer.Id == config.LeaderID {
			leaderMark = " (Leader)"
			role += leaderMark
		}

		fmt.Printf("%-8d %-20s %-10s %-30s\n", 
			peer.Id, 
			peer.RaftListenAddr, 
			role, 
			peer.AppServiceAddress)
	}
	fmt.Println(strings.Repeat("-", 80))
}

func (cc *ClusterCommand) Help() string {
	return "cluster - Display current cluster configuration and member information"
}