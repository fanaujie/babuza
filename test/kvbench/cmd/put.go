package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/client/kvgrpc"
	"github.com/fanaujie/babuza/test/kvbench/kvbenchpb"
	"github.com/fanaujie/babuza/test/kvbench/report"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"math"
	"math/rand"
	"os"
	"time"
)

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark write performance with PUT operations",
	Long: `Executes a configurable PUT workload to measure write performance metrics.

This command allows fine-tuning of the PUT benchmark with options including:
- Custom key and value sizes
- Sequential or random key generation patterns
- Adjustable operation rates and total operations count
- Key space size control for testing different data distribution patterns

Results include throughput (ops/sec), latency distribution (min/avg/p90/p99/max), 
and other performance metrics with optional time-series sampling.`,
	Run: putFunc,
}

var (
	keySize      int
	valSize      int
	putTotal     int
	putRate      int
	keySpaceSize int
	seqKeys      bool
)

func init() {
	ClientCmd.AddCommand(putCmd)

	// Put-specific flags
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request in bytes")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request in bytes")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")
	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	putCmd.Flags().BoolVar(&seqKeys, "sequential-keys", false, "Use sequential keys")
}

func putFunc(cmd *cobra.Command, args []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "Expected positive --key-space-size, got (%v)\n", keySpaceSize)
		os.Exit(1)
	}

	if shardCount > 1 && keySpaceSize < int(shardCount) {
		fmt.Fprintf(os.Stderr, "Expected --key-space-size >= --shard-count (%v < %v)\n", keySpaceSize, shardCount)
		os.Exit(1)
	}
	// Setup rate limiter
	if putRate == 0 {
		putRate = math.MaxInt32
	}

	// Create clients
	config := client.Config{
		Endpoints:    endpoints,
		Connections:  connections,
		TargetLeader: targetLeader,
		ShardCount:   shardCount,
	}
	factory, err := kvgrpc.NewGRPCFactory(clientClusterID, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client factory: %v\n", err)
		os.Exit(1)
	}
	defer factory.Close()

	// Create progress bar
	bar = pb.New(putTotal)
	bar.Start()

	// Initialize reporter
	reporter = newReporter()
	// for multiple shards
	if shardCount > 1 {
		shardClients := make(map[uint64][]client.Client)
		shardRequests := make(map[uint64]chan kvbenchpb.KvOP)

		clientsPerShard := totalClients / shardCount
		if clientsPerShard == 0 {
			clientsPerShard = 1
		}

		for i := uint64(1); i <= uint64(shardCount); i++ {
			shardRequests[i] = make(chan kvbenchpb.KvOP, 1024)
			var shardClientsList []client.Client

			for j := uint(0); j < clientsPerShard; j++ {
				shardClientsList = append(shardClientsList, factory.NewClient(config))
			}
			shardClients[i] = shardClientsList

			for _, c := range shardClientsList {
				wg.Add(1)
				go func(c client.Client, requests chan kvbenchpb.KvOP) {
					defer wg.Done()
					limit := rate.NewLimiter(rate.Limit(putRate), 1)

					for op := range requests {
						// Wait for rate limiter
						limit.Wait(context.Background())
						ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
						res := c.Put(ctx, op.GroupID, op.Key, op.Value)
						if res.Error != nil {
							cancel()
							fmt.Fprintf(os.Stderr, "Error in PUT operation: %v\n", res.Error)
						}
						cancel()
						reporter.Results() <- report.Result{
							Err:   res.Error,
							Start: res.StartTime,
							End:   res.EndTime,
						}
						bar.Increment()
					}
				}(c, shardRequests[i])
			}
		}
		go func() {
			for i := 0; i < putTotal; i++ {
				keyTemplate := make([]byte, keySize)
				var groupID uint64
				if seqKeys {
					rKey := i % keySpaceSize
					groupID = uint64(uint(rKey)%shardCount) + 1
					binary.PutVarint(keyTemplate, int64(rKey))
				} else {
					rKey := rand.Intn(keySpaceSize)
					groupID = uint64(uint(rKey)%shardCount) + 1
					binary.PutVarint(keyTemplate, int64(rKey))
				}
				shardRequests[groupID] <- kvbenchpb.KvOP{
					GroupID: groupID,
					Key:     keyTemplate,
					Value:   mustRandBytes(valSize),
				}
			}

			for i := uint64(1); i <= uint64(shardCount); i++ {
				close(shardRequests[i])
			}
		}()
	} else { // for single shard
		requests := make(chan kvbenchpb.KvOP, 1024)
		limit := rate.NewLimiter(rate.Limit(putRate), 1)

		var clients []client.Client
		for i := uint(0); i < totalClients; i++ {
			clients = append(clients, factory.NewClient(config))
		}

		// Start worker goroutines
		for i := range clients {
			wg.Add(1)
			go func(c client.Client) {
				defer wg.Done()

				for op := range requests {
					// Wait for rate limiter
					limit.Wait(context.Background())

					res := c.Put(context.Background(), op.GroupID, op.Key, op.Value)
					if res.Error != nil {
						fmt.Fprintf(os.Stderr, "Error in PUT operation: %v\n", res.Error)
					}
					reporter.Results() <- report.Result{
						Err:   res.Error,
						Start: res.StartTime,
						End:   res.EndTime,
					}
					bar.Increment()
				}
			}(clients[i])
		}

		// Generate requests
		go func() {
			for i := 0; i < putTotal; i++ {
				keyTemplate := make([]byte, keySize)
				if seqKeys {
					binary.PutVarint(keyTemplate, int64(i%keySpaceSize))
				} else {
					binary.PutVarint(keyTemplate, int64(rand.Intn(keySpaceSize)))
				}
				requests <- kvbenchpb.KvOP{
					GroupID: 0,
					Key:     keyTemplate,
					Value:   mustRandBytes(valSize),
				}
			}
			close(requests)
		}()
	}

	// Wait for report completion
	rc := reporter.Run()

	// Wait for workers to complete
	wg.Wait()

	// Finalize reporter and progress bar
	close(reporter.Results())
	bar.Finish()

	// Print the report
	result := <-rc
	fmt.Println(result)
}
