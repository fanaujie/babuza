package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/fanaujie/babuza/test/kvbench/client"
	"github.com/fanaujie/babuza/test/kvbench/client/dummy"
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
	Short: "Benchmark put operations",
	Long: `Benchmark put operations on the key-value service.
This command allows configuration of key size, value size, and rate limiting.`,
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
	RootCmd.AddCommand(putCmd)

	// Put-specific flags
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request in bytes")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request in bytes")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")
	putCmd.Flags().IntVar(&putTotal, "total", 10000, "Total number of put requests")
	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1000, "Maximum possible keys")
	putCmd.Flags().BoolVar(&seqKeys, "sequential-keys", false, "Use sequential keys")
}

func putFunc(cmd *cobra.Command, args []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "Expected positive --key-space-size, got (%v)\n", keySpaceSize)
		os.Exit(1)
	}

	// Setup rate limiter
	if putRate == 0 {
		putRate = math.MaxInt32
	}

	// Create a channel for operations
	requests := make(chan struct{}, totalClients)
	limit := rate.NewLimiter(rate.Limit(putRate), 1)

	// Create clients
	clients := mustCreateClients(dummy.NewDummyFactory(time.Millisecond * 100))

	// Generate key and value templates
	keyTemplate := make([]byte, keySize)
	value := string(mustRandBytes(valSize))

	// Create progress bar
	bar = pb.New(putTotal)
	bar.Start()

	// Initialize reporter
	reporter = newReporter()

	// Start worker goroutines
	for i := range clients {
		wg.Add(1)
		go func(c client.Client) {
			defer wg.Done()
			for range requests {
				// Wait for rate limiter
				limit.Wait(context.Background())

				// Generate key
				key := make([]byte, keySize)
				copy(key, keyTemplate)

				// Add request context
				ctx, cancel := context.WithTimeout(context.Background(), reqTimeout)

				startTime := time.Now()
				// Execute the put operation
				res := c.Put(ctx, key, []byte(value))

				// Send result to reporter
				reporter.Results() <- report.Result{
					Err:   res.Error,
					Start: startTime,
					End:   res.EndTime,
				}

				// Update progress bar
				bar.Increment()

				// Clean up
				cancel()
			}
		}(clients[i])
	}

	// Generate requests
	go func() {
		for i := 0; i < putTotal; i++ {
			// Generate a key based on sequential or random strategy
			if seqKeys {
				binary.PutVarint(keyTemplate, int64(i%keySpaceSize))
			} else {
				binary.PutVarint(keyTemplate, int64(rand.Intn(keySpaceSize)))
			}

			// Send request
			requests <- struct{}{}
		}
		close(requests)
	}()

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

	// Clean up
	for _, c := range clients {
		c.Close()
	}
}
