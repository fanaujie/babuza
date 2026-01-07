package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fanaujie/babuza/examples/distlock/server/lockstore"
	"github.com/fanaujie/babuza/examples/distlock/server/request"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var serverAddress string

func NewClientCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var clientCommand = &cobra.Command{
		Use:   "client",
		Short: "run interactive lock client",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runClient()
		},
	}

	clientCommand.Flags().StringVar(&serverAddress, "server", "localhost:24200",
		"Lock server address")

	return clientCommand
}

func runClient() error {
	fmt.Println("Welcome to DistLock CLI!")
	fmt.Println("Type 'help' to see available commands.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]
		args := parts[1:]

		switch cmd {
		case "lease-grant":
			handleLeaseGrant(args)
		case "lease-revoke":
			handleLeaseRevoke(args)
		case "lease-keepalive":
			handleLeaseKeepAlive(args)
		case "lease-status":
			handleLeaseStatus(args)
		case "acquire":
			handleAcquire(args)
		case "release":
			handleRelease(args)
		case "wait":
			handleWait(args)
		case "status":
			handleStatus(args)
		case "help":
			printHelp()
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return nil
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			printHelp()
		}
	}

	return scanner.Err()
}

func handleLeaseGrant(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: lease-grant <ttl_seconds>")
		return
	}

	ttl, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		fmt.Printf("Invalid TTL: %s\n", args[0])
		return
	}

	req := request.LeaseGrantRequest{
		TTLSeconds: ttl,
	}

	body, _ := json.Marshal(req)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/leases", serverAddress),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result lockstore.LeaseResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		return
	}

	ttlRemaining := time.Duration(result.ExpiresAt-time.Now().UnixNano()) / time.Second
	fmt.Printf("Lease granted: lease_id=%d, ttl=%ds, expires_in=%ds\n", result.LeaseID, result.TTL, ttlRemaining)
}

func handleLeaseRevoke(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: lease-revoke <lease_id>")
		return
	}

	leaseID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Printf("Invalid lease_id: %s\n", args[0])
		return
	}

	req := request.LeaseRevokeRequest{
		LeaseID: leaseID,
	}

	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/leases", serverAddress), bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var result lockstore.LeaseResult
		json.NewDecoder(resp.Body).Decode(&result)
		if len(result.ReleasedLocks) > 0 {
			fmt.Printf("Lease revoked, released locks: %v\n", result.ReleasedLocks)
		} else {
			fmt.Println("Lease revoked")
		}
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Failed to revoke lease: %s\n", string(respBody))
	}
}

func handleLeaseKeepAlive(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: lease-keepalive <lease_id>")
		return
	}

	leaseID, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Printf("Invalid lease_id: %s\n", args[0])
		return
	}

	req := request.LeaseKeepAliveRequest{
		LeaseID: leaseID,
	}

	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/leases", serverAddress), bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var result lockstore.LeaseResult
		json.NewDecoder(resp.Body).Decode(&result)
		ttlRemaining := time.Duration(result.ExpiresAt-time.Now().UnixNano()) / time.Second
		fmt.Printf("Lease renewed, expires_in=%ds\n", ttlRemaining)
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Failed to keep alive lease: %s\n", string(respBody))
	}
}

func handleLeaseStatus(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: lease-status <lease_id>")
		return
	}

	leaseID := args[0]

	resp, err := http.Get(fmt.Sprintf("http://%s/leases?id=%s", serverAddress, leaseID))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("Lease not found")
		return
	}

	var result lockstore.LeaseResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		return
	}

	ttlRemaining := time.Duration(result.ExpiresAt-time.Now().UnixNano()) / time.Second
	fmt.Printf("Lease: %d, TTL: %ds, Remaining: %ds, Locks: %v\n",
		result.LeaseID, result.TTL, ttlRemaining, result.Locks)
}

func handleAcquire(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: acquire <lock_name> <owner_id> <lease_id>")
		return
	}

	leaseID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		fmt.Printf("Invalid lease_id: %s\n", args[2])
		return
	}

	req := request.LockAcquireRequest{
		LockName: args[0],
		OwnerID:  args[1],
		LeaseID:  leaseID,
	}

	body, _ := json.Marshal(req)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/locks", serverAddress),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result lockstore.LockResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		return
	}

	if result.Acquired {
		fmt.Printf("Lock acquired: fencing_token=%d, lease_id=%d\n", result.FencingToken, result.LeaseID)
	} else {
		fmt.Println("Lock not acquired (held by another owner)")
	}
}

func handleRelease(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: release <lock_name> <owner_id> <fencing_token>")
		return
	}

	token, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		fmt.Printf("Invalid fencing token: %s\n", args[2])
		return
	}

	req := request.LockReleaseRequest{
		LockName:     args[0],
		OwnerID:      args[1],
		FencingToken: token,
	}

	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/locks", serverAddress), bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("Lock released")
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Failed to release lock: %s\n", string(respBody))
	}
}

func handleStatus(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: status <lock_name>")
		return
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/locks?name=%s", serverAddress, args[0]))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result lockstore.LockResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("Error decoding response: %v\n", err)
		return
	}

	if result.Acquired {
		fmt.Printf("Lock: %s, Owner: %s, Token: %d, Lease: %d, Waiters: %d\n",
			result.LockName, result.OwnerID, result.FencingToken, result.LeaseID, result.QueuePosition)
	} else {
		fmt.Printf("Lock: %s (not held)\n", result.LockName)
	}
}

func handleWait(args []string) {
	if len(args) < 3 {
		fmt.Println("Usage: wait <lock_name> <owner_id> <lease_id> [wait_timeout_seconds]")
		return
	}

	leaseID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		fmt.Printf("Invalid lease_id: %s\n", args[2])
		return
	}

	var waitTimeout int64 = 30
	if len(args) >= 4 {
		waitTimeout, err = strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			fmt.Printf("Invalid wait timeout: %s\n", args[3])
			return
		}
	}

	requestID := uuid.New().String()

	req := request.LockAcquireRequest{
		LockName:           args[0],
		OwnerID:            args[1],
		LeaseID:            leaseID,
		WaitTimeoutSeconds: waitTimeout,
		RequestID:          requestID,
	}

	fmt.Printf("Waiting for lock %s (request_id: %s, timeout: %ds)...\n", args[0], requestID[:8], waitTimeout)

	deadline := time.Now().Add(time.Duration(waitTimeout) * time.Second)

	for time.Now().Before(deadline) {
		remainingTimeout := time.Until(deadline).Seconds()
		if remainingTimeout <= 0 {
			break
		}
		req.WaitTimeoutSeconds = int64(remainingTimeout) + 1

		client := &http.Client{
			Timeout: time.Duration(remainingTimeout+5) * time.Second,
		}

		body, _ := json.Marshal(req)
		resp, err := client.Post(
			fmt.Sprintf("http://%s/locks", serverAddress),
			"application/json",
			bytes.NewReader(body),
		)
		if err != nil {
			fmt.Printf("Connection error, retrying with same request_id: %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		if resp.StatusCode == http.StatusRequestTimeout {
			resp.Body.Close()
			fmt.Println("Wait timeout - lock not acquired")
			return
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			resp.Body.Close()
			fmt.Println("Leader changed, retrying...")
			time.Sleep(time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Printf("Error: %s\n", string(respBody))
			return
		}

		var result lockstore.LockResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			fmt.Printf("Error decoding response: %v\n", err)
			return
		}
		resp.Body.Close()

		if result.Acquired {
			fmt.Printf("Lock acquired: fencing_token=%d, lease_id=%d\n", result.FencingToken, result.LeaseID)
			return
		}

		fmt.Printf("In queue (position: %d), connection lost, retrying...\n", result.QueuePosition)
		time.Sleep(time.Second)
	}

	fmt.Println("Wait timeout - lock not acquired")
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println()
	fmt.Println("Lease operations:")
	fmt.Println("  lease-grant <ttl_seconds>           - Create a new lease")
	fmt.Println("  lease-revoke <lease_id>             - Revoke a lease (releases all locks)")
	fmt.Println("  lease-keepalive <lease_id>          - Extend lease TTL")
	fmt.Println("  lease-status <lease_id>             - Check lease status")
	fmt.Println()
	fmt.Println("Lock operations:")
	fmt.Println("  acquire <lock_name> <owner_id> <lease_id>  - Acquire a lock")
	fmt.Println("  release <lock_name> <owner_id> <fencing_token> - Release a lock")
	fmt.Println("  wait <lock_name> <owner_id> <lease_id> [timeout] - Wait to acquire lock")
	fmt.Println("  status <lock_name>                  - Check lock status")
	fmt.Println()
	fmt.Println("Other:")
	fmt.Println("  help                                - Show this help")
	fmt.Println("  exit                                - Exit the client")
}
