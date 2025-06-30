package report

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Result represents the result of a single operation
type Result struct {
	Err   error
	Start time.Time
	End   time.Time
}

// Reporter collects and processes benchmark results
type Reporter struct {
	results chan Result
	sample  bool
	precise bool

	totalCount   int
	errorCount   int
	totalLatency time.Duration

	secondResults  map[int][]Result // Results grouped by second for sampling
	requestResults []Result
	startTime      time.Time
}

// NewReporter creates a new Reporter instance
func NewReporter(sample, precise bool) *Reporter {
	return &Reporter{
		results:        make(chan Result, 4096),
		sample:         sample,
		precise:        precise,
		secondResults:  make(map[int][]Result),
		requestResults: make([]Result, 0, 4096),
		startTime:      time.Now(),
	}
}

// Results returns the results channel
func (r *Reporter) Results() chan Result {
	return r.results
}

// Run processes incoming results
func (r *Reporter) Run() <-chan string {
	donec := make(chan string, 1)
	go func() {
		defer close(donec)

		for res := range r.results {
			r.totalCount++
			if res.Err != nil {
				r.errorCount++
			} else {
				lat := res.End.Sub(res.Start)
				r.totalLatency += lat

				// Sample if enabled
				if r.sample {
					secSinceStart := int(res.Start.Sub(r.startTime).Seconds())
					r.secondResults[secSinceStart] = append(r.secondResults[secSinceStart], res)
				}
				r.requestResults = append(r.requestResults, res)
			}
		}

		donec <- r.report()
	}()
	return donec
}

// report generates the final report
func (r *Reporter) report() string {
	// Calculate overall statistics
	successCount := r.totalCount - r.errorCount
	total := float64(r.totalCount)
	successPercent := float64(successCount) / total * 100.0
	totalDuration := time.Since(r.startTime)
	s := totalDuration.Seconds()

	// Calculate IOPS
	avgIOPS := float64(successCount) / s

	// Create the report
	report := []string{
		fmt.Sprintf("Total requests: %d", r.totalCount),
		fmt.Sprintf("Completed requests: %d (%.2f%%)", successCount, successPercent),
		fmt.Sprintf("Failed requests: %d (%.2f%%)", r.errorCount, float64(r.errorCount)/total*100.0),
		fmt.Sprintf("Total time: %.2f seconds", s),
		fmt.Sprintf("Average IOPS: %.2f", avgIOPS),
	}

	// Add latency statistics to the report
	var latencies []time.Duration
	if len(r.requestResults) > 0 {
		for _, res := range r.requestResults {
			if res.Err == nil {
				latencies = append(latencies, res.End.Sub(res.Start))
			}
		}
		// Add latency report to the final report
		latReport := r.latencyReport(latencies, "Overall")
		report = append(report, "") // Empty line for better readability
		report = append(report, latReport...)
	}

	// Add second-by-second statistics if sampling was enabled
	if r.sample && len(r.secondResults) > 0 {
		report = append(report, "\nSecond-by-second statistics:")

		// Sort seconds
		seconds := make([]int, 0, len(r.secondResults))
		for sec := range r.secondResults {
			seconds = append(seconds, sec)
		}
		sort.Ints(seconds)

		for _, sec := range seconds {
			results := r.secondResults[sec]
			count := len(results)

			// Skip empty seconds
			if count == 0 {
				continue
			}

			// Calculate average latency
			var totalLat time.Duration
			var minLat, maxLat time.Duration

			// Initialize min and max latency
			if count > 0 {
				lat := results[0].End.Sub(results[0].Start)
				minLat = lat
				maxLat = lat
				totalLat = lat

				// Iterate from the second result
				for i := 1; i < count; i++ {
					rlat := results[i].End.Sub(results[i].Start)
					totalLat += rlat
					if rlat < minLat {
						minLat = rlat
					}
					if rlat > maxLat {
						maxLat = rlat
					}
				}
			}

			avgLat := totalLat / time.Duration(count)

			report = append(report, fmt.Sprintf("  Second %d: %d ops (latency min: %s, avg: %s max: %s)",
				sec, count, minLat, avgLat, maxLat))
		}
	}

	return strings.Join(report, "\n")
}

// latencyReport generates a report section for latency statistics
func (r *Reporter) latencyReport(latencies []time.Duration, prefix string) []string {
	if len(latencies) == 0 {
		return []string{fmt.Sprintf("%s latency samples: 0", prefix)}
	}

	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate statistics
	n := len(latencies)
	total := int64(0)
	for _, lat := range latencies {
		total += int64(lat)
	}
	avg := time.Duration(total / int64(n))
	minLatency := latencies[0]
	maxLatency := latencies[n-1]

	// Calculate percentiles
	p50 := percentile(latencies, 0.50)
	p75 := percentile(latencies, 0.75)
	p90 := percentile(latencies, 0.90)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	p999 := percentile(latencies, 0.999)

	// Format latency with precision based on settings
	format := "%s"
	if r.precise {
		format = "%.9f"
	}

	// Build the report
	report := []string{
		fmt.Sprintf("%s latency samples: %d", prefix, n),
		fmt.Sprintf("%s latency (min/avg/max): %s/%s/%s",
			prefix, formatDuration(minLatency, format), formatDuration(avg, format), formatDuration(maxLatency, format)),
		fmt.Sprintf("%s latency percentiles:",
			prefix),
		fmt.Sprintf("  p50: %s", formatDuration(p50, format)),
		fmt.Sprintf("  p75: %s", formatDuration(p75, format)),
		fmt.Sprintf("  p90: %s", formatDuration(p90, format)),
		fmt.Sprintf("  p95: %s", formatDuration(p95, format)),
		fmt.Sprintf("  p99: %s", formatDuration(p99, format)),
		fmt.Sprintf("  p99.9: %s", formatDuration(p999, format)),
	}

	return report
}

// percentile calculates the specified percentile from the sorted latencies
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	if p == 0.0 {
		return latencies[0]
	}
	if p == 1.0 {
		return latencies[len(latencies)-1]

	}

	// Calculate the index
	idx := float64(len(latencies)-1) * p
	fidx := math.Floor(idx)
	cidx := math.Ceil(idx)

	// If the index is an integer, return the exact value
	if fidx == cidx {
		return latencies[int(fidx)]
	}

	// Interpolate between the two values
	lower := float64(latencies[int(fidx)])
	upper := float64(latencies[int(cidx)])
	weight := idx - fidx

	return time.Duration(lower + weight*(upper-lower))
}

// formatDuration formats a duration based on precision settings
func formatDuration(d time.Duration, format string) string {
	if format == "%s" {
		return d.String()
	}
	sec := float64(d) / float64(time.Second)
	return fmt.Sprintf(format+"s", sec)
}
