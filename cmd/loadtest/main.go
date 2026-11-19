package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type checkoutReq struct {
	CustomerID       string   `json:"customer_id"`
	Amount           float64  `json:"amount"`
	Currency         string   `json:"currency"`
	Items            []string `json:"items"`
	SimulateFailStep string   `json:"simulate_fail_step,omitempty"`
}

type checkoutResp struct {
	WorkflowID string `json:"workflow_id"`
	Status     string `json:"status"`
}

type domainStateSummary struct {
	TotalChargedCount    int     `json:"total_charged_transactions"`
	TotalRefundedCount   int     `json:"total_refunded_transactions"`
	NetCapturedRevenue   float64 `json:"net_captured_revenue"`
	TotalLabelsGenerated int     `json:"total_labels_generated"`
	TotalLabelsVoided    int     `json:"total_labels_voided"`
	TotalEmailsSent      int     `json:"total_emails_sent"`
	InventoryCatalog     []struct {
		SKU            string `json:"sku"`
		AvailableStock int    `json:"available_stock"`
		ReservedStock  int    `json:"reserved_stock"`
	} `json:"inventory_catalog"`
}

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "Base URL of the Edge API Gateway")
	concurrency := flag.Int("concurrency", 25, "Number of concurrent load-generating goroutines")
	duration := flag.Duration("duration", 30*time.Second, "Total duration of the stress test")
	numClients := flag.Int("clients", 15, "Number of distinct X-API-Key clients to simulate")
	rollbackRate := flag.Float64("rollback-rate", 0.20, "Probability (0.0-1.0) of triggering a Saga Rollback (Real stock/carrier failure)")
	dlqRate := flag.Float64("dlq-rate", 0.10, "Probability (0.0-1.0) of triggering Notification failure (Retries + DLQ)")
	thinkTime := flag.Duration("think-time", 40*time.Millisecond, "Delay between requests per worker (set 0s for max saturation)")
	flag.Parse()

	endpoint := *baseURL + "/api/v1/workflows/checkout"
	catalogSKUs := []string{"keyboard_mech", "mouse_wireless", "monitor_4k", "gpu_rtx", "psu_850w", "headset"}

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	// Pre-seed ample warehouse stock for catalog items before stress test
	for _, sku := range catalogSKUs {
		restockBody, _ := json.Marshal(map[string]interface{}{"sku": sku, "quantity": 50000})
		req, err := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/domain/inventory/restock", bytes.NewReader(restockBody))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			if resp, doErr := httpClient.Do(req); doErr == nil {
				_ = resp.Body.Close()
			}
		}
	}

	fmt.Println("================================================================================")
	fmt.Println(" DISTRIBUTED WORKFLOW ORCHESTRATOR - STATEFUL DOMAIN STRESS & LOAD TESTER")
	fmt.Println("================================================================================")
	fmt.Printf(" Target Endpoint : %s\n", endpoint)
	fmt.Printf(" Concurrency     : %d goroutines\n", *concurrency)
	fmt.Printf(" Duration        : %v\n", *duration)
	fmt.Printf(" Simulated Keys  : %d API keys\n", *numClients)
	fmt.Printf(" Rollback Ratio  : %.0f%% (Real Prohibited Parcel & Out-of-Stock SKU rollbacks)\n", *rollbackRate*100)
	fmt.Printf(" DLQ Ratio       : %.0f%% (Invalid recipient SMTP retry + DLQ stress)\n", *dlqRate*100)
	fmt.Printf(" Think Time      : %v\n", *thinkTime)
	fmt.Println("================================================================================")

	var (
		totalReqs        atomic.Int64
		status202        atomic.Int64
		status429        atomic.Int64
		statusOther      atomic.Int64
		sagaCompleted    atomic.Int64
		sagaRolledBack   atomic.Int64
		sagaTriggeredDLQ atomic.Int64
		networkErrors    atomic.Int64

		latMu     sync.Mutex
		latencies = make([]time.Duration, 0, 10000)
	)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	startTime := time.Now()

	// Progress reporter goroutine
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				tot := totalReqs.Load()
				rps := float64(tot) / elapsed
				fmt.Printf("[+%5.1fs] Reqs: %-6d | RPS: %6.1f/s | 202 Accepted: %-5d | 429 Blocked: %-5d | Completed: %-5d | RolledBack: %-4d\n",
					elapsed, tot, rps, status202.Load(), status429.Load(), sagaCompleted.Load(), sagaRolledBack.Load())
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*997))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				clientID := rng.Intn(*numClients) + 1
				apiKey := fmt.Sprintf("stress-client-%02d", clientID)

				sku1 := catalogSKUs[rng.Intn(len(catalogSKUs))]
				sku2 := catalogSKUs[rng.Intn(len(catalogSKUs))]
				items := []string{sku1, sku2}
				customerID := fmt.Sprintf("cust_%d_%d", workerID, rng.Intn(10000))

				isDLQ := false
				roll := rng.Float64()
				if roll < *rollbackRate {
					if rng.Float64() < 0.70 {
						// Real Fulfillment rejection: prohibited hazard item triggers ReleaseItems + RefundCard
						items = append(items, "restricted_hazard_item")
					} else {
						// Real Inventory shortage: 0-stock SKU triggers RefundCard
						items = append(items, "out_of_stock_item")
					}
				} else if roll < (*rollbackRate + *dlqRate) {
					// Real Notification failure: unsubscribed recipient triggers 3x backoff + DLQ
					customerID = "unsubscribed_customer"
					isDLQ = true
				}

				payload := checkoutReq{
					CustomerID: customerID,
					Amount:     float64(50 + rng.Intn(450)),
					Currency:   "BRL",
					Items:      items,
				}
				bodyBytes, _ := json.Marshal(payload)

				reqStart := time.Now()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-API-Key", apiKey)

				resp, err := httpClient.Do(req)
				reqDur := time.Since(reqStart)

				if err != nil {
					if ctx.Err() != nil {
						return
					}
					networkErrors.Add(1)
					continue
				}

				totalReqs.Add(1)
				latMu.Lock()
				latencies = append(latencies, reqDur)
				latMu.Unlock()

				respBytes, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusAccepted:
					status202.Add(1)
					var cr checkoutResp
					if json.Unmarshal(respBytes, &cr) == nil {
						if cr.Status == "COMPLETED" {
							sagaCompleted.Add(1)
							if isDLQ {
								sagaTriggeredDLQ.Add(1)
							}
						} else if cr.Status == "ROLLED_BACK" {
							sagaRolledBack.Add(1)
						}
					}
				case http.StatusTooManyRequests:
					status429.Add(1)
				default:
					statusOther.Add(1)
				}

				if *thinkTime > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(*thinkTime):
					}
				}
			}
		}(w)
	}

	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Compute Latency Percentiles
	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	count := len(latencies)
	var minLat, maxLat, avgLat, p50, p90, p95, p99 time.Duration
	if count > 0 {
		minLat = latencies[0]
		maxLat = latencies[count-1]
		var sum time.Duration
		for _, d := range latencies {
			sum += d
		}
		avgLat = sum / time.Duration(count)
		p50 = latencies[int(float64(count)*0.50)]
		p90 = latencies[int(float64(count)*0.90)]
		p95 = latencies[int(float64(count)*0.95)]
		p99 = latencies[int(float64(count)*0.99)]
	}
	latMu.Unlock()

	tot := totalReqs.Load()
	rps := float64(tot) / totalElapsed.Seconds()

	fmt.Println("\n================================================================================")
	fmt.Println(" FINAL STRESS TEST REPORT")
	fmt.Println("================================================================================")
	fmt.Printf(" Total Elapsed Time          : %v\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf(" Total HTTP Requests         : %d\n", tot)
	fmt.Printf(" Average Throughput          : %.2f req/sec\n", rps)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" HTTP Gateway Statuses:")
	fmt.Printf("   202 Accepted (Allowed)    : %d (%.1f%%)\n", status202.Load(), pct(status202.Load(), tot))
	fmt.Printf("   429 Too Many Requests     : %d (%.1f%%)\n", status429.Load(), pct(status429.Load(), tot))
	fmt.Printf("   Other HTTP Statuses       : %d\n", statusOther.Load())
	fmt.Printf("   Network/Timeout Errors    : %d\n", networkErrors.Load())
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" Saga Orchestrator Outcomes (among 202 Accepted):")
	fmt.Printf("   COMPLETED Successfully    : %d\n", sagaCompleted.Load())
	fmt.Printf("   ROLLED_BACK (Compensated) : %d\n", sagaRolledBack.Load())
	fmt.Printf("   Notification -> DLQ       : %d\n", sagaTriggeredDLQ.Load())
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" End-to-End Latency Distribution:")
	fmt.Printf("   Min : %-10v | Avg : %-10v | Max : %-10v\n", minLat.Round(time.Microsecond), avgLat.Round(time.Microsecond), maxLat.Round(time.Microsecond))
	fmt.Printf("   P50 : %-10v | P90 : %-10v | P95 : %-10v | P99 : %-10v\n",
		p50.Round(time.Microsecond), p90.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))

	// Fetch live Domain Store snapshot to show real persisted financial & inventory state
	if domResp, err := httpClient.Get(*baseURL + "/api/v1/domain/state"); err == nil {
		defer domResp.Body.Close()
		var ds domainStateSummary
		if json.NewDecoder(domResp.Body).Decode(&ds) == nil {
			fmt.Println("--------------------------------------------------------------------------------")
			fmt.Println(" Persisted Domain Store State (GET /api/v1/domain/state):")
			fmt.Printf("   Payment Ledger Charged    : %d transactions (Net Captured: %.2f BRL)\n", ds.TotalChargedCount, ds.NetCapturedRevenue)
			fmt.Printf("   Payment Ledger Refunded   : %d compensated transactions\n", ds.TotalRefundedCount)
			fmt.Printf("   Shipping Labels Generated : %d (Voided: %d)\n", ds.TotalLabelsGenerated, ds.TotalLabelsVoided)
			fmt.Printf("   Confirmation Emails Sent  : %d persisted in outbox\n", ds.TotalEmailsSent)
		}
	}
	fmt.Println("================================================================================")
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(part) / float64(total)) * 100.0
}
