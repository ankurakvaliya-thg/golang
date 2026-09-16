/*
Package main implements a concurrent URL health monitoring service.

Features:
  - Concurrent health checks across many URLs
  - Configurable worker pool with rate limiting
  - Retry logic with exponential backoff
  - Persistent results with JSON storage
  - Real-time metrics and statistics
  - Graceful shutdown with context cancellation
  - HTTP API to query status
*/
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================
// CONFIGURATION
// ============================================================

// Config holds all runtime configuration for the monitor.
type Config struct {
	Workers         int           `json:"workers"`
	Interval        time.Duration `json:"interval"`
	Timeout         time.Duration `json:"timeout"`
	MaxRetries      int           `json:"max_retries"`
	RetryBaseDelay  time.Duration `json:"retry_base_delay"`
	RateLimitPerSec int           `json:"rate_limit_per_sec"`
	OutputFile      string        `json:"output_file"`
	ListenAddr      string        `json:"listen_addr"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Workers:         5,
		Interval:        30 * time.Second,
		Timeout:         10 * time.Second,
		MaxRetries:      3,
		RetryBaseDelay:  500 * time.Millisecond,
		RateLimitPerSec: 20,
		OutputFile:      "health_results.json",
		ListenAddr:      ":8080",
	}
}

// ============================================================
// DOMAIN MODELS
// ============================================================

// CheckResult represents the outcome of a single health check.
type CheckResult struct {
	URL          string        `json:"url"`
	StatusCode   int           `json:"status_code"`
	Duration     time.Duration `json:"duration_ns"`
	DurationMs   float64       `json:"duration_ms"`
	Success      bool          `json:"success"`
	Error        string        `json:"error,omitempty"`
	Attempts     int           `json:"attempts"`
	Timestamp    time.Time     `json:"timestamp"`
	ResponseSize int64         `json:"response_size"`
}

// URLStats aggregates statistics for a single URL over time.
type URLStats struct {
	URL              string        `json:"url"`
	TotalChecks      int64         `json:"total_checks"`
	SuccessCount     int64         `json:"success_count"`
	FailureCount     int64         `json:"failure_count"`
	AvgDuration      time.Duration `json:"avg_duration_ns"`
	MinDuration      time.Duration `json:"min_duration_ns"`
	MaxDuration      time.Duration `json:"max_duration_ns"`
	LastCheck        time.Time     `json:"last_check"`
	LastStatus       int           `json:"last_status"`
	ConsecutiveFails int           `json:"consecutive_fails"`
}

// SuccessRate returns the success percentage (0-100).
func (s *URLStats) SuccessRate() float64 {
	if s.TotalChecks == 0 {
		return 0
	}
	return float64(s.SuccessCount) / float64(s.TotalChecks) * 100
}

// ============================================================
// RATE LIMITER (token bucket)
// ============================================================

// RateLimiter implements a simple token-bucket rate limiter.
type RateLimiter struct {
	tokens   chan struct{}
	ticker   *time.Ticker
	stopOnce sync.Once
	stop     chan struct{}
}

// NewRateLimiter creates a limiter allowing `rate` operations per second.
func NewRateLimiter(rate int) *RateLimiter {
	if rate <= 0 {
		rate = 1
	}
	rl := &RateLimiter{
		tokens: make(chan struct{}, rate),
		ticker: time.NewTicker(time.Second / time.Duration(rate)),
		stop:   make(chan struct{}),
	}
	// Pre-fill bucket.
	for i := 0; i < rate; i++ {
		rl.tokens <- struct{}{}
	}
	go rl.refill()
	return rl
}

func (rl *RateLimiter) refill() {
	for {
		select {
		case <-rl.ticker.C:
			select {
			case rl.tokens <- struct{}{}:
			default:
				// Bucket full, drop token.
			}
		case <-rl.stop:
			return
		}
	}
}

// Wait blocks until a token is available or the context is cancelled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop shuts down the rate limiter.
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() {
		rl.ticker.Stop()
		close(rl.stop)
	})
}

// ============================================================
// HEALTH CHECKER
// ============================================================

// Checker performs HTTP health checks with retry logic.
type Checker struct {
	client      *http.Client
	timeout     time.Duration
	maxRetries  int
	baseDelay   time.Duration
	rateLimiter *RateLimiter
}

// NewChecker creates a new Checker.
func NewChecker(cfg Config) *Checker {
	return &Checker{
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout:     cfg.Timeout,
		maxRetries:  cfg.MaxRetries,
		baseDelay:   cfg.RetryBaseDelay,
		rateLimiter: NewRateLimiter(cfg.RateLimitPerSec),
	}
}

// Check performs a health check for a URL with retries.
func (c *Checker) Check(ctx context.Context, url string) CheckResult {
	result := CheckResult{
		URL:       url,
		Timestamp: time.Now(),
	}

	var lastErr error
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		result.Attempts = attempt

		if err := c.rateLimiter.Wait(ctx); err != nil {
			result.Error = err.Error()
			return result
		}

		start := time.Now()
		statusCode, size, err := c.doRequest(ctx, url)
		result.Duration = time.Since(start)
		result.DurationMs = float64(result.Duration.Microseconds()) / 1000.0

		if err == nil && statusCode >= 200 && statusCode < 400 {
			result.StatusCode = statusCode
			result.ResponseSize = size
			result.Success = true
			return result
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("HTTP %d", statusCode)
			result.StatusCode = statusCode
		}

		// Retry with exponential backoff.
		if attempt < c.maxRetries {
			delay := time.Duration(float64(c.baseDelay) * math.Pow(2, float64(attempt-1)))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				result.Error = ctx.Err().Error()
				return result
			}
		}
	}

	result.Error = lastErr.Error()
	return result
}

func (c *Checker) doRequest(ctx context.Context, url string) (int, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("User-Agent", "GoHealthMonitor/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	// Read and discard body to allow connection reuse.
	buf := make([]byte, 4096)
	var total int64
	for {
		n, err := resp.Body.Read(buf)
		total += int64(n)
		if err != nil {
			break
		}
		if total > 10*1024*1024 {
			break // Cap at 10MB
		}
	}
	return resp.StatusCode, total, nil
}

// Close releases the checker's rate limiter.
func (c *Checker) Close() {
	c.rateLimiter.Stop()
}

// ============================================================
// MONITOR
// ============================================================

// Monitor orchestrates the health checks and statistics.
type Monitor struct {
	config   Config
	checker  *Checker
	urls     []string
	stats    map[string]*URLStats
	statsMu  sync.RWMutex
	results  chan CheckResult
	updates  chan CheckResult
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	checksRun int64
}

// NewMonitor creates a Monitor for the given URLs.
func NewMonitor(cfg Config, urls []string) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Monitor{
		config:  cfg,
		checker: NewChecker(cfg),
		urls:    urls,
		stats:   make(map[string]*URLStats),
		results: make(chan CheckResult, 100),
		updates: make(chan CheckResult, 100),
		ctx:     ctx,
		cancel:  cancel,
	}
	for _, u := range urls {
		m.stats[u] = &URLStats{URL: u}
	}
	return m
}

// Start begins the worker pool and result aggregator.
func (m *Monitor) Start() {
	// Start aggregator.
	m.wg.Add(1)
	go m.aggregate()

	// Start workers.
	for i := 0; i < m.config.Workers; i++ {
		m.wg.Add(1)
		go m.worker(i)
	}

	// Start periodic scheduler.
	m.wg.Add(1)
	go m.schedule()

	log.Printf("Monitor started: %d workers, interval=%s, urls=%d",
		m.config.Workers, m.config.Interval, len(m.urls))
}

// schedule enqueues checks on every interval tick.
func (m *Monitor) schedule() {
	defer m.wg.Done()

	// Run immediately on start.
	m.runAllChecks()

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.runAllChecks()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Monitor) runAllChecks() {
	for _, url := range m.urls {
		select {
		case m.results <- CheckResult{URL: url}: // just a signal
		case <-m.ctx.Done():
			return
		}
	}
}

// worker pulls URLs from the channel and performs checks.
func (m *Monitor) worker(id int) {
	defer m.wg.Done()
	log.Printf("Worker %d ready", id)

	for {
		select {
		case <-m.ctx.Done():
			log.Printf("Worker %d stopping", id)
			return
		case sig, ok := <-m.results:
			if !ok {
				return
			}
			result := m.checker.Check(m.ctx, sig.URL)
			atomic.AddInt64(&m.checksRun, 1)

			select {
			case m.updates <- result:
			case <-m.ctx.Done():
				return
			}
		}
	}
}

// aggregate consumes results and updates statistics.
func (m *Monitor) aggregate() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case r, ok := <-m.updates:
			if !ok {
				return
			}
			m.updateStats(r)
			m.logResult(r)
		}
	}
}

func (m *Monitor) updateStats(r CheckResult) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()

	s, exists := m.stats[r.URL]
	if !exists {
		s = &URLStats{URL: r.URL}
		m.stats[r.URL] = s
	}

	s.TotalChecks++
	s.LastCheck = r.Timestamp
	s.LastStatus = r.StatusCode

	if r.Success {
		s.SuccessCount++
		s.ConsecutiveFails = 0
	} else {
		s.FailureCount++
		s.ConsecutiveFails++
	}

	// Update timing (only for successful responses).
	if r.Success {
		if s.MinDuration == 0 || r.Duration < s.MinDuration {
			s.MinDuration = r.Duration
		}
		if r.Duration > s.MaxDuration {
			s.MaxDuration = r.Duration
		}
		// Rolling average.
		prevTotal := s.AvgDuration * time.Duration(s.SuccessCount-1)
		s.AvgDuration = (prevTotal + r.Duration) / time.Duration(s.SuccessCount)
	}
}

func (m *Monitor) logResult(r CheckResult) {
	marker := "✓"
	if !r.Success {
		marker = "✗"
	}
	log.Printf("[%s] %s %d %.1fms attempts=%d%s",
		marker, r.URL, r.StatusCode, r.DurationMs, r.Attempts,
		errorSuffix(r.Error))
}

func errorSuffix(errStr string) string {
	if errStr == "" {
		return ""
	}
	return " err=" + errStr
}

// Snapshot returns a copy of the current statistics.
func (m *Monitor) Snapshot() []URLStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	out := make([]URLStats, 0, len(m.stats))
	for _, s := range m.stats {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].URL < out[j].URL
	})
	return out
}

// Save writes statistics to a JSON file.
func (m *Monitor) Save(path string) error {
	snapshot := m.Snapshot()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal stats: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the monitor.
func (m *Monitor) Stop() {
	log.Println("Stopping monitor...")
	m.cancel()
	close(m.results)

	// Give workers a moment to drain.
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All workers stopped")
	case <-time.After(5 * time.Second):
		log.Println("Shutdown timeout; forcing exit")
	}

	m.checker.Close()
	close(m.updates)
}

// ============================================================
// HTTP API
// ============================================================

// Server exposes monitor statistics over HTTP.
type Server struct {
	monitor *Monitor
	srv     *http.Server
}

// NewServer creates an HTTP server for the monitor.
func NewServer(m *Monitor, addr string) *Server {
	s := &Server{monitor: m}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/stats/", s.handleURLStats)

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	return s
}

// Start begins serving HTTP requests in a background goroutine.
func (s *Server) Start() {
	go func() {
		log.Printf("HTTP server listening on %s", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}

// Stop shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.monitor.Snapshot())
}

func (s *Server) handleURLStats(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Path[len("/stats/"):]
	for _, st := range s.monitor.Snapshot() {
		if st.URL == target {
			writeJSON(w, http.StatusOK, st)
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "url not found"})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

// ============================================================
// MAIN
// ============================================================

func main() {
	// Parse flags.
	workers := flag.Int("workers", 5, "number of concurrent workers")
	interval := flag.Duration("interval", 30*time.Second, "check interval")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	retries := flag.Int("retries", 3, "max retry attempts")
	output := flag.String("output", "health_results.json", "results output file")
	listen := flag.String("listen", ":8080", "HTTP API listen address")
	flag.Parse()

	cfg := DefaultConfig()
	cfg.Workers = *workers
	cfg.Interval = *interval
	cfg.Timeout = *timeout
	cfg.MaxRetries = *retries
	cfg.OutputFile = *output
	cfg.ListenAddr = *listen

	// URLs to monitor.
	urls := flag.Args()
	if len(urls) == 0 {
		urls = []string{
			"https://www.google.com",
			"https://www.github.com",
			"https://www.cloudflare.com",
			"https://go.dev",
			"https://httpbin.org/status/200",
			"https://httpbin.org/status/500",
			"https://example.invalid",
		}
	}

	// Create monitor and server.
	monitor := NewMonitor(cfg, urls)
	server := NewServer(monitor, cfg.ListenAddr)

	monitor.Start()
	server.Start()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutdown signal received")
	monitor.Stop()

	// Save final results.
	if err := monitor.Save(cfg.OutputFile); err != nil {
		log.Printf("Failed to save results: %v", err)
	} else {
		log.Printf("Results saved to %s", cfg.OutputFile)
	}

	// Shutdown HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Stop(ctx)

	// Print final summary.
	printSummary(monitor.Snapshot())
}

func printSummary(stats []URLStats) {
	fmt.Println("\n" + "============================================================")
	fmt.Println("FINAL SUMMARY")
	fmt.Println("============================================================")
	for _, s := range stats {
		fmt.Printf("\n  %s\n", s.URL)
		fmt.Printf("    Checks:    %d (✓ %d, ✗ %d)\n",
			s.TotalChecks, s.SuccessCount, s.FailureCount)
		fmt.Printf("    Success:   %.1f%%\n", s.SuccessRate())
		fmt.Printf("    Avg time:  %s\n", s.AvgDuration)
		fmt.Printf("    Min/Max:   %s / %s\n", s.MinDuration, s.MaxDuration)
		fmt.Printf("    Last:      %d (%s)\n", s.LastStatus, s.LastCheck.Format(time.RFC3339))
	}
	fmt.Println("\n" + "============================================================")
}
