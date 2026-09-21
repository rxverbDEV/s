package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// --- YAPILANDIRMA ---
const (
	SafeThreads = 10 // Worker pool boyutu
	WebhookURL  = "https://discord.com/api/webhooks/1548315868944142386/68B2biKu_Wz2_KNVwnwJwgtAbixCNnBcghiUDKFq8m5HkqsH0Ecipnsbx3i3BqzyOnLI"
	OutputFile  = "available_instagram.txt"
)

// --- METRICS & STATE ---
var (
	metricReqs        atomic.Uint64
	metric429s        atomic.Uint64
	metric5xxs        atomic.Uint64
	metricTimeouts    atomic.Uint64
	metricHits        atomic.Uint64
	metricUnavailable atomic.Uint64
	metricErrors      atomic.Uint64
	metricProcessed   atomic.Uint64
	metricLatSum      atomic.Uint64
	metricLatCount    atomic.Uint64
	metricWorkers     atomic.Int32
	lastLatency       atomic.Uint64

	globalPauseUntil atomic.Int64
	blacklistMap     = make(map[string]struct{})
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
}

// Global, optimize edilmiş HTTP Client
var client = &http.Client{
	Timeout: 8 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableKeepAlives:     false,
	},
	// Instagram login sayfasına (302) gereksiz redirect olmamak için
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

type CheckResult struct {
	Name   string
	Status string
}

type WebhookPayload struct {
	Embeds []WebhookEmbed `json:"embeds"`
}

type WebhookEmbed struct {
	Title       string         `json:"title"`
	Color       int            `json:"color"`
	Description string         `json:"description,omitempty"`
	Fields      []WebhookField `json:"fields"`
	Footer      WebhookFooter  `json:"footer"`
}

type WebhookField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type WebhookFooter struct {
	Text string `json:"text"`
}

func main() {
	workerID := flag.Int("worker", 0, "Bu sunucunun/programin ID'si (Örn: 0)")
	totalNodes := flag.Int("total", 1, "Toplam çalışacak sunucu/program sayısı (Örn: 1)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Print("\n\033[?25h") // Cursor'ı geri getir
		fmt.Println("\n\n⚠️ Kapatma sinyali alındı. Veriler kaydedilerek güvenlice durduruluyor...")
		cancel()
	}()

	fmt.Print("\033[?25l") // Cursor'ı gizle
	defer fmt.Print("\033[?25h")

	fmt.Println("⚡ === INSTAGRAM GÜVENLİ TARAYICI BAŞLATILIYOR === ⚡")
	loadBlacklist()

	webhookQueue := make(chan WebhookPayload, 1000)
	var wgWebhooks sync.WaitGroup
	for i := 0; i < 2; i++ {
		wgWebhooks.Add(1)
		go webhookWorker(ctx, webhookQueue, &wgWebhooks)
	}

	fmt.Println("⚙️ Geçerli isim kombinasyonları oluşturuluyor (b + 4 harf)...")
	validNames := generateInstagramNames()

	fmt.Println("🔀 İsimler karıştırılıyor (Homojen dağılım)...")
	shuffleList(validNames)

	totalCombinations := len(validNames)
	if totalCombinations == 0 {
		fmt.Println("❌ Üretilen kombinasyon yok. Çıkılıyor.")
		return
	}

	chunkSize := (totalCombinations + *totalNodes - 1) / *totalNodes
	startIdx := *workerID * chunkSize
	endIdx := startIdx + chunkSize
	if endIdx > totalCombinations {
		endIdx = totalCombinations
	}
	if startIdx >= totalCombinations {
		fmt.Println("❌ Hatalı Worker ID. Kapatılıyor.")
		return
	}

	myNames := validNames[startIdx:endIdx]
	totalMyNames := len(myNames)

	fmt.Println("==================================================")
	fmt.Printf("Platform : Instagram\n")
	fmt.Printf("Hız      : %d Thread\n", SafeThreads)
	fmt.Printf("Kural    : b[a-z]{4} (Örn: baabc)\n")
	fmt.Printf("Toplam   : %d Kombinasyon\n", totalCombinations)
	fmt.Printf("Görev    : %d İsim (Bu Worker)\n", totalMyNames)
	fmt.Println("==================================================\n")

	time.Sleep(1 * time.Second) // Dashboard'un temiz başlaması için ufak bekleme

	jobs := make(chan string, SafeThreads*3)
	results := make(chan CheckResult, 500)
	hitLogQueue := make(chan string, 100)

	var wgResultHandler sync.WaitGroup
	wgResultHandler.Add(1)
	go resultHandler(ctx, results, webhookQueue, hitLogQueue, &wgResultHandler)

	var wgWorkers sync.WaitGroup
	for i := 0; i < SafeThreads; i++ {
		wgWorkers.Add(1)
		ua := userAgents[i%len(userAgents)]
		go worker(ctx, jobs, results, ua, &wgWorkers)
	}

	// Dashboard Goroutine
	var wgDashboard sync.WaitGroup
	wgDashboard.Add(1)
	go metricsDashboard(ctx, totalMyNames, hitLogQueue, &wgDashboard)

	// Job Dağıtımı
outerLoop:
	for _, name := range myNames {
		select {
		case <-ctx.Done():
			break outerLoop
		case jobs <- name:
		}
	}

	close(jobs)
	wgWorkers.Wait()

	close(results)
	wgResultHandler.Wait()

	close(webhookQueue)
	wgWebhooks.Wait()

	// Biraz bekle ve dashboard'u sonlandır
	time.Sleep(1 * time.Second)
	wgDashboard.Wait()

	fmt.Print("\n\033[?25h") // Cursor'ı geri getir
	fmt.Printf("\n✅ Tarama güvenle tamamlandı!\n")
}

// generateInstagramNames, sadece b + 4 harf kombinasyonlarını sıfır GC presi ile üretir
func generateInstagramNames() []string {
	// Toplam olasılık: 26^4 = 456,976
	total := 456976
	results := make([]string, 0, total)
	buf := make([]byte, 5)
	buf[0] = 'b'

	charset := "abcdefghijklmnopqrstuvwxyz"

	for i := 0; i < 26; i++ {
		buf[1] = charset[i]
		for j := 0; j < 26; j++ {
			buf[2] = charset[j]
			for k := 0; k < 26; k++ {
				buf[3] = charset[k]
				for l := 0; l < 26; l++ {
					buf[4] = charset[l]
					results = append(results, string(buf))
				}
			}
		}
	}
	return results
}

func shuffleList(slice []string) {
	seed := uint32(time.Now().UnixNano())
	for i := len(slice) - 1; i > 0; i-- {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		j := int(seed % uint32(i+1))
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func metricsDashboard(ctx context.Context, total int, hitLogQueue <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastReqs uint64
	startTime := time.Now()
	linesToClear := 0

	for {
		select {
		case <-ctx.Done():
			// Son kez yazdır ve çık
			printDashboard(total, &lastReqs, startTime, &linesToClear, hitLogQueue, true)
			return
		case <-ticker.C:
			printDashboard(total, &lastReqs, startTime, &linesToClear, hitLogQueue, false)
		}
	}
}

func printDashboard(total int, lastReqs *uint64, startTime time.Time, linesToClear *int, hitLogQueue <-chan string, isStopped bool) {
	// Bekleyen önemli hit loglarını yazdır
	hasLogs := false
	for {
		select {
		case logMsg := <-hitLogQueue:
			if *linesToClear > 0 {
				fmt.Printf("\033[%dA\033[J", *linesToClear) // Önceki dashboard'u sil
				*linesToClear = 0
			}
			fmt.Println(logMsg)
			hasLogs = true
		default:
			goto DashboardRender
		}
	}

DashboardRender:
	if *linesToClear > 0 && !hasLogs {
		fmt.Printf("\033[%dA", *linesToClear) // Sadece imleci yukarı al
	}

	reqs := metricReqs.Load()
	latSum := metricLatSum.Load()
	latCount := metricLatCount.Load()
	processed := int(metricProcessed.Load())
	workers := metricWorkers.Load()
	hits := metricHits.Load()
	unavail := metricUnavailable.Load()
	errs := metricErrors.Load()
	c429 := metric429s.Load()
	c5xx := metric5xxs.Load()
	timeouts := metricTimeouts.Load()
	lastLat := lastLatency.Load()

	deltaReqs := reqs - *lastReqs
	*lastReqs = reqs

	var avgLat uint64
	if latCount > 0 {
		avgLat = latSum / latCount
	}

	percentage := float64(processed) / float64(total) * 100
	if math.IsNaN(percentage) {
		percentage = 0
	}

	remaining := total - processed
	if remaining < 0 {
		remaining = 0
	}

	elapsed := time.Since(startTime)
	etaStr := "--:--:--"
	if deltaReqs > 0 {
		etaSeconds := int(remaining) / int(deltaReqs)
		etaDur := time.Duration(etaSeconds) * time.Second
		etaStr = formatDuration(etaDur)
	}

	status := "RUNNING"
	if isStopped {
		status = "STOPPED"
		deltaReqs = 0
		workers = 0
	}

	dashboard := fmt.Sprintf(`==================================================
INSTAGRAM USERNAME SCANNER

Status       : %s
Target       : Instagram
Pattern      : b[a-z]{4}
Total        : %d
Checked      : %d
Remaining    : %d
Progress     : %.2f%%
Speed        : %d req/s
Last Latency : %d ms
Avg Latency  : %d ms
Available    : %d
Unavailable  : %d
Errors       : %d
HTTP 429     : %d
HTTP 5xx     : %d
Timeout      : %d
Workers      : %d
Elapsed      : %s
ETA          : %s
==================================================`,
		status, total, processed, remaining, percentage, deltaReqs,
		lastLat, avgLat, hits, unavail, errs, c429, c5xx, timeouts,
		workers, formatDuration(elapsed), etaStr,
	)

	fmt.Println(dashboard)
	*linesToClear = 20
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func waitIfRateLimited(ctx context.Context) {
	for {
		now := time.Now().UnixNano()
		pauseUntil := globalPauseUntil.Load()
		if pauseUntil <= now {
			return
		}
		sleepDur := time.Duration(pauseUntil - now)
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepDur):
		}
	}
}

func updateGlobalPause(d time.Duration) {
	pauseUntil := time.Now().Add(d).UnixNano()
	for {
		current := globalPauseUntil.Load()
		if pauseUntil <= current {
			break
		}
		if globalPauseUntil.CompareAndSwap(current, pauseUntil) {
			break
		}
	}
}

func worker(ctx context.Context, jobs <-chan string, results chan<- CheckResult, userAgent string, wg *sync.WaitGroup) {
	defer wg.Done()
	metricWorkers.Add(1)
	defer metricWorkers.Add(-1)

	for {
		select {
		case <-ctx.Done():
			return
		case name, ok := <-jobs:
			if !ok {
				return
			}
			checkInstagramName(ctx, name, userAgent, results)
			metricProcessed.Add(1)
		}
	}
}

func checkInstagramName(ctx context.Context, name, userAgent string, results chan<- CheckResult) {
	maxRetries := 3
	urlStr := "https://www.instagram.com/" + name + "/"

	for attempt := 0; attempt < maxRetries; attempt++ {
		waitIfRateLimited(ctx)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		if err != nil {
			metricErrors.Add(1)
			return
		}

		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		req.Header.Set("Connection", "keep-alive")

		start := time.Now()
		resp, err := client.Do(req)
		latency := uint64(time.Since(start).Milliseconds())

		if err != nil {
			metricTimeouts.Add(1)
			metricErrors.Add(1)
			time.Sleep(1 * time.Second)
			continue
		}

		metricReqs.Add(1)
		metricLatSum.Add(latency)
		metricLatCount.Add(1)
		lastLatency.Store(latency)

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// Instagram HTTP Status Kontrolü
		if resp.StatusCode == http.StatusOK {
			// Mevcut veya kullanılamaz
			metricUnavailable.Add(1)
			return
		} else if resp.StatusCode == http.StatusNotFound {
			// 404 genellikle boştaki hesaptır
			results <- CheckResult{Name: name, Status: "🟢 Alınabilir"}
			return
		} else if resp.StatusCode == http.StatusFound || resp.StatusCode == http.StatusMovedPermanently { // 302 / 301
			// Yönlendirme genellikle login sayfasına olur, rate limit habercisi olabilir
			metricUnavailable.Add(1)
			return
		} else if resp.StatusCode == http.StatusTooManyRequests {
			metric429s.Add(1)
			retryAfterStr := resp.Header.Get("Retry-After")
			var pauseDuration time.Duration
			if retryAfterStr != "" {
				if retryAfter, err := strconv.ParseFloat(retryAfterStr, 64); err == nil {
					pauseDuration = time.Duration(retryAfter * float64(time.Second))
				}
			}
			if pauseDuration <= 0 {
				pauseDuration = 30 * time.Second // Instagram için varsayılan 429 cezası genelde uzundur
			}
			updateGlobalPause(pauseDuration + (500 * time.Millisecond))
			attempt--
			continue
		} else if resp.StatusCode >= http.StatusInternalServerError {
			metric5xxs.Add(1)
			metricErrors.Add(1)
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		} else {
			metricErrors.Add(1)
			return
		}
	}
}

func resultHandler(ctx context.Context, results <-chan CheckResult, webhookQueue chan<- WebhookPayload, hitLogQueue chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	seenHits := make(map[string]struct{})

	f, err := os.OpenFile(OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("\n❌ Dosya açılamadı: %v\n", err)
		return
	}
	defer f.Close()

	writer := bufio.NewWriterSize(f, 4096)
	defer writer.Flush()

	flushTicker := time.NewTicker(3 * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushTicker.C:
			writer.Flush()
			f.Sync()
		case res, ok := <-results:
			if !ok {
				return
			}
			lowerName := strings.ToLower(res.Name)
			if _, exists := blacklistMap[lowerName]; exists {
				continue
			}
			if _, seen := seenHits[lowerName]; seen {
				continue
			}

			seenHits[lowerName] = struct{}{}
			metricHits.Add(1)

			// Terminali bozmadan dashboard üstüne yazdırmak için kanala gönderiyoruz
			select {
			case hitLogQueue <- fmt.Sprintf("🔥 [AVAILABLE] %s", res.Name):
			default:
			}

			writer.WriteString(res.Name + "\n")

			payload := BuildInstagramWebhookPayload(res)
			select {
			case webhookQueue <- payload:
			default:
			}
		}
	}
}

func BuildInstagramWebhookPayload(hit CheckResult) WebhookPayload {
	timeStr := time.Now().UTC().Format("2006-01-02 15:04 UTC")
	profileURL := fmt.Sprintf("https://www.instagram.com/%s", hit.Name)

	fields := []WebhookField{
		{Name: "👤 Kullanıcı Adı", Value: fmt.Sprintf("`%s`", hit.Name), Inline: true},
		{Name: "🔗 Profil", Value: fmt.Sprintf("[Kayıt Ol](%s)", profileURL), Inline: true},
		{Name: "🕐 Zaman", Value: fmt.Sprintf("`%s`", timeStr), Inline: false},
	}

	return WebhookPayload{
		Embeds: []WebhookEmbed{
			{
				Title:  "🎯 INSTAGRAM USERNAME BULUNDU",
				Color:  13506161, // Instagram temasına uygun bir renk (Magenta/Pembe tonları)
				Fields: fields,
				Footer: WebhookFooter{Text: "Instagram Pro Scanner"},
			},
		},
	}
}

func webhookWorker(ctx context.Context, queue <-chan WebhookPayload, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-queue:
			if !ok {
				return
			}
			sendToDiscord(payload)
		}
	}
}

func sendToDiscord(payload WebhookPayload) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest("POST", WebhookURL, bytes.NewReader(jsonBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

func loadBlacklist() {
	data, err := os.ReadFile("blacklist.txt")
	if err != nil {
		return // Dosya yoksa sorun değil
	}
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			blacklistMap[strings.ToLower(string(line))] = struct{}{}
		}
	}
}
