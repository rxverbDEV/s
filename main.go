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
	"net/url"
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
	TargetLength = 3 // Aranan kelimenin uzunluğu
	SafeThreads  = 3 // Tek IP için en güvenli thread sayısı
	WebhookURL   = "https://discord.com/api/webhooks/1548315868944142386/68B2biKu_Wz2_KNVwnwJwgtAbixCNnBcghiUDKFq8m5HkqsH0Ecipnsbx3i3BqzyOnLI"
)

// --- METRICS & STATE ---
var (
	metricReqs      atomic.Uint64
	metric429s      atomic.Uint64
	metric5xxs      atomic.Uint64
	metricTimeouts  atomic.Uint64
	metricHits      atomic.Uint64
	metricProcessed atomic.Uint64
	metricLatSum    atomic.Uint64
	metricLatCount  atomic.Uint64
	metricWorkers   atomic.Int32

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
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableKeepAlives:     false,
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
	Title  string         `json:"title"`
	Color  int            `json:"color"`
	Fields []WebhookField `json:"fields"`
	Footer WebhookFooter  `json:"footer"`
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
		fmt.Println("\n⚠️ Kapatma sinyali alındı. Veriler kaydedilerek güvenlice durduruluyor...")
		cancel()
	}()

	fmt.Print("\033[?25l") // Cursor'ı gizle
	defer fmt.Print("\033[?25h")

	fmt.Println("⚡ === CHESS.COM GÜVENLİ (STABİL) TARAYICI BAŞLATILIYOR === ⚡")
	loadBlacklist()

	webhookQueue := make(chan WebhookPayload, 1000)
	var wgWebhooks sync.WaitGroup
	for i := 0; i < 2; i++ {
		wgWebhooks.Add(1)
		go webhookWorker(ctx, webhookQueue, &wgWebhooks)
	}

	fmt.Println("⚙️ Geçerli isim kombinasyonları oluşturuluyor...")
	validNames := generateValidNames(TargetLength)

	fmt.Println("🔀 İsimler karıştırılıyor (Homojen dağılım)...")
	shuffleList(validNames)

	totalCombinations := len(validNames)
	if totalCombinations == 0 {
		fmt.Println("❌ Üretilen geçerli kombinasyon yok. Çıkılıyor.")
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

	fmt.Printf("Platform: Chess.com\n")
	fmt.Printf("Hız: %d Thread (Safe Mode) | Kapsam: %d Karakter\n", SafeThreads, TargetLength)
	fmt.Printf("🎯 Toplam Geçerli İsim: %d | Bu Worker'ın Görevi: %d isim\n", totalCombinations, totalMyNames)
	fmt.Printf("🔔 Discord Webhook: AKTİF\n")
	fmt.Println("===========================================\n")

	jobs := make(chan string, SafeThreads*2)
	results := make(chan CheckResult, 100)

	var wgResultHandler sync.WaitGroup
	wgResultHandler.Add(1)
	go resultHandler(ctx, results, webhookQueue, &wgResultHandler)

	var wgWorkers sync.WaitGroup
	for i := 0; i < SafeThreads; i++ {
		wgWorkers.Add(1)
		ua := userAgents[i%len(userAgents)]
		go worker(ctx, jobs, results, ua, &wgWorkers)
	}

	go metricsDashboard(ctx, totalMyNames)

	startTime := time.Now()

	// Ana isim dağıtım döngüsü
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

	elapsed := time.Since(startTime)
	fmt.Printf("\n✅ Tarama tamamlandı! Geçen Süre: %v\n", elapsed)
}

func generateValidNames(length int) []string {
	charset := []byte("abcdefghijklmnopqrstuvwxyz123456789_")
	results := make([]string, 0, 45000)
	buf := make([]byte, length)

	var gen func(pos int, hasLetter bool, lastChar byte)
	gen = func(pos int, hasLetter bool, lastChar byte) {
		if pos == length {
			if hasLetter {
				results = append(results, string(buf))
			}
			return
		}

		for _, c := range charset {
			if pos == 0 && c == '_' {
				continue
			}
			if pos == length-1 && c == '_' {
				continue
			}
			if c == '_' && lastChar == '_' {
				continue
			}

			buf[pos] = c
			isLetter := (c >= 'a' && c <= 'z')
			gen(pos+1, hasLetter || isLetter, c)
		}
	}

	gen(0, false, 0)
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

func metricsDashboard(ctx context.Context, total int) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastReqs, lastLatSum, lastLatCount uint64
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqs := metricReqs.Load()
			latSum := metricLatSum.Load()
			latCount := metricLatCount.Load()
			processed := metricProcessed.Load()
			workers := metricWorkers.Load()

			deltaReqs := reqs - lastReqs
			deltaLatSum := latSum - lastLatSum
			deltaLatCount := latCount - lastLatCount

			var avgLat uint64
			if deltaLatCount > 0 {
				avgLat = deltaLatSum / deltaLatCount
			}

			percentage := float64(processed) / float64(total) * 100
			if math.IsNaN(percentage) {
				percentage = 0
			}

			elapsed := time.Since(startTime).Round(time.Second)

			fmt.Printf("\r\033[K[%5.1f%%] 📊 Hız: %3d req/s | 📡 Ping: %4d ms | 🛑 429: %d | 🎯 Bulunan: %d | ⚡ Aktif: %d | ⏱️ %v",
				percentage, deltaReqs, avgLat, metric429s.Load(), metricHits.Load(), workers, elapsed)

			lastReqs = reqs
			lastLatSum = latSum
			lastLatCount = latCount
		}
	}
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
			checkChessName(ctx, name, userAgent, results)
			metricProcessed.Add(1)
		}
	}
}

func checkChessName(ctx context.Context, name, userAgent string, results chan<- CheckResult) {
	maxRetries := 3
	urlStr := "https://api.chess.com/pub/player/" + name

	for attempt := 0; attempt < maxRetries; attempt++ {
		waitIfRateLimited(ctx)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		if err != nil {
			return
		}

		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Connection", "keep-alive")

		start := time.Now()
		resp, err := client.Do(req)
		latency := uint64(time.Since(start).Milliseconds())

		if err != nil {
			metricTimeouts.Add(1)
			time.Sleep(1 * time.Second)
			continue
		}

		metricReqs.Add(1)
		metricLatSum.Add(latency)
		metricLatCount.Add(1)

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return
		} else if resp.StatusCode == http.StatusNotFound {
			results <- CheckResult{Name: name, Status: "🟢 Alınabilir"}
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
				pauseDuration = 5 * time.Second
			}
			updateGlobalPause(pauseDuration + (250 * time.Millisecond))
			attempt--
			continue
		} else if resp.StatusCode >= http.StatusInternalServerError {
			metric5xxs.Add(1)
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		} else {
			return
		}
	}
}

func resultHandler(ctx context.Context, results <-chan CheckResult, webhookQueue chan<- WebhookPayload, wg *sync.WaitGroup) {
	defer wg.Done()
	seenHits := make(map[string]struct{})

	f, err := os.OpenFile("hits_chess.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("\n❌ Dosya açılamadı: %v\n", err)
		return
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	defer writer.Flush()

	flushTicker := time.NewTicker(5 * time.Second)
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
			
			fmt.Printf("\r\033[K🔥 [BULUNDU] -> %s\n", res.Name)

			writer.WriteString(res.Name + "\n")

			payload := BuildChessWebhookPayload(res)
			select {
			case webhookQueue <- payload:
			default:
			}
		}
	}
}

func BuildChessWebhookPayload(hit CheckResult) WebhookPayload {
	timeStr := time.Now().UTC().Format("2006-01-02 15:04 UTC")
	score, typeDesc := evaluateNameDetailed(hit.Name)
	encodedName := url.PathEscape(hit.Name)
	profileURL := fmt.Sprintf("https://www.chess.com/member/%s", encodedName)

	fields := []WebhookField{
		{Name: "👤 İsim", Value: fmt.Sprintf("`%s`", hit.Name), Inline: true},
		{Name: "⭐ Puan", Value: fmt.Sprintf("`%s`", score), Inline: true},
		{Name: "🏷️ Kategori", Value: fmt.Sprintf("`%s`", typeDesc), Inline: true},
		{Name: "🔗 Profil", Value: fmt.Sprintf("[Kayıt Ol](%s)", profileURL), Inline: false},
		{Name: "🕐 Zaman", Value: fmt.Sprintf("`%s`", timeStr), Inline: false},
	}

	return WebhookPayload{
		Embeds: []WebhookEmbed{
			{
				Title:  "🎯 CHESS.COM KULLANICI ADI BULUNDU",
				Color:  5763719,
				Fields: fields,
				Footer: WebhookFooter{Text: "Chess.com Pro Scanner"},
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
		return
	}
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			blacklistMap[strings.ToLower(string(line))] = struct{}{}
		}
	}
}

func evaluateNameDetailed(name string) (string, string) {
	hasLetter, hasNumber, hasSpecial := false, false, false
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			hasLetter = true
		} else if c >= '0' && c <= '9' {
			hasNumber = true
		} else if c == '_' || c == '-' {
			hasSpecial = true
		}
	}

	score := 5.0
	typeDesc := "Karışık"

	if hasLetter && !hasNumber && !hasSpecial {
		score += 4.0
		typeDesc = "Saf Harf"
	} else if hasNumber && !hasLetter && !hasSpecial {
		score += 5.0
		typeDesc = "Saf Sayı"
	} else if hasLetter && hasNumber && !hasSpecial {
		score += 2.0
		typeDesc = "Harf + Sayı"
	} else if hasSpecial {
		typeDesc = "Sembol İçeriyor"
	}

	if len(name) == 3 {
		score += 1.0
	}

	if score > 10.0 {
		score = 10.0
	}
	return fmt.Sprintf("%.1f/10", score), typeDesc
}
