package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// --- METRICS & RATE LIMIT STATE ---
var (
	metricReqs     atomic.Uint64
	metric429s     atomic.Uint64
	metric5xxs     atomic.Uint64
	metricTimeouts atomic.Uint64
	metricHits     atomic.Uint64
	metricLatSum   atomic.Uint64 // ms cinsinden toplam gecikme
	metricLatCount atomic.Uint64

	// Tüm worker'ları durduracak global timestamp (UnixNano)
	globalPauseUntil atomic.Int64
)

var (
	length     = getEnvInt("LENGTH", 3)
	charsetOpt = getEnvInt("CHARSET", 4)
	threads    = getEnvInt("THREADS", 1) // Güvenli başlangıç değeri. Çok artırmak 429'a neden olur.
	workerID   = getEnvInt("WORKER_ID", 0)
	totalNodes = getEnvInt("TOTAL_WORKERS", 1)
	webhookURL = os.Getenv("WEBHOOK_URL")
)

// HTTP Client Optimizasyonu: Yüksek verim, düşük connection pressure.
var client = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100, // Discord'a aynı anda açılacak maksimum bağlantı sınırı
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false, // Bant genişliği tasarrufu için açık kalsın
		ForceAttemptHTTP2:     true,
	},
}

const (
	StatusAvailable = "🟢 Alınabilir"
	StatusUsed      = "🔴 Kullanılıyor"
	StatusUnknown   = "⚪ Bilinmiyor"
)

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

var webhookQueue = make(chan WebhookPayload, 1000)
var blacklistMap = make(map[string]struct{})
var currentLoop atomic.Int64

func main() {
	// 🌐 Render PORT entegrasyonu ve sağlık kontrolü sunucusu
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Discord Scanner 24/7 Aktif! 🚀"))
		})
		http.ListenAndServe(":"+port, nil)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n⚠️ Kapatma sinyali alındı. Güvenlice durduruluyor...")
		cancel()
	}()

	fmt.Println("⚡ === DISCORD GÜVENLİ (LOW-RISK) TARAYICI BAŞLATILIYOR === ⚡")
	loadBlacklist()

	if webhookURL != "" {
		for i := 0; i < 2; i++ {
			go webhookWorker(ctx, webhookQueue)
		}
	}

	var charset []byte
	switch charsetOpt {
	case 1:
		charset = []byte("abcdefghijklmnopqrstuvwxyz")
	case 2:
		charset = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	case 3:
		charset = []byte("abcdefghijklmnopqrstuvwxyz_")
	default:
		charset = []byte("abcdefghijklmnopqrstuvwxyz0123456789_")
	}

	totalCombinations := getDiscordCombinations(length, charset)
	chunkSize := int64(math.Ceil(float64(totalCombinations) / float64(totalNodes)))
	startIdx := int64(workerID) * chunkSize
	endIdx := startIdx + chunkSize
	if endIdx > totalCombinations {
		endIdx = totalCombinations
	}

	fmt.Printf("Platform: Discord\n")
	fmt.Printf("Hız: %d Thread (Güvenli Mod) | Kapsam: %d Karakter\n", threads, length)
	fmt.Printf("🎯 Görev Dağılımı: Hedef %d isim\n", endIdx-startIdx)
	if webhookURL != "" {
		fmt.Println("🔔 Discord Webhook: AKTİF")
	}
	fmt.Printf("🛡️ Blacklist: %d kayıt yüklendi\n", len(blacklistMap))
	fmt.Println("===========================================")

	// Metrik izleyicisini başlat
	go metricsTicker(ctx)

	jobs := make(chan string, threads*2)
	results := make(chan CheckResult, 100)

	go resultHandler(ctx, results)

	for i := 0; i < threads; i++ {
		go worker(ctx, jobs, results)
	}

	currentLoop.Store(1)

outerLoop:
	for {
		loopVal := currentLoop.Load()
		fmt.Printf("\n🔄 --- [TARAMA DÖNGÜSÜ: %d. TUR BAŞLIYOR] ---\n", loopVal)

		for idx := startIdx; idx < endIdx; idx++ {
			select {
			case <-ctx.Done():
				break outerLoop
			default:
				name := generateDiscordName(idx, length, charset)
				jobs <- name
			}
		}

		fmt.Printf("✅ --- [%d. TUR LİSTESİ BİTTİ, 5 SANİYE BEKLENİYOR] ---\n", loopVal)
		time.Sleep(5 * time.Second)
		currentLoop.Add(1)
	}

	close(jobs)
	time.Sleep(2 * time.Second)
}

// metricsTicker canlı performansı konsola yazar
func metricsTicker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastReqs, lastLatSum, lastLatCount uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqs := metricReqs.Load()
			latSum := metricLatSum.Load()
			latCount := metricLatCount.Load()

			deltaReqs := reqs - lastReqs
			deltaLatSum := latSum - lastLatSum
			deltaLatCount := latCount - lastLatCount

			reqRate := deltaReqs / 5

			var avgLat uint64
			if deltaLatCount > 0 {
				avgLat = deltaLatSum / deltaLatCount
			}

			fmt.Printf("📊 Speed: %d req/s | 📡 Avg Latency: %d ms | 🟢 429s: %d | 🔴 5xxs: %d | 🎯 Hits: %d\n",
				reqRate, avgLat, metric429s.Load(), metric5xxs.Load(), metricHits.Load())

			lastReqs = reqs
			lastLatSum = latSum
			lastLatCount = latCount
		}
	}
}

// waitIfRateLimited global duraklatma süresine kadar bekler
func waitIfRateLimited(ctx context.Context) {
	for {
		now := time.Now().UnixNano()
		pauseUntil := globalPauseUntil.Load()
		if pauseUntil <= now {
			return
		}
		
		sleepDur := time.Duration(pauseUntil - now)
		
		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			return
		}
	}
}

// updateGlobalPause yeni bir bekleme süresi set eder
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

func generateDiscordName(index int64, length int, charset []byte) string {
	b := make([]byte, length)
	cLen := int64(len(charset))

	for i := length - 1; i >= 0; i-- {
		b[i] = charset[index%cLen]
		index /= cLen
	}
	return string(b)
}

func getDiscordCombinations(length int, charset []byte) int64 {
	return intPow(int64(len(charset)), int64(length))
}

func checkDiscordName(ctx context.Context, name string, results chan<- CheckResult) {
	payloadBytes := []byte(`{"username":"` + name + `"}`)
	token := os.Getenv("DISCORD_TOKEN")

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		
		waitIfRateLimited(ctx)

		req, err := http.NewRequestWithContext(ctx, "POST", "https://discord.com/api/v9/users/@me/pomelo-attempt", bytes.NewReader(payloadBytes))
		if err != nil {
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "SafeScanner/1.0 (Rate-Limit Compliant Bot)")
		req.Header.Set("Accept", "*/*")
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start).Milliseconds()

		if err != nil {
			metricTimeouts.Add(1)
			time.Sleep(1 * time.Second)
			continue
		}

		metricReqs.Add(1)
		metricLatSum.Add(uint64(latency))
		metricLatCount.Add(1)

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		respStr := string(bodyBytes)

		remainingStr := resp.Header.Get("X-RateLimit-Remaining")
		if remainingStr == "0" {
			resetAfterStr := resp.Header.Get("X-RateLimit-Reset-After")
			if resetAfter, err := strconv.ParseFloat(resetAfterStr, 64); err == nil {
				updateGlobalPause(time.Duration(resetAfter * float64(time.Second)))
			}
		}

		if resp.StatusCode == 200 {
			if strings.Contains(respStr, `"taken": false`) || strings.Contains(respStr, `"taken":false`) {
				results <- CheckResult{Name: name, Status: StatusAvailable}
			} else {
				results <- CheckResult{Name: name, Status: StatusUsed}
			}
			return

		} else if resp.StatusCode == 429 {
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
			
			updateGlobalPause(pauseDuration + (100 * time.Millisecond))
			
			attempt--
			continue

		} else if resp.StatusCode >= 500 {
			metric5xxs.Add(1)
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			jitter := time.Duration(rand.Intn(500)) * time.Millisecond
			time.Sleep(backoff + jitter)
			continue
		} else {
			results <- CheckResult{Name: name, Status: StatusUnknown}
			return
		}
	}
	
	results <- CheckResult{Name: name, Status: StatusUnknown}
}

func BuildDiscordWebhookPayload(hit CheckResult) WebhookPayload {
	score := evaluateName(hit.Name)
	charCount := len(hit.Name)
	timeStr := time.Now().UTC().Format("2006-01-02 15:04 UTC")
	typeDesc := fmt.Sprintf("%dL", charCount)

	fields := []WebhookField{
		{Name: "👤 İsim", Value: fmt.Sprintf("`%s`", hit.Name), Inline: true},
		{Name: "⭐ Değer", Value: fmt.Sprintf("`%s`", score), Inline: true},
		{Name: "📊 Durum", Value: fmt.Sprintf("`%s`", hit.Status), Inline: true},
		{Name: "🧩 Karakter", Value: fmt.Sprintf("`%d karakter`", charCount), Inline: true},
		{Name: "🏷️ Tip", Value: fmt.Sprintf("`%s`", typeDesc), Inline: true},
		{Name: "🕐 Bulunma", Value: fmt.Sprintf("`%s`", timeStr), Inline: false},
	}

	return WebhookPayload{
		Embeds: []WebhookEmbed{
			{
				Title:  "🎯 DISCORD USERNAME HIT",
				Color:  5763719,
				Fields: fields,
				Footer: WebhookFooter{Text: "Safe Scanner • Discord"},
			},
		},
	}
}

func webhookWorker(ctx context.Context, queue <-chan WebhookPayload) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-queue:
			sendToDiscord(payload)
		}
	}
}

func sendToDiscord(payload WebhookPayload) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", webhookURL, bytes.NewReader(jsonBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
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

func evaluateName(name string) string {
	score := 5.0
	hasLetter, hasNumber, hasUnderscore := false, false, false

	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			hasLetter = true
		} else if c >= '0' && c <= '9' {
			hasNumber = true
		} else if c == '_' {
			hasUnderscore = true
		}
	}

	if hasLetter && !hasNumber && !hasUnderscore {
		score += 3.0
	} else if hasNumber && !hasLetter && !hasUnderscore {
		score += 1.0
	} else if hasLetter && hasNumber && !hasUnderscore {
		score += 2.0
	}

	if len(name) == 3 {
		score += 2.0
	} else if len(name) == 4 {
		score += 1.0
	}

	if score > 10.0 {
		score = 10.0
	}
	return fmt.Sprintf("%.1f/10", score)
}

func worker(ctx context.Context, jobs <-chan string, results chan<- CheckResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case name, ok := <-jobs:
			if !ok {
				return
			}
			checkDiscordName(ctx, name, results)
		}
	}
}

func resultHandler(ctx context.Context, results <-chan CheckResult) {
	seenHits := make(map[string]struct{})

	f, err := os.OpenFile("hits_discord.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Dosya açılamadı:", err)
		return
	}
	defer f.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			lowerName := strings.ToLower(res.Name)

			if _, exists := blacklistMap[lowerName]; exists {
				continue
			}
			if _, seen := seenHits[lowerName]; seen {
				continue
			}
			
			if res.Status == StatusAvailable {
				seenHits[lowerName] = struct{}{}
				metricHits.Add(1)
				fmt.Printf("🔥 [%s] -> %s\n", res.Status, res.Name)
				f.WriteString(fmt.Sprintf("%s | %s\n", res.Name, res.Status))
				f.Sync()

				if webhookURL != "" {
					payload := BuildDiscordWebhookPayload(res)
					select {
					case webhookQueue <- payload:
					default:
					}
				}
			}
		}
	}
}

func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}

func intPow(base, exp int64) int64 {
	result := int64(1)
	for i := int64(0); i < exp; i++ {
		result *= base
	}
	return result
}
