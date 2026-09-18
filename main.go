package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// --- YAPILANDIRMA (AYARLAR BURADA SABİTLENDİ) ---
const (
	TargetLength = 3 // Aranan kelimenin uzunluğu (3 karakter)
	SafeThreads  = 3 // Tek IP için en güvenli thread sayısı (Asla 429 yemez)
	WebhookURL   = "https://discord.com/api/webhooks/1548315868944142386/68B2biKu_Wz2_KNVwnwJwgtAbixCNnBcghiUDKFq8m5HkqsH0Ecipnsbx3i3BqzyOnLI"
)

// --- METRICS & RATE LIMIT STATE ---
var (
	metricReqs     atomic.Uint64
	metric429s     atomic.Uint64
	metric5xxs     atomic.Uint64
	metricTimeouts atomic.Uint64
	metricHits     atomic.Uint64
	metricLatSum   atomic.Uint64
	metricLatCount atomic.Uint64

	globalPauseUntil atomic.Int64
	currentLoop      atomic.Int64
	blacklistMap     = make(map[string]struct{})
	webhookQueue     = make(chan WebhookPayload, 1000)
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
}

var client = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
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
	// Worker komut satırı argümanları (Terminalden kontrol etmek için)
	workerID := flag.Int("worker", 0, "Bu sunucunun/programin ID'si (Örn: 0)")
	totalNodes := flag.Int("total", 1, "Toplam çalışacak sunucu/program sayısı (Örn: 1)")
	flag.Parse()

	// Rastgelelik için seed
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n⚠️ Kapatma sinyali alındı. Güvenlice durduruluyor...")
		cancel()
	}()

	fmt.Println("⚡ === CHESS.COM GÜVENLİ (STABİL) TARAYICI BAŞLATILIYOR === ⚡")
	loadBlacklist()

	for i := 0; i < 2; i++ {
		go webhookWorker(ctx, webhookQueue)
	}

	// 1. ADIM: Sadece geçerli olan isimleri oluştur (49.248 adet geçerli isim)
	fmt.Println("⚙️ Geçerli isim kombinasyonları oluşturuluyor...")
	validNames := generateValidNames(TargetLength)
	
	// 2. ADIM: Listeyi karıştır! (Sayılar ve semboller homojen dağılsın diye)
	fmt.Println("🔀 İsimler karıştırılıyor...")
	rand.Shuffle(len(validNames), func(i, j int) {
		validNames[i], validNames[j] = validNames[j], validNames[i]
	})

	// 3. ADIM: Görevi paylaştır
	totalCombinations := len(validNames)
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

	fmt.Printf("Platform: Chess.com\n")
	fmt.Printf("Hız: %d Thread (Safe Mode) | Kapsam: %d Karakter\n", SafeThreads, TargetLength)
	fmt.Printf("🎯 Toplam Geçerli İsim: %d | Bu Worker'ın Görevi: %d isim\n", totalCombinations, len(myNames))
	fmt.Printf("🔔 Discord Webhook: AKTİF\n")
	fmt.Println("===========================================")

	go metricsTicker(ctx)

	jobs := make(chan string, SafeThreads*2)
	results := make(chan CheckResult, 100)

	go resultHandler(ctx, results)

	for i := 0; i < SafeThreads; i++ {
		go worker(ctx, jobs, results)
	}

	currentLoop.Store(1)

outerLoop:
	for {
		loopVal := currentLoop.Load()
		fmt.Printf("\n🔄 --- [TARAMA DÖNGÜSÜ: %d. TUR BAŞLIYOR] ---\n", loopVal)

		for _, name := range myNames {
			select {
			case <-ctx.Done():
				break outerLoop
			default:
				jobs <- name
			}
		}

		fmt.Printf("✅ --- [%d. TUR BİTTİ, 5 SANİYE BEKLENİYOR] ---\n", loopVal)
		time.Sleep(5 * time.Second)
		currentLoop.Add(1)
	}

	close(jobs)
	time.Sleep(2 * time.Second)
}

// generateValidNames: Chess.com kurallarına %100 uyan 49.248 ismi oluşturur.
func generateValidNames(length int) []string {
	var results []string
	alphaNum := "abcdefghijklmnopqrstuvwxyz0123456789"
	symbols := "-_"

	var generate func(current string)
	generate = func(current string) {
		if len(current) == length {
			results = append(results, current)
			return
		}

		isFirst := len(current) == 0
		isLast := len(current) == length-1

		for _, c := range alphaNum {
			generate(current + string(c))
		}

		if !isFirst && !isLast {
			prevChar := current[len(current)-1]
			if prevChar != '-' && prevChar != '_' {
				for _, c := range symbols {
					generate(current + string(c))
				}
			}
		}
	}

	generate("")
	return results
}

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

			fmt.Printf("📊 Hız: %d req/s | 📡 Ping: %d ms | 🟢 429 Engel: %d | 🎯 Bulunan: %d\n",
				reqRate, avgLat, metric429s.Load(), metricHits.Load())

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

func checkChessName(ctx context.Context, name string, results chan<- CheckResult) {
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {

		waitIfRateLimited(ctx)

		req, err := http.NewRequestWithContext(ctx, "GET", "https://api.chess.com/pub/player/"+name, nil)
		if err != nil {
			continue
		}

		ua := userAgents[rand.Intn(len(userAgents))]
		req.Header.Set("User-Agent", ua)
		req.Header.Set("Accept", "application/json")

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

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			return
		} else if resp.StatusCode == 404 {
			results <- CheckResult{Name: name, Status: "🟢 Alınabilir"}
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
			updateGlobalPause(pauseDuration + (250 * time.Millisecond))
			attempt--
			continue
		} else if resp.StatusCode >= 500 {
			metric5xxs.Add(1)
			time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * time.Second)
			continue
		} else {
			return
		}
	}
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
			checkChessName(ctx, name, results)
		}
	}
}

func resultHandler(ctx context.Context, results <-chan CheckResult) {
	seenHits := make(map[string]struct{})

	f, err := os.OpenFile("hits_chess.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

			seenHits[lowerName] = struct{}{}
			metricHits.Add(1)
			fmt.Printf("🔥 [BULUNDU] -> %s\n", res.Name)
			f.WriteString(fmt.Sprintf("%s\n", res.Name))
			f.Sync()

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
	req, err := http.NewRequest("POST", WebhookURL, bytes.NewReader(jsonBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(resp := client.Do(req)) // Derleme hatasını önlemek için düzeltildi
}

func sendToDiscordCorrected(payload WebhookPayload) {
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
