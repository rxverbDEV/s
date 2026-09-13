package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
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
		
		// Context bitişini kaçırmamak için select ile bekle
		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			// Bekleme bitti, çık
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
			break // Sistem zaten daha uzun süre duraklatılmış
		}
		if globalPauseUntil.CompareAndSwap(current, pauseUntil) {
			break
		}
	}
}

// ------------------------------------

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
	// Sıfır allocation payload (string concat Go'da küçüktür)
	payloadBytes := []byte(`{"username":"` + name + `"}`)
	token := os.Getenv("DISCORD_TOKEN")

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		
		// 🛡️ GÜVENLİK & KONTROL: Rate Limit varsa tüm worker'lar burada bekler
		waitIfRateLimited(ctx)

		req, err := http.NewRequestWithContext(ctx, "POST", "https://discord.com/api/v9/users/@me/pomelo-attempt", bytes.NewReader(payloadBytes))
		if err != nil {
			continue
		}

		// 🛡️ GÜVENLİK: Dürüst ve standart header'lar. Spoofing YOK.
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
			time.Sleep(1 * time.Second) // Ağ hatası durumunda minik backoff
			continue
		}

		metricReqs.Add(1)
		metricLatSum.Add(uint64(latency))
		metricLatCount.Add(1)

		// Body'i oku, Connection Reuse için kesin kapatılması lazım
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		respStr := string(bodyBytes)

		// 🛡️ GÜVENLİK: Discord Header tabanlı Adaptive Rate Control
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
			
			// 🛡️ GÜVENLİK: Retry-After değerine mutlak itaat.
			retryAfterStr := resp.Header.Get("Retry-After")
			var pauseDuration time.Duration
			if retryAfterStr != "" {
				if retryAfter, err := strconv.ParseFloat(retryAfterStr, 64); err == nil {
					pauseDuration = time.Duration(retryAfter * float64(time.Second))
				}
			}
			
			if pauseDuration <= 0 {
				pauseDuration = 5 * time.Second // Fallback değer
			}
			
			// Güvenlik marjı (+100ms) ekleyelim ki request sınırda tekrar 429 yemesin.
			updateGlobalPause(pauseDuration + (100 * time.Millisecond))
			
			// Hata attempt'ini tüketmemek için attempt'i 1 geri al (Çünkü bu bizim suçumuz değil, limit)
			attempt--
			continue

		} else if resp.StatusCode >= 500 {
			metric5xxs.Add(1)
			// Exponential Backoff + Jitter
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			jitter := time.Duration(rand.Intn(500)) * time.Millisecond
			time.Sleep(backoff + jitter)
			continue
		} else {
			// 401, 403, 404 gibi çözülemeyen client hataları
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
	
	// Bağlantının Connection Pool'a geri dönebilmesi için body'i kesinlikle discard edip kapatmalıyız.
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}
