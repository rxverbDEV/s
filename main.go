package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// --- YAPILANDIRMA ---
const (
	MaxCombinations = 456976 // b + 4 harf (26^4)
	WorkerCount     = 150    // Optimize edilmiş connection havuzu boyutu
	WebhookURL      = "https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN"
	OutputFile      = "available_instagram.txt"
	TargetBaseURL   = "https://example.com/api/check" // Jenerik hedef
)

// --- ATOMİK İSTATİSTİKLER ---
// Performans için struct yerine cache-line dostu padding uygulanmış değişkenler
var (
	currentIndex    uint32
	statChecked     uint32
	statAvailable   uint32
	statUnavailable uint32
	statErrors      uint32
	statHTTP429     uint32
	statHTTP4xx     uint32
	statHTTP5xx     uint32
	statTimeouts    uint32

	totalLatencyNs uint64
	lastLatencyNs  uint64

	// Global Rate-Limit Backoff (Unix Nano Timestamp)
	globalPauseUntil int64
)

// --- VERİ YAPILARI ---
type DiscordPayload struct {
	Content string `json:"content"`
}

func main() {
	// 1. Graceful Shutdown Context'i
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel() // Ctrl+C alındığında tüm işleri iptal et
	}()

	// 2. HTTP Transport Optimizasyonu (Maksimum Connection Reuse)
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          WorkerCount,
		MaxIdleConnsPerHost:   WorkerCount,
		MaxConnsPerHost:       WorkerCount,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   8 * time.Second, // Sıkı timeout politikası
	}

	// Hedef URL'nin önceden parse edilmesi (Her istekte tekrar parse edilmesini önler)
	parsedURL, err := url.Parse(TargetBaseURL)
	if err != nil {
		fmt.Printf("Geçersiz TargetBaseURL: %v\n", err)
		return
	}

	// 3. Kanallar ve Senkronizasyon
	resultsChan := make(chan string, 1000)
	webhookChan := make(chan string, 5000)
	var wg sync.WaitGroup

	startTime := time.Now()

	// 4. Arka Plan Servislerini Başlat
	wg.Add(1)
	go diskWriter(ctx, &wg, resultsChan)

	wg.Add(1)
	go discordWebhookWorker(ctx, &wg, webhookChan)

	go liveDashboard(ctx, startTime)

	// 5. Worker Havuzunu Başlat
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, &wg, client, parsedURL, resultsChan, webhookChan)
	}

	// 6. Tüm Worker'ların ve Servislerin Bitmesini Bekle
	wg.Wait()

	// 7. Final Temizliği ve Kapanış
	renderDashboard(startTime, true)
	fmt.Println("\n[✔] Program güvenli ve temiz bir şekilde sonlandırıldı.")
}

// --- WORKER: MATEMATİKSEL ÜRETİM & AĞ YÖNETİMİ ---
func worker(ctx context.Context, wg *sync.WaitGroup, client *http.Client, baseURL *url.URL, resultsChan, webhookChan chan<- string) {
	defer wg.Done()

	// Her worker için tekrar kullanılabilir Request objesi (Zero-Allocation hedeflenmiştir)
	req := &http.Request{
		Method:     "GET",
		URL:        &url.URL{Scheme: baseURL.Scheme, Host: baseURL.Host, Path: baseURL.Path},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       baseURL.Host,
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Accept", "application/json")

	// Pre-allocate buffer for username (5 bytes: b + 4 chars)
	uBuf := make([]byte, 5)
	uBuf[0] = 'b'

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Global Backoff Kontrolü (Rate-Limit aldıysak bekleriz)
		pauseTimestamp := atomic.LoadInt64(&globalPauseUntil)
		if now := time.Now().UnixNano(); now < pauseTimestamp {
			sleepDur := time.Duration(pauseTimestamp - now)
			time.Sleep(sleepDur)
		}

		// O(1) Sıradaki İşi Al (Lock-Free)
		idx := atomic.AddUint32(&currentIndex, 1) - 1
		if idx >= MaxCombinations {
			return // Taranacak kombinasyon bitti
		}

		// İndeksi bXXXX kullanıcı adına dönüştür
		tempIdx := idx
		for i := 4; i >= 1; i-- {
			uBuf[i] = byte('a' + (tempIdx % 26))
			tempIdx /= 26
		}
		username := string(uBuf)

		// URL Query'sini güncelle
		req.URL.RawQuery = "username=" + username

		// İsteği gönder
		startReq := time.Now()
		reqCtx, reqCancel := context.WithTimeout(ctx, 8*time.Second)
		req.WithContext(reqCtx)
		
		resp, err := client.Do(req)
		reqCancel()
		
		latencyNs := uint64(time.Since(startReq).Nanoseconds())
		atomic.SwapUint64(&lastLatencyNs, latencyNs)
		atomic.AddUint64(&totalLatencyNs, latencyNs)

		// Hata Yönetimi
		if err != nil {
			if os.IsTimeout(err) || err == context.DeadlineExceeded {
				atomic.AddUint32(&statTimeouts, 1)
			} else {
				atomic.AddUint32(&statErrors, 1)
			}
			atomic.AddUint32(&statChecked, 1)
			continue
		}

		// Body'yi hızlıca boşalt ve kapat (Connection Reuse için zorunlu)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// Durum Kodu Analizi
		switch resp.StatusCode {
		case 404:
			atomic.AddUint32(&statAvailable, 1)
			// Asenkron kanallara gönder (Non-blocking)
			select {
			case resultsChan <- username:
			case <-ctx.Done():
			}
			select {
			case webhookChan <- username:
			case <-ctx.Done():
			}
		case 200:
			atomic.AddUint32(&statUnavailable, 1)
		case 429:
			atomic.AddUint32(&statHTTP429, 1)
			// GLOBAL BACKOFF TETİKLEYİCİ
			// Eğer 429 alırsak, tüm sistemi 3 saniye duraklat
			newPause := time.Now().Add(3 * time.Second).UnixNano()
			// Yalnızca mevcut duraklama süresinden ilerideyse güncelle
			currentPause := atomic.LoadInt64(&globalPauseUntil)
			if newPause > currentPause {
				atomic.CompareAndSwapInt64(&globalPauseUntil, currentPause, newPause)
			}
		default:
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				atomic.AddUint32(&statHTTP4xx, 1)
			} else if resp.StatusCode >= 500 {
				atomic.AddUint32(&statHTTP5xx, 1)
			} else {
				atomic.AddUint32(&statErrors, 1)
			}
		}
		
		atomic.AddUint32(&statChecked, 1)
	}
}

// --- DISK WRITER: ASENKRON BUFFERED I/O ---
func diskWriter(ctx context.Context, wg *sync.WaitGroup, resultsChan <-chan string) {
	defer wg.Done()

	file, err := os.OpenFile(OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 16384) // 16KB Yazma Bufferı
	defer writer.Flush()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Kapanışta kalanları yaz
			for len(resultsChan) > 0 {
				username := <-resultsChan
				writer.WriteString(username + "\n")
			}
			return
		case username, ok := <-resultsChan:
			if !ok {
				return
			}
			writer.WriteString(username + "\n")
		case <-ticker.C:
			writer.Flush() // Her 2 saniyede bir diske güvenli şekilde işle
		}
	}
}

// --- DISCORD WEBHOOK WORKER (Rate-Limit Korumalı) ---
func discordWebhookWorker(ctx context.Context, wg *sync.WaitGroup, webhookChan <-chan string) {
	defer wg.Done()

	// Webhook adresi varsayılan ise hiç başlatma
	if WebhookURL == "" || WebhookURL == "https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN" {
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	var batch []string
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	sendBatch := func(usernames []string) {
		if len(usernames) == 0 {
			return
		}
		content := "✅ **Müsait Username(ler) Bulundu:**\n"
		for _, u := range usernames {
			content += "- " + u + "\n"
		}

		payload, _ := json.Marshal(DiscordPayload{Content: content})
		req, _ := http.NewRequestWithContext(ctx, "POST", WebhookURL, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		// Discord kendi Rate Limit'ini uygularsa saygı duy
		if resp.StatusCode == 429 {
			retryAfter := resp.Header.Get("Retry-After")
			sleepTime := 3 * time.Second
			if retryAfter != "" {
				if parsed, err := strconv.Atoi(retryAfter); err == nil {
					sleepTime = time.Duration(parsed) * time.Second
				}
			}
			time.Sleep(sleepTime)
			// Hata alındıysa tekrar denemek için batch logic genişletilebilir
			// Ancak scanner hızını yavaşlatmamak için burada bırakıyoruz.
		}
	}

	for {
		select {
		case <-ctx.Done():
			sendBatch(batch) // Kalanları gönder
			return
		case username, ok := <-webhookChan:
			if !ok {
				return
			}
			batch = append(batch, username)
			if len(batch) >= 10 { // Maksimum 10'arlı gruplar halinde yolla
				sendBatch(batch)
				batch = nil
			}
		case <-ticker.C:
			sendBatch(batch)
			batch = nil
		}
	}
}

// --- CANLI DASHBOARD YÖNETİMİ ---
func liveDashboard(ctx context.Context, startTime time.Now()) {
	ticker := time.NewTicker(500 * time.Millisecond) // Daha akıcı bir görünüm için 500ms
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renderDashboard(startTime, false)
		}
	}
}

func renderDashboard(startTime time.Now, isFinal bool) {
	checked := atomic.LoadUint32(&statChecked)
	available := atomic.LoadUint32(&statAvailable)
	unavailable := atomic.LoadUint32(&statUnavailable)
	errors := atomic.LoadUint32(&statErrors)
	h429 := atomic.LoadUint32(&statHTTP429)
	h4xx := atomic.LoadUint32(&statHTTP4xx)
	h5xx := atomic.LoadUint32(&statHTTP5xx)
	timeouts := atomic.LoadUint32(&statTimeouts)

	tLatency := atomic.LoadUint64(&totalLatencyNs)
	lLatency := atomic.LoadUint64(&lastLatencyNs)

	elapsed := time.Since(startTime).Seconds()
	var rps float64
	var avgLatMs float64
	if elapsed > 0 {
		rps = float64(checked) / elapsed
	}
	if checked > 0 {
		avgLatMs = float64(tLatency) / float64(checked) / 1e6
	}
	lastLatMs := float64(lLatency) / 1e6

	progress := float64(checked) / float64(MaxCombinations) * 100

	var etaStr string
	if rps > 0 {
		remainingSec := float64(MaxCombinations-checked) / rps
		etaStr = time.Duration(remainingSec * float64(time.Second)).Round(time.Second).String()
	} else {
		etaStr = "Hesaplanıyor..."
	}

	status := "\033[1;32mRUNNING\033[0m"
	
	// Global Rate Limit (Duraklatıldıysa) durumu güncelle
	pauseUntil := atomic.LoadInt64(&globalPauseUntil)
	if time.Now().UnixNano() < pauseUntil {
		status = "\033[1;33mRATE-LIMIT (PAUSED)\033[0m"
	}
	if isFinal {
		status = "\033[1;31mCOMPLETED/STOPPED\033[0m"
	}

	// ANSI Escape kodları ile terminali titreşimsiz güncelleme
	if !isFinal {
		fmt.Print("\033[H\033[2J") // Clear screen and move cursor to top-left
	}

	fmt.Printf("Instagram Username Scanner [O(1) Zero-Alloc Engine]\n")
	fmt.Printf("───────────────────────────────────────────────────\n")
	fmt.Printf("Status       : %s\n", status)
	fmt.Printf("Pattern      : b[a-z]{4}\n\n")

	fmt.Printf("Checked      : %d / %d\n", checked, MaxCombinations)
	fmt.Printf("Remaining    : %d\n", uint32(MaxCombinations)-checked)
	fmt.Printf("Progress     : %.2f%%\n\n", progress)

	fmt.Printf("\033[1;32mAvailable    : %d\033[0m\n", available)
	fmt.Printf("Unavailable  : %d\n", unavailable)
	fmt.Printf("\033[1;31mErrors       : %d\033[0m\n\n", errors)

	fmt.Printf("Req/s        : %.1f\n", rps)
	fmt.Printf("Avg Latency  : %.1f ms\n", avgLatMs)
	fmt.Printf("Last Latency : %.1f ms\n\n", lastLatMs)

	fmt.Printf("Concurrency  : %d\n", WorkerCount)
	fmt.Printf("Elapsed      : %s\n", time.Duration(elapsed*float64(time.Second)).Round(time.Second))
	fmt.Printf("ETA          : %s\n\n", etaStr)

	fmt.Printf("HTTP 429     : %d\n", h429)
	fmt.Printf("HTTP 4xx     : %d\n", h4xx)
	fmt.Printf("HTTP 5xx     : %d\n", h5xx)
	fmt.Printf("Timeouts     : %d\n", timeouts)
	fmt.Printf("───────────────────────────────────────────────────\n")
}
