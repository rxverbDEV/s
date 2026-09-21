package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/valyala/fasthttp"
)

// --- YAPILANDIRMA ---
const (
	MaxCombinations = 456976 // b + 4 harf (26^4)
	WorkerCount     = 150    // Sürdürülebilir connection havuzu boyutu

	// Güncellenmiş Webhook URL'niz  
	WebhookURL      = "https://discord.com/api/webhooks/1548315868944142386/68B2biKu_Wz2_KNVwnwJwgtAbixCNnBcghiUDKFq8m5HkqsH0Ecipnsbx3i3BqzyOnLI"  
	
	OutputFile      = "available_instagram.txt"  
	TargetBaseURL   = "https://www.instagram.com" // Instagram Ana Domain
)

// --- GLOBAL ATOMİK İSTATİSTİKLER ---
// L1/L2 Cache dostu, memory padding uygulanmış global sayaclar
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

	globalPauseUntil int64 // UnixNano timestamp (Rate-limit global kilit)
)

// --- WORKER LOCAL METRIC BATCHING (CACHE CONTENTION ÖNLEYİCİ) ---
type LocalStats struct {
	checked     uint32
	available   uint32
	unavailable uint32
	errors      uint32
	http429     uint32
	http4xx     uint32
	http5xx     uint32
	timeouts    uint32
	totalLatNs  uint64
	lastLatNs   uint64
}

func (s *LocalStats) Flush() {
	if s.checked == 0 {
		return
	}
	atomic.AddUint32(&statChecked, s.checked)
	atomic.AddUint32(&statAvailable, s.available)
	atomic.AddUint32(&statUnavailable, s.unavailable)
	atomic.AddUint32(&statErrors, s.errors)
	atomic.AddUint32(&statHTTP429, s.http429)
	atomic.AddUint32(&statHTTP4xx, s.http4xx)
	atomic.AddUint32(&statHTTP5xx, s.http5xx)
	atomic.AddUint32(&statTimeouts, s.timeouts)
	atomic.AddUint64(&totalLatencyNs, s.totalLatNs)
	atomic.StoreUint64(&lastLatencyNs, s.lastLatNs)

	*s = LocalStats{} // Local veriyi sıfırla
}

type DiscordPayload struct {
	Content string `json:"content"`
}

// Zero-allocation string dönüşümü
func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful Shutdown - OS sinyallerini dinle  
	sigChan := make(chan os.Signal, 1)  
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)  
	go func() {  
		<-sigChan  
		cancel()  
	}()  

	// High-Performance fasthttp Client (Connection Pooling)  
	client := &fasthttp.Client{  
		Name:                     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",  
		MaxConnsPerHost:          WorkerCount,  
		ReadTimeout:              5 * time.Second,  
		WriteTimeout:             5 * time.Second,  
		MaxIdemRequestsPerConn:   1, // Güvenli bağlantı yenileme  
		NoDefaultUserAgentHeader: true,  
	}  

	resultsChan := make(chan string, 2000)  
	webhookChan := make(chan string, 5000)  
	var wg sync.WaitGroup  

	startTime := time.Now()  

	// Arka Plan Servisleri (Non-blocking I/O)  
	wg.Add(1)  
	go diskWriter(ctx, &wg, resultsChan)  

	wg.Add(1)  
	go discordWebhookWorker(ctx, &wg, webhookChan)  

	go liveDashboard(ctx, startTime)  

	// Worker Havuzunu Başlat  
	for i := 0; i < WorkerCount; i++ {  
		wg.Add(1)  
		go worker(ctx, &wg, client, resultsChan, webhookChan)  
	}  

	// Bitişi bekle  
	wg.Wait()  

	renderDashboard(startTime, true)  
	fmt.Println("\n[✔] Program güvenli ve maksimum performans ile sonlandırıldı.")
}

// --- WORKER ENGINE (TAMAMEN ZERO-ALLOCATION) ---
func worker(ctx context.Context, wg *sync.WaitGroup, client *fasthttp.Client, resultsChan, webhookChan chan<- string) {
	defer wg.Done()

	var localStats LocalStats  

	// Pre-allocated Request & Response (Object Reuse - Garbage Collector yükü 0)  
	req := fasthttp.AcquireRequest()  
	resp := fasthttp.AcquireResponse()  
	defer fasthttp.ReleaseRequest(req)  
	defer fasthttp.ReleaseResponse(resp)  

	req.Header.SetMethod("GET")  
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")  
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	// Base URI'nin byte dizisi olarak hazırlanması: https://www.instagram.com/bAAAA/
	baseURI := []byte(TargetBaseURL + "/bAAAA/")  
	usernameOffset := len(baseURI) - 6 // 'b' harfinin başladığı indeks  

	for {  
		select {  
		case <-ctx.Done():  
			localStats.Flush()  
			return  
		default:  
		}  

		// GÜVENLİK VE MAKSİMUM HIZ: Global Backoff Kontrolü  
		pauseTimestamp := atomic.LoadInt64(&globalPauseUntil)  
		if now := time.Now().UnixNano(); now < pauseTimestamp {  
			localStats.Flush()  
			time.Sleep(time.Duration(pauseTimestamp - now))  
		}  

		// O(1) Lock-Free Index Alınması  
		idx := atomic.AddUint32(&currentIndex, 1) - 1  
		if idx >= MaxCombinations {  
			localStats.Flush()  
			return  
		}  

		// Matematiksel Kullanıcı Adı Üretimi (In-Place Byte Mutation)  
		temp := idx  
		c4 := temp % 26  
		temp /= 26  
		c3 := temp % 26  
		temp /= 26  
		c2 := temp % 26  
		temp /= 26  
		c1 := temp % 26  

		baseURI[usernameOffset+1] = byte('a' + c1)  
		baseURI[usernameOffset+2] = byte('a' + c2)  
		baseURI[usernameOffset+3] = byte('a' + c3)  
		baseURI[usernameOffset+4] = byte('a' + c4)  

		req.SetRequestURIBytes(baseURI)  

		// İstek Gönderimi  
		startReq := time.Now()  
		err := client.DoTimeout(req, resp, 5*time.Second)  
		latNs := uint64(time.Since(startReq).Nanoseconds())  

		localStats.totalLatNs += latNs  
		localStats.lastLatNs = latNs  
		localStats.checked++  

		if err != nil {  
			if err == fasthttp.ErrTimeout {  
				localStats.timeouts++  
			} else {  
				localStats.errors++  
			}  
		} else {  
			statusCode := resp.StatusCode()  
			bodyBytes := resp.Body()

			switch statusCode {  
			case 404:  
				// Kesinlikle Alınabilir (Profil yok)
				localStats.available++  
				username := bytesToString(baseURI[usernameOffset : usernameOffset+5])  
				select {  
				case resultsChan <- username:  
				case <-ctx.Done():  
				}  
				select {  
				case webhookChan <- username:  
				case <-ctx.Done():  
				}  
			case 200:  
				// Instagram 200 dönse bile bazen hata / challenge / login sayfaları dönebilir.
				// Sayfa içeriğini analiz ederek gerçek durumu ayırt ediyoruz.
				if bytes.Contains(bodyBytes, []byte("Page Not Found")) || 
				   bytes.Contains(bodyBytes, []byte("The link you followed may be broken")) ||
				   bytes.Contains(bodyBytes, []byte("üzgünüm, bu sayfaya ulaşılamıyor")) {
					// Aslında profil yok -> ALINABİLİR
					localStats.available++  
					username := bytesToString(baseURI[usernameOffset : usernameOffset+5])  
					select {  
					case resultsChan <- username:  
					case <-ctx.Done():  
					}  
					select {  
					case webhookChan <- username:  
					case <-ctx.Done():  
					}  
				} else if bytes.Contains(bodyBytes, []byte("accounts/login")) || 
				          bytes.Contains(bodyBytes, []byte("Please wait")) ||
						  bytes.Contains(bodyBytes, []byte("checkpoint")) {
					// BİLİNMİYOR (Bot duvarı / Login yönlendirmesi - Kesinlikle müsait sayma)
					localStats.errors++
				} else {
					// Profil gerçekten var -> ALINAMAZ (Unavailable)
					localStats.unavailable++  
				}
			case 429:  
				localStats.http429++  
				  
				// GÜVENLİK TETİKLEYİCİSİ (Rate Limit Saygısı)  
				newPause := time.Now().Add(3 * time.Second).UnixNano()  
				for {  
					curr := atomic.LoadInt64(&globalPauseUntil)  
					if newPause <= curr {  
						break  
					}  
					if atomic.CompareAndSwapInt64(&globalPauseUntil, curr, newPause) {  
						break  
					}  
				}  
			default:  
				if statusCode >= 400 && statusCode < 500 {  
					localStats.http4xx++  
				} else if statusCode >= 500 {  
					localStats.http5xx++  
				} else {  
					localStats.errors++  
				}  
			}  
		}  

		// Cache Contention Önleyici: İstatistikleri her 32 istekte bir flush et  
		if localStats.checked >= 32 {  
			localStats.Flush()  
		}  

		// Nesneleri bir sonraki tur için sıfırla (Reuse)  
		req.Reset()  
		resp.Reset()  
		req.Header.SetMethod("GET")  
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")  
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	}
}

// --- DISK WRITER (BUFFERED ASYNC I/O) ---
func diskWriter(ctx context.Context, wg *sync.WaitGroup, resultsChan <-chan string) {
	defer wg.Done()

	file, err := os.OpenFile(OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)  
	if err != nil {  
		return  
	}  
	defer file.Close()  

	writer := bufio.NewWriterSize(file, 65536) // 64KB Write Buffer  
	defer writer.Flush()  

	ticker := time.NewTicker(2 * time.Second)  
	defer ticker.Stop()  

	for {  
		select {  
		case <-ctx.Done():  
			for len(resultsChan) > 0 {  
				writer.WriteString(<-resultsChan + "\n")  
			}  
			return  
		case username, ok := <-resultsChan:  
			if !ok {  
				return  
			}  
			writer.WriteString(username + "\n")  
		case <-ticker.C:  
			writer.Flush()  
		}  
	}
}

// --- DISCORD WEBHOOK WORKER (BATCHING & BACKOFF) ---
func discordWebhookWorker(ctx context.Context, wg *sync.WaitGroup, webhookChan <-chan string) {
	defer wg.Done()

	if WebhookURL == "" {  
		return  
	}  

	client := &fasthttp.Client{ReadTimeout: 5 * time.Second}  
	var batch []string  
	ticker := time.NewTicker(1 * time.Second)  
	defer ticker.Stop()  

	sendBatch := func(usernames []string) {  
		if len(usernames) == 0 {  
			return  
		}  
		content := "✅ **Müsait Username(ler) Bulundu:**\n"  
		for _, u := range usernames {  
			content += "- " + u + " (https://www.instagram.com/" + u + "/)\n"  
		}  

		payload, _ := json.Marshal(DiscordPayload{Content: content})  

		req := fasthttp.AcquireRequest()  
		resp := fasthttp.AcquireResponse()  
		defer fasthttp.ReleaseRequest(req)  
		defer fasthttp.ReleaseResponse(resp)  

		req.SetRequestURI(WebhookURL)  
		req.Header.SetMethod("POST")  
		req.Header.SetContentType("application/json")  
		req.SetBody(payload)  

		if err := client.DoTimeout(req, resp, 5*time.Second); err == nil {  
			if resp.StatusCode() == 429 {  
				retryAfterHeader := string(resp.Header.Peek("Retry-After"))  
				sleepSec := 3  
				if val, err := strconv.Atoi(retryAfterHeader); err == nil {  
					sleepSec = val  
				}  
				time.Sleep(time.Duration(sleepSec) * time.Second)  
			}  
		}  
	}  

	for {  
		select {  
		case <-ctx.Done():  
			sendBatch(batch)  
			return  
		case username, ok := <-webhookChan:  
			if !ok {  
				return  
			}  
			batch = append(batch, username)  
			if len(batch) >= 10 {  
				sendBatch(batch)  
				batch = nil  
			}  
		case <-ticker.C:  
			sendBatch(batch)  
			batch = nil  
		}  
	}
}

// --- CANLI DASHBOARD ---
func liveDashboard(ctx context.Context, startTime time.Now()) {
	ticker := time.NewTicker(250 * time.Millisecond) // Akıcı terminal görünümü
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
	pauseUntil := atomic.LoadInt64(&globalPauseUntil)  
	if time.Now().UnixNano() < pauseUntil {  
		status = "\033[1;33mRATE-LIMIT (PAUSED)\033[0m"  
	}  
	if isFinal {  
		status = "\033[1;31mSTOPPED\033[0m"  
	}  

	if !isFinal {  
		fmt.Print("\033[H\033[2J")  
	}  

	fmt.Printf("Instagram Profile Scanner [fasthttp Deep-Check Engine]\n")  
	fmt.Printf("───────────────────────────────────────────────────\n")  
	fmt.Printf("Status       : %s\n", status)  
	fmt.Printf("Pattern      : b[a-z]{4}\n\n")  

	fmt.Printf("Checked      : %d / %d\n", checked, MaxCombinations)  
	fmt.Printf("Remaining    : %d\n", MaxCombinations-checked)  
	fmt.Printf("Progress     : %.2f%%\n\n", progress)  

	fmt.Printf("\033[1;32mAvailable (Alınabilir) : %d\033[0m\n", available)  
	fmt.Printf("Unavailable (Alınamaz): %d\n", unavailable)  
	fmt.Printf("\033[1;31mErrors/Bilinmiyor     : %d\033[0m\n\n", errors)  

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
