package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
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

var webhookQueue = make(chan WebhookPayload, 1000)
var blacklistMap = make(map[string]struct{})
var currentLoop atomic.Int64

func main() {
	// Render PORT entegrasyonu ve sağlık kontrolü sunucusu
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
