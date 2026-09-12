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
	threads    = 1 // 🛡️ GÜVENLİK: Discord için ban riskini sıfırlamak adına zorunlu 1 yapıldı
	workerID   = getEnvInt("WORKER_ID", 0)
	totalNodes = getEnvInt("TOTAL_WORKERS", 1)
	webhookURL = os.Getenv("WEBHOOK_URL")
)

var client = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true, // Modern tarayıcı gibi davranması için
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
var (
	scannedCount int64
	currentLoop  int64 = 1
)

func main() {
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
	case 4:
		charset = []byte("abcdefghijklmnopqrstuvwxyz0123456789_")
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

	jobs := make(chan string, threads*2)
	results := make(chan CheckResult, 100)

	go resultHandler(ctx, results)

	for i := 0; i < threads; i++ {
		go worker(ctx, jobs, results)
	}

outerLoop:
	for {
		atomic.StoreInt64(&currentLoop, atomic.LoadInt64(&currentLoop))
		fmt.Printf("\n🔄 --- [TARAMA DÖNGÜSÜ: %d. TUR BAŞLIYOR] ---\n", atomic.LoadInt64(&currentLoop))

		for idx := startIdx; idx < endIdx; idx++ {
			select {
			case <-ctx.Done():
				break outerLoop
			default:
				name := generateDiscordName(idx, length, charset)
				jobs <- name
			}
		}

		fmt.Printf("✅ --- [%d. TUR LİSTESİ BİTTİ, 5 SANİYE BEKLENİYOR] ---\n", atomic.LoadInt64(&currentLoop))
		time.Sleep(5 * time.Second)
		atomic.AddInt64(&currentLoop, 1)
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

	if score > 10.0 { score = 10.0 }
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
			atomic.AddInt64(&scannedCount, 1)
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

			if _, exists := blacklistMap[lowerName]; exists { continue }
			if _, seen := seenHits[lowerName]; seen { continue }
			seenHits[lowerName] = struct{}{}

			if res.Status == StatusUsed || res.Status == StatusUnknown { continue }

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
