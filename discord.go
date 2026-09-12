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
	"strings"
	"time"
)

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
	// 🛡️ GÜVENLİK: İnsansı davranış simülasyonu. İstek atmadan önce rastgele 3-6 saniye bekle.
	jitter := time.Duration(3000+rand.Intn(3000)) * time.Millisecond
	time.Sleep(jitter)

	payload := fmt.Sprintf(`{"username":"%s"}`, name)
	req, err := http.NewRequestWithContext(ctx, "POST", "https://discord.com/api/v9/users/@me/pomelo-attempt", strings.NewReader(payload))
	if err != nil {
		results <- CheckResult{Name: name, Status: StatusUnknown}
		return
	}

	// 🛡️ GÜVENLİK: Google Chrome tarayıcı taklidi
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://discord.com")

	if token := os.Getenv("DISCORD_TOKEN"); token != "" {
		req.Header.Set("Authorization", token)
	}

	resp, err := client.Do(req)
	if err != nil {
		results <- CheckResult{Name: name, Status: StatusUnknown}
		return
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	respStr := string(bodyBytes)

	if resp.StatusCode == 200 {
		if strings.Contains(respStr, `"taken": false`) || strings.Contains(respStr, `"taken":false`) {
			results <- CheckResult{Name: name, Status: StatusAvailable}
			return
		}
		results <- CheckResult{Name: name, Status: StatusUsed}
		return
	} else if resp.StatusCode == 429 {
		// 🛡️ GÜVENLİK: Çok fazla istek uyarısı alırsak IP dinlendirmesi için 60 saniye bekle
		fmt.Printf("⚠️ [DISCORD RATE LIMIT] Sistem %s için 60 saniye duraklatıldı!\n", name)
		time.Sleep(60 * time.Second)
		results <- CheckResult{Name: name, Status: StatusUnknown}
		return
	} else {
		results <- CheckResult{Name: name, Status: StatusUnknown}
		return
	}
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
	if err != nil { return }

	req, err := http.NewRequest("POST", webhookURL, bytes.NewReader(jsonBytes))
	if err != nil { return }
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil { return }
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}
