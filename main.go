package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite"
)

var ctx = context.Background()
var rdb *redis.Client
var db *sql.DB

var erpURL = os.Getenv("ERP_URL")

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	},
}

var stmtUpdateFailed *sql.Stmt
var stmtUpdateMatched *sql.Stmt
var stmtUpdateUnmatched *sql.Stmt

type InvoicePayload struct {
	InvoiceID   string  `json:"invoice_id"`
	ContentHash string  `json:"content_hash"`
	Amount      float64 `json:"amount"`
	VendorID    string  `json:"vendor_id"`
	PONumber    string  `json:"po_number"`
	Currency    string  `json:"currency"`
}

func initSystem() {
	if erpURL == "" {
		erpURL = "http://mock-erp:9100/erp/purchase-orders/match"
	}

	dbPath := os.Getenv("DATABASE_URL")
	if dbPath == "" {
		dbPath = "./invoices.db?_busy_timeout=5000"
	}
	var err error
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("Database connection error: %v", err)
	}
	db.SetMaxOpenConns(10)

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS invoices (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		invoice_id TEXT,
		content_hash TEXT UNIQUE,
		amount REAL,
		vendor_id TEXT,
		status TEXT DEFAULT 'PENDING',
		reason TEXT
	);`
	if _, err = db.Exec(createTableQuery); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Prepare SQL statements
	stmtUpdateFailed, _ = db.Prepare("UPDATE invoices SET status = 'FAILED', reason = ? WHERE content_hash = ?")
	stmtUpdateMatched, _ = db.Prepare("UPDATE invoices SET status = 'MATCHED', reason = 'exact_match' WHERE content_hash = ?")
	stmtUpdateUnmatched, _ = db.Prepare("UPDATE invoices SET status = 'UNMATCHED', reason = ? WHERE content_hash = ?")

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Redis URL error: %v", err)
	}
	rdb = redis.NewClient(opt)
}

// --- API Handlers ---

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload InvoicePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.ContentHash == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON or missing content_hash"})
		return
	}

	// Atomic de-duplication gate using Redis SADD
	added, err := rdb.SAdd(ctx, "seen_invoice_hashes", payload.ContentHash).Result()
	if err != nil {
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	if added == 0 {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "duplicate_ignored"})
		return
	}

	// Save initial PENDING state in SQLite
	_, err = db.Exec(
		"INSERT INTO invoices (invoice_id, content_hash, amount, vendor_id, status) VALUES (?, ?, ?, ?, 'PENDING')",
		payload.InvoiceID, payload.ContentHash, payload.Amount, payload.VendorID,
	)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Push raw payload to Redis processing queue
	payloadBytes, _ := json.Marshal(payload)
	if err := rdb.LPush(ctx, "invoice_queue", payloadBytes).Err(); err != nil {
		http.Error(w, "Queue error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	var total, pending, matched, unmatched, failed int64

	db.QueryRow("SELECT COUNT(*) FROM invoices").Scan(&total)
	db.QueryRow("SELECT COUNT(*) FROM invoices WHERE status = 'PENDING'").Scan(&pending)
	db.QueryRow("SELECT COUNT(*) FROM invoices WHERE status = 'MATCHED'").Scan(&matched)
	db.QueryRow("SELECT COUNT(*) FROM invoices WHERE status = 'UNMATCHED'").Scan(&unmatched)
	db.QueryRow("SELECT COUNT(*) FROM invoices WHERE status = 'FAILED'").Scan(&failed)

	queueDepth, _ := rdb.LLen(ctx, "invoice_queue").Result()

	response := map[string]interface{}{
		"total_processed": total,
		"queue_depth":     queueDepth,
		"status_counts": map[string]int64{
			"PENDING":   pending,
			"MATCHED":   matched,
			"UNMATCHED": unmatched,
			"FAILED":    failed,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// --- Background Worker & Rate Limiter ---

func enforceGlobalRateLimit() {
	for {
		now := float64(time.Now().UnixNano()) / 1e9
		pipe := rdb.Pipeline()
		pipe.ZRemRangeByScore(ctx, "erp_rate_limit", "0", fmt.Sprintf("%f", now-1))
		cardCmd := pipe.ZCard(ctx, "erp_rate_limit")
		_, err := pipe.Exec(ctx)

		if err == nil && cardCmd.Val() < 10 {
			rdb.ZAdd(ctx, "erp_rate_limit", redis.Z{Score: now, Member: time.Now().UnixNano()})
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func callERP(poNumber, vendorID string, amount float64, currency string) (map[string]interface{}, error) {
	maxRetries := 5
	baseDelay := 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		enforceGlobalRateLimit()

		if currency == "" {
			currency = "USD"
		}
		url := fmt.Sprintf("%s?po_number=%s&vendor_id=%s&amount=%.2f&currency=%s", erpURL, poNumber, vendorID, amount, currency)
		resp, err := httpClient.Get(url)

		if err == nil {
			if resp.StatusCode == 429 || resp.StatusCode == 503 || resp.StatusCode == 504 {
				resp.Body.Close()
				log.Printf("Transient ERP Error %d. Retrying attempt %d...", resp.StatusCode, attempt)
			} else if resp.StatusCode == http.StatusOK {
				// Fixed defer leak by explicitly closing body after decoding
				var result map[string]interface{}
				decodeErr := json.NewDecoder(resp.Body).Decode(&result)
				resp.Body.Close()
				if decodeErr != nil {
					return nil, decodeErr
				}
				return result, nil
			} else {
				resp.Body.Close()
				return nil, fmt.Errorf("ERP returned status code %d", resp.StatusCode)
			}
		} else {
			log.Printf("Network error communicating with ERP: %v. Retrying...", err)
		}

		backoff := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
		time.Sleep(backoff)
	}
	return nil, fmt.Errorf("max retries exceeded for ERP call")
}

func processInvoice(payload map[string]interface{}) {
	contentHash, ok := payload["content_hash"].(string)
	if !ok {
		return
	}

	invoiceID, _ := payload["invoice_id"].(string)
	poNumber, hasPO := payload["po_number"].(string)
	amountVal, hasAmount := payload["amount"]

	if !hasPO || !hasAmount || invoiceID == "" {
		log.Printf("DEAD-LETTERED: Malformed invoice %s", invoiceID)
		stmtUpdateFailed.Exec("Malformed data", contentHash)
		return
	}

	amount := amountVal.(float64)
	vendorID, _ := payload["vendor_id"].(string)
	currency, _ := payload["currency"].(string)

	cacheKey := fmt.Sprintf("erp_cache:%s:%s:%.2f", poNumber, vendorID, amount)
	cachedResult, err := rdb.Get(ctx, cacheKey).Result()

	var erpData map[string]interface{}
	if err == nil {
		json.Unmarshal([]byte(cachedResult), &erpData)
	} else {
		erpData, err = callERP(poNumber, vendorID, amount, currency)
		if err != nil {
			stmtUpdateFailed.Exec(err.Error(), contentHash)
			return
		}
		erpBytes, _ := json.Marshal(erpData)
		rdb.Set(ctx, cacheKey, erpBytes, time.Hour)
	}

	matched, _ := erpData["matched"].(bool)
	if matched {
		stmtUpdateMatched.Exec(contentHash)
	} else {
		reason, _ := erpData["reason"].(string)
		if reason == "" {
			reason = "unknown"
		}
		stmtUpdateUnmatched.Exec(reason, contentHash)
	}
}

func startWorker() {
	log.Println("Background worker started...")
	for {
		result, err := rdb.BRPop(ctx, 1*time.Second, "invoice_queue").Result()
		if err == nil && len(result) >= 2 {
			var payload map[string]interface{}
			if err := json.Unmarshal([]byte(result[1]), &payload); err == nil {
				processInvoice(payload)
			}
		}
	}
}

func main() {
	initSystem()
	defer db.Close()
	defer rdb.Close()

	// Launch background worker concurrently in a Go routine
	go startWorker()

	// Start HTTP API server
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/api/invoices", ingestHandler)
	mux.HandleFunc("/api/stats", statsHandler)

	log.Println("Go API gateway starting on port 8000...")
	if err := http.ListenAndServe(":8000", mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
