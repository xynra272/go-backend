package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

var db *sql.DB

type Job struct {
	ID      string
	Payload string
}

type Result struct {
	JobID         string
	ProcessedData string
}

type JobInfo struct {
	Status    string
	UpdatedAt time.Time
}

const MaxQueue = 100

var jobs = make(chan Job, MaxQueue)
var results = make(chan Result, MaxQueue)
var jobStore sync.Map

func initDB() {
	var err error
	db, err = sql.Open("sqlite", "./app.db")
	if err != nil {
		log.Fatalf("Gagal membuka database: %v", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS notes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		content TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		log.Fatalf("Gagal membuat tabel: %v", err)
	}
}

// --- WORKERS DENGAN WAITGROUP ---
func computeWorker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		time.Sleep(100 * time.Millisecond) // Simulasi kerja berat
		processed := fmt.Sprintf("[Sanitized by W%d] %s", id, job.Payload)

		jobStore.Store(job.ID, JobInfo{
			Status:    "Processing (I/O Wait)",
			UpdatedAt: time.Now(),
		})
		results <- Result{JobID: job.ID, ProcessedData: processed}
	}
}

func dbWriter(writerDone *sync.WaitGroup) {
	defer writerDone.Done()
	for res := range results {
		_, err := db.Exec("INSERT INTO notes (content) VALUES (?)", res.ProcessedData)
		if err != nil {
			log.Printf("[DB FATAL] %v\n", err)
			jobStore.Store(res.JobID, JobInfo{
				Status:    "Failed (DB Error)",
				UpdatedAt: time.Now(),
			})
			continue
		}
		jobStore.Store(res.JobID, JobInfo{
			Status:    "Success",
			UpdatedAt: time.Now(),
		})
	}
}

func startTTLBackgroundCleaner(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				now := time.Now()
				jobStore.Range(func(key, value interface{}) bool {
					info, ok := value.(JobInfo)
					if !ok {
						return true
					}
					isFinal := info.Status == "Success" || strings.HasPrefix(info.Status, "Failed")
					if isFinal && now.Sub(info.UpdatedAt) > 5*time.Minute {
						jobStore.Delete(key)
					}
					return true
				})
			}
		}
	}()
}

// Middleware tetap sama...
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{w, http.StatusOK}
		next.ServeHTTP(recorder, r)
		log.Printf("[%s] %s | Status: %d | Durasi: %v", r.Method, r.URL.Path, recorder.statusCode, time.Since(start))
	})
}

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		expectedKey := os.Getenv("API_KEY")
		clientKey := r.Header.Get("X-API-Key")
		if expectedKey == "" || clientKey != expectedKey {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "Kredensial tidak valid"})
			return
		}
		next(w, r)
	}
}

func getNotesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, content, created_at FROM notes ORDER BY id DESC")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Note struct {
		ID        int       `json:"id"`
		Content   string    `json:"content"`
		CreatedAt time.Time `json:"created_at"`
	}
	var notes []Note
	for rows.Next() {
		var n Note
		var tStr string
		if err := rows.Scan(&n.ID, &n.Content, &tStr); err == nil {
			n.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", tStr)
			notes = append(notes, n)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(notes)
}

func createNoteHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Content == "" {
		http.Error(w, "Payload JSON tidak valid", http.StatusBadRequest)
		return
	}

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

	jobStore.Store(jobID, JobInfo{
		Status:    "Pending (In Queue)",
		UpdatedAt: time.Now(),
	})

	select {
	case jobs <- Job{ID: jobID, Payload: input.Content}:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Data sedang diproses",
			"job_id":  jobID,
		})
	default:
		jobStore.Store(jobID, JobInfo{
			Status:    "Failed (Queue Full)",
			UpdatedAt: time.Now(),
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "Server kelebihan beban"})
	}
}

func jobStatusHandler(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/api/status/")
	if jobID == "" {
		http.Error(w, "Job ID tidak disertakan", http.StatusBadRequest)
		return
	}

	val, exists := jobStore.Load(jobID)
	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Job ID tidak ditemukan atau sudah kedaluwarsa"})
		return
	}

	info := val.(JobInfo)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"job_id":     jobID,
		"status":     info.Status,
		"updated_at": info.UpdatedAt,
	})
}

func main() {
	if os.Getenv("API_KEY") == "" {
		log.Fatal("KRITIS: API_KEY tidak ditemukan!")
	}

	initDB()

	// Context untuk TTL Cleaner
	ctxClean, cancelClean := context.WithCancel(context.Background())
	defer cancelClean()
	startTTLBackgroundCleaner(ctxClean)

	// Sinkronisasi Worker
	var workerWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// 1. Nyalakan Database Writer
	writerWg.Add(1)
	go dbWriter(&writerWg)

	// 2. Nyalakan Compute Workers
	const numWorkers = 3
	for w := 1; w <= numWorkers; w++ {
		workerWg.Add(1)
		go computeWorker(w, &workerWg)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/notes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getNotesHandler(w, r)
		case http.MethodPost:
			authMiddleware(createNoteHandler)(w, r)
		default:
			http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/status/", jobStatusHandler)

	loggedMux := loggingMiddleware(mux)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: loggedMux,
	}

	// Jalankan server di goroutine terpisah
	go func() {
		fmt.Printf("[*] Server Robust beroperasi di port 8080...\n")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server HTTP gagal: %v", err)
		}
	}()

	// --- PENANGANAN SINYAL GRACEFUL SHUTDOWN ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[SHUTDOWN] Sinyal berhenti diterima. Memulai Graceful Shutdown...")

	// Langkah A: Hentikan server HTTP (tolak request baru, selesaikan request aktif)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[SHUTDOWN] Peringatan penutupan HTTP: %v\n", err)
	}

	// Langkah B: Tutup channel jobs dan tunggu compute workers selesai menghabiskan antrean
	close(jobs)
	workerWg.Wait()
	log.Println("[SHUTDOWN] Semua Compute Workers selesai memproses sisa antrean.")

	// Langkah C: Tutup channel results dan tunggu DB Writer menulis semuanya ke SQLite
	close(results)
	writerWg.Wait()
	log.Println("[SHUTDOWN] DB Writer selesai menulis seluruh data ke SQLite.")

	// Langkah D: Tutup koneksi database secara permanen
	db.Close()
	log.Println("[SHUTDOWN] Sistem berhasil dimatikan dengan aman. Tidak ada data korup.")
}
