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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"runtime"
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

// --- INISIALISASI DATABASE DENGAN WAL MODE & PRAGMA TUNING ---
func initDB() {
	var err error
	// Membuka database dengan parameter tambahan untuk mengaktifkan WAL mode secara instan
	db, err = sql.Open("sqlite", "./app.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatalf("Gagal membuka database: %v", err)
	}

	// Eksekusi tambahan PRAGMA untuk performa maksimal di tingkat koneksi
	_, err = db.Exec(`
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = -2000; -- Alokasikan cache ~2MB RAM untuk SQLite
	PRAGMA busy_timeout = 5000;
	`)
	if err != nil {
		log.Fatalf("Gagal mengonfigurasi PRAGMA SQLite: %v", err)
	}

	// Buat tabel catatan
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

// --- TTL BACKGROUND CLEANER ---
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

// --- MIDDLEWARES ---
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

// --- HANDLERS ---
func getNotesHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// Nilai default pagination
	limit := 10
	offset := 0

	// Parse parameter limit jika ada
	if lStr := query.Get("limit"); lStr != "" {
		if parsedLimit, err := strconv.Atoi(lStr); err == nil && parsedLimit > 0 {
			if parsedLimit > 100 {
				limit = 100 // Batasi maksimal 100 item per request untuk mencegah beban berlebih
			} else {
				limit = parsedLimit
			}
		}
	}

	// Parse parameter offset jika ada
	if oStr := query.Get("offset"); oStr != "" {
		if parsedOffset, err := strconv.Atoi(oStr); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	// Kueri SQLite dengan LIMIT dan OFFSET
	rows, err := db.Query("SELECT id, content, created_at FROM notes ORDER BY id DESC LIMIT ? OFFSET ?", limit, offset)
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

	// Inisialisasi slice kosong agar menghasilkan JSON `[]` alih-alih `null` jika data kosong
	notes := []Note{}
	for rows.Next() {
		var n Note
		var tStr string
		if err := rows.Scan(&n.ID, &n.Content, &tStr); err == nil {
			n.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", tStr)
			notes = append(notes, n)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"limit":  limit,
		"offset": offset,
		"data":   notes,
	})
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

// --- MAIN ROUTINE & GRACEFUL SHUTDOWN ---
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
	mux.HandleFunc("/api/health", healthCheckHandler)	
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
		fmt.Printf("[*] Server WAL Mode Optimal beroperasi di port 8080...\n")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server HTTP gagal: %v", err)
		}
	}()

	// Penanganan Sinyal Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[SHUTDOWN] Sinyal berhenti diterima. Memulai Graceful Shutdown...")

	// Langkah A: Hentikan server HTTP
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[SHUTDOWN] Peringatan penutupan HTTP: %v\n", err)
	}

	// Langkah B: Habiskan antrean komputasi
	close(jobs)
	workerWg.Wait()
	log.Println("[SHUTDOWN] Semua Compute Workers selesai memproses sisa antrean.")

	// Langkah C: Habiskan antrean tulis database
	close(results)
	writerWg.Wait()
	log.Println("[SHUTDOWN] DB Writer selesai menulis seluruh data ke SQLite.")

	// Langkah D: Tutup database
	db.Close()
	log.Println("[SHUTDOWN] Sistem berhasil dimatikan dengan aman.")
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Periksa denyut nadi Database SQLite
	dbStatus := "healthy"
	if err := db.Ping(); err != nil {
		dbStatus = "unhealthy: " + err.Error()
	}

	// 2. Ambil metrik antrean RAM
	queueLength := len(jobs)
	queueCapacity := cap(jobs)

	// 3. Ambil statistik memori Go Runtime
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Konversi byte ke Megabyte (MB) untuk keterbacaan yang lebih mudah
	allocMB := float64(m.Alloc) / 1024 / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	// Tentukan status keseluruhan sistem
	overallStatus := "UP"
	statusCode := http.StatusOK
	if dbStatus != "healthy" || queueLength >= queueCapacity {
		overallStatus = "DEGRADED"
		statusCode = http.StatusServiceUnavailable
	}

	// Susun respons JSON
	response := map[string]any{
		"status":   overallStatus,
		"database": dbStatus,
		"queue": map[string]any{
			"current_length": queueLength,
			"capacity":       queueCapacity,
		},
		"memory": map[string]any{
			"alloc_mb": fmt.Sprintf("%.2f MB", allocMB),
			"sys_mb":   fmt.Sprintf("%.2f MB", sysMB),
		},
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}
