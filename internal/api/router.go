package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"log"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"server-test/internal/worker" // Sesuaikan dengan nama module di go.mod Anda
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{w, http.StatusOK}
		next.ServeHTTP(recorder, r)
		log.Printf("[%s] %s | Status: %d | Durasi: %v", r.Method, r.URL.Path, recorder.statusCode, time.Since(start))
	})
}

func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
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

func SetupRouter(db *sql.DB) http.Handler {
	mux := http.NewServeMux()

	// GET & POST Notes
	mux.HandleFunc("/api/notes", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getNotesHandler(db, w, r)
		case http.MethodPost:
			authMiddleware(createNoteHandler)(w, r)
		default:
			http.Error(w, "Metode tidak diizinkan", http.StatusMethodNotAllowed)
		}
	})

	// Job Status
	mux.HandleFunc("/api/status/", jobStatusHandler)

	// Health Check
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		healthCheckHandler(db, w, r)
	})

	return CorsMiddleware(LoggingMiddleware(mux))
}

func getNotesHandler(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 10
	offset := 0

	if lStr := query.Get("limit"); lStr != "" {
		if parsedLimit, err := strconv.Atoi(lStr); err == nil && parsedLimit > 0 {
			if parsedLimit > 100 {
				limit = 100
			} else {
				limit = parsedLimit
			}
		}
	}
	if oStr := query.Get("offset"); oStr != "" {
		if parsedOffset, err := strconv.Atoi(oStr); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

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
	worker.JobStore.Store(jobID, worker.JobInfo{
		Status:    "Pending (In Queue)",
		UpdatedAt: time.Now(),
	})

	select {
	case worker.Jobs <- worker.Job{ID: jobID, Payload: input.Content}:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Data sedang diproses",
			"job_id":  jobID,
		})
	default:
		worker.JobStore.Store(jobID, worker.JobInfo{
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

	val, exists := worker.JobStore.Load(jobID)
	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Job ID tidak ditemukan atau sudah kedaluwarsa"})
		return
	}

	info := val.(worker.JobInfo)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"job_id":     jobID,
		"status":     info.Status,
		"updated_at": info.UpdatedAt,
	})
}

func healthCheckHandler(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	dbStatus := "healthy"
	if err := db.Ping(); err != nil {
		dbStatus = "unhealthy: " + err.Error()
	}

	queueLength := len(worker.Jobs)
	queueCapacity := cap(worker.Jobs)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB := float64(m.Alloc) / 1024 / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	overallStatus := "UP"
	statusCode := http.StatusOK
	if dbStatus != "healthy" || queueLength >= queueCapacity {
		overallStatus = "DEGRADED"
		statusCode = http.StatusServiceUnavailable
	}

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
