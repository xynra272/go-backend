package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"server-test/internal/api"
	"server-test/internal/database"
	"server-test/internal/worker"
)

func main() {
	if os.Getenv("API_KEY") == "" {
		log.Fatal("KRITIS: API_KEY tidak ditemukan!")
	}

	db := database.InitDB()

	// Context untuk TTL Cleaner
	ctxClean, cancelClean := context.WithCancel(context.Background())
	defer cancelClean()
	worker.StartTTLBackgroundCleaner(ctxClean)

	var workerWg sync.WaitGroup
	var writerWg sync.WaitGroup

	// 1. Nyalakan Database Writer
	writerWg.Add(1)
	go worker.DBWriter(db, &writerWg)

	// 2. Nyalakan Compute Workers
	const numWorkers = 3
	for w := 1; w <= numWorkers; w++ {
		workerWg.Add(1)
		go worker.ComputeWorker(w, &workerWg)
	}

	handler := api.SetupRouter(db)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	go func() {
		log.Printf("[*] Server Modular beroperasi di port 8080...\n")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server HTTP gagal: %v", err)
		}
	}()

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[SHUTDOWN] Sinyal berhenti diterima...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)

	close(worker.Jobs)
	workerWg.Wait()

	close(worker.Results)
	writerWg.Wait()

	db.Close()
	log.Println("[SHUTDOWN] Sistem berhasil dimatikan.")
}
