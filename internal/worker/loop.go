package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

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

var (
	Jobs     = make(chan Job, MaxQueue)
	Results  = make(chan Result, MaxQueue)
	JobStore sync.Map
)

func ComputeWorker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range Jobs {
		time.Sleep(100 * time.Millisecond) // Simulasi kerja berat
		processed := fmt.Sprintf("[Sanitized by W%d] %s", id, job.Payload)

		JobStore.Store(job.ID, JobInfo{
			Status:    "Processing (I/O Wait)",
			UpdatedAt: time.Now(),
		})
		Results <- Result{JobID: job.ID, ProcessedData: processed}
	}
}

func DBWriter(db *sql.DB, writerDone *sync.WaitGroup) {
	defer writerDone.Done()
	for res := range Results {
		_, err := db.Exec("INSERT INTO notes (content) VALUES (?)", res.ProcessedData)
		if err != nil {
			log.Printf("[DB FATAL] %v\n", err)
			JobStore.Store(res.JobID, JobInfo{
				Status:    "Failed (DB Error)",
				UpdatedAt: time.Now(),
			})
			continue
		}
		JobStore.Store(res.JobID, JobInfo{
			Status:    "Success",
			UpdatedAt: time.Now(),
		})
	}
}

func StartTTLBackgroundCleaner(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				now := time.Now()
				JobStore.Range(func(key, value interface{}) bool {
					info, ok := value.(JobInfo)
					if !ok {
						return true
					}
					isFinal := info.Status == "Success" || strings.HasPrefix(info.Status, "Failed")
					if isFinal && now.Sub(info.UpdatedAt) > 5*time.Minute {
						JobStore.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
