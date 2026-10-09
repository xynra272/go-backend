package database

import (
	"database/sql"
	"log"

	_ "modernc.org/sqlite"
)

func InitDB() *sql.DB {
	db, err := sql.Open("sqlite", "./app.db?_pragma=journal_mode(WAL)&_pragma=syncronous(NORMAL)&_PRAGMA=BUSY_TIMEOUT(5000)")
	if err != nil {
		log.Fatalf("gagal membuka dtabase: %v", err)
	}

	_, err = db.Exec(`
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = -2000;
	PRAGMA busy_timeout = 5000;
	`)
	if err != nil {
		log.Fatalf("Gagal mengonfigurasi PRAGMA SQLite: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS notes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		content TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		log.Fatalf("Gagal membuat tabel: %v", err)
	}

	return db
}
