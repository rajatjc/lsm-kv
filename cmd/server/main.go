// Command server exposes the LSM engine over HTTP and serves the web UI.
//
// Environment:
//
//	PORT         listen port (default 8080)
//	DATA_DIR     where WAL and SSTables live (default ./data)
//	SYNC_WRITES  "true" to fsync the WAL on every write
//	CORS_ORIGIN  allowed origin when the UI is hosted elsewhere (e.g. https://you.github.io)
//	MAX_DATA_MB  refuse writes once data on disk exceeds this (default 100)
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rajatjc/lsm-kv/engine"
	"github.com/rajatjc/lsm-kv/web"
)

const (
	maxKeyBytes   = 256
	maxValueBytes = 64 << 10
	maxBulk       = 5000
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	maxDataMB, _ := strconv.Atoi(env("MAX_DATA_MB", "100"))
	db, err := engine.Open(engine.Options{
		Dir:           env("DATA_DIR", "./data"),
		MemtableBytes: 64 << 10, // small so flushes and compactions are visible in the demo
		CompactAt:     4,
		SyncWrites:    env("SYNC_WRITES", "false") == "true",
	})
	if err != nil {
		log.Fatal(err)
	}
	s := &server{db: db, maxDataBytes: int64(maxDataMB) << 20}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/kv/{key}", s.get)
	mux.HandleFunc("PUT /api/kv/{key}", s.put)
	mux.HandleFunc("DELETE /api/kv/{key}", s.del)
	mux.HandleFunc("GET /api/scan", s.scan)
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("POST /api/flush", s.flush)
	mux.HandleFunc("POST /api/compact", s.compact)
	mux.HandleFunc("POST /api/bulk", s.bulk)
	mux.Handle("GET /", http.FileServerFS(web.Files))

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           cors(env("CORS_ORIGIN", ""), mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Hosting platforms send SIGTERM on redeploy; close the DB so the WAL is synced.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	if err := db.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}

type server struct {
	db           *engine.DB
	maxDataBytes int64
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func validKey(w http.ResponseWriter, key string) bool {
	if key == "" || len(key) > maxKeyBytes {
		writeErr(w, http.StatusBadRequest, "key must be 1-256 bytes")
		return false
	}
	return true
}

func (s *server) full() bool {
	st := s.db.Stats()
	total := int64(st.MemtableBytes) + st.WALBytes
	for _, t := range st.Tables {
		total += t.Bytes
	}
	return total > s.maxDataBytes
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if !validKey(w, key) {
		return
	}
	v, err := s.db.Get([]byte(key))
	if errors.Is(err, engine.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": string(v)})
}

func (s *server) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if !validKey(w, key) {
		return
	}
	if s.full() {
		writeErr(w, http.StatusInsufficientStorage, "demo storage limit reached")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "value must be at most 64KB")
		return
	}
	if err := s.db.Put([]byte(key), body); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) del(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if !validKey(w, key) {
		return
	}
	if err := s.db.Delete([]byte(key)); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) scan(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 || limit > 1000 {
		limit = 100
	}
	type kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	out := []kv{}
	for _, e := range s.db.Scan([]byte(r.URL.Query().Get("prefix")), limit) {
		out = append(out, kv{string(e.Key), string(e.Value)})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.db.Stats())
}

func (s *server) flush(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Flush(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.db.Stats())
}

func (s *server) compact(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Compact(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.db.Stats())
}

// bulk writes n random keys so the UI can show flushes and compactions happen.
func (s *server) bulk(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil || n <= 0 || n > maxBulk {
		n = 1000
	}
	if s.full() {
		writeErr(w, http.StatusInsufficientStorage, "demo storage limit reached")
		return
	}
	start := time.Now()
	buf := make([]byte, 8)
	for i := 0; i < n; i++ {
		rand.Read(buf)
		id := hex.EncodeToString(buf)
		if err := s.db.Put([]byte("user:"+id), []byte(`{"id":"`+id+`"}`)); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"written": n, "elapsedMs": time.Since(start).Milliseconds()})
}

func cors(origin string, next http.Handler) http.Handler {
	if origin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
