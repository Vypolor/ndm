package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const defaultPort = 8080

type Handler struct {
	queue *MemoryQueue
}

func NewHandler(queue *MemoryQueue) *Handler {
	return &Handler{
		queue: queue,
	}
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queue")
	msg := r.URL.Query().Get("v")
	if msg == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := h.queue.Push(name, msg); err != nil {
		slog.Error(fmt.Sprintf("failed to push message to queue: %v", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queue")
	ctx := r.Context()
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		sec, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "invalid timeout: "+err.Error(), http.StatusBadRequest)
			return
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(sec)*time.Second)
		defer cancel()
	}

	msg, err := h.queue.Pop(ctx, name)
	if err != nil {
		switch {
		case errors.Is(err, ErrQueueNotFound), errors.Is(err, context.DeadlineExceeded):
			w.WriteHeader(http.StatusNotFound)
		case errors.Is(err, context.Canceled):
			http.Error(w, "client disconnected", http.StatusRequestTimeout)
		default:
			slog.Error(fmt.Sprintf("failed to push message to queue: %v", err))
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	if _, err = w.Write([]byte(msg)); err != nil {
		slog.Error(fmt.Sprintf("failed to write response: %v", err))
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /{queue}", h.Put)
	mux.HandleFunc("GET /{queue}", h.Get)
}

func main() {
	port := flag.Int("port", defaultPort, "server port")
	flag.Parse()

	mux := http.NewServeMux()
	NewHandler(NewMemoryQueue()).Register(mux)

	addr := fmt.Sprintf(":%d", *port)
	slog.Info(fmt.Sprintf("listening on %s", addr))
	log.Fatal(http.ListenAndServe(addr, mux))
}
