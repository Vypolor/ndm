package main

import (
	"context"
	"errors"
	"sync"
)

var ErrQueueNotFound = errors.New("queue not found")

type queue struct {
	msgs    []string
	waiters []chan string
}

type MemoryQueue struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		queues: make(map[string]*queue),
	}
}

func (s *MemoryQueue) Push(name, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.getOrCreate(name)

	for len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]

		select {
		case w <- msg:
			return nil
		default:
		}
	}

	q.msgs = append(q.msgs, msg)
	return nil
}

func (s *MemoryQueue) Pop(ctx context.Context, name string) (string, error) {
	s.mu.Lock()
	q, ok := s.queues[name]
	if !ok {
		s.mu.Unlock()
		return "", ErrQueueNotFound
	}
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		s.mu.Unlock()
		return msg, nil
	}
	w := make(chan string, 1)
	q.waiters = append(q.waiters, w)
	s.mu.Unlock()

	select {
	case msg := <-w:
		return msg, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *MemoryQueue) getOrCreate(name string) *queue {
	q, ok := s.queues[name]
	if !ok {
		q = &queue{}
		s.queues[name] = q
	}
	return q
}
