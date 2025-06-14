package utils

import (
	"reflect"
	"testing"
	"time"
)

func TestBatcher_BatchSize(t *testing.T) {
	in := make(chan int)
	out := Batcher(in, 3, 100*time.Millisecond)
	go func() {
		defer close(in)
		in <- 1
		in <- 2
		in <- 3
		in <- 4
		in <- 5
		// chiude il channel
	}()
	var batches [][]int
	for batch := range out {
		batches = append(batches, batch)
	}
	expected := [][]int{{1, 2, 3}, {4, 5}}
	if !reflect.DeepEqual(batches, expected) {
		t.Errorf("batching per size: got %v, want %v", batches, expected)
	}
}

func TestBatcher_BatchTimeout(t *testing.T) {
	in := make(chan int)
	out := Batcher(in, 10, 100*time.Millisecond)
	go func() {
		defer close(in)
		in <- 1
		in <- 2
		// aspetta per forzare il timeout
		time.Sleep(150 * time.Millisecond)
		in <- 3
	}()
	var batches [][]int
	for batch := range out {
		batches = append(batches, batch)
	}
	if len(batches) < 2 {
		t.Fatalf("expected almeno 2 batch, got %d", len(batches))
	}
	if !reflect.DeepEqual(batches[0], []int{1, 2}) {
		t.Errorf("primo batch: got %v, want [1 2]", batches[0])
	}
	if !reflect.DeepEqual(batches[1], []int{3}) {
		t.Errorf("secondo batch: got %v, want [3]", batches[1])
	}
}
