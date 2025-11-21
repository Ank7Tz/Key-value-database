package datastore

import (
	"fmt"
	"os"
	"testing"
)

// Test 1: Create and close LevelDB store
func TestNewLevelDBStore(t *testing.T) {
	path := "./test-db-1"
	defer os.RemoveAll(path) // Clean up after test

	store, err := NewLevelDBStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	if store == nil {
		t.Fatal("Store is nil")
	}

	err = store.Close()
	if err != nil {
		t.Fatalf("Failed to close store: %v", err)
	}

	fmt.Println("✓ Test 1 PASSED: LevelDB store created and closed")
}

// Test 2: Basic Write and Read
func TestWriteAndRead(t *testing.T) {
	path := "./test-db-2"
	defer os.RemoveAll(path)

	store, err := NewLevelDBStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write a key
	key := "user:alice"
	value := []byte("Alice's data")

	err = store.Write(key, value)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read it back
	readValue, err := store.Read(key)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("Expected '%s', got '%s'", value, readValue)
	}

	fmt.Printf("  Wrote: %s → %s\n", key, value)
	fmt.Printf("  Read:  %s → %s\n", key, readValue)
	fmt.Println("✓ Test 2 PASSED: Write and read successful")
}

// Test 3: Delete operation
func TestDelete(t *testing.T) {
	path := "./test-db-3"
	defer os.RemoveAll(path)

	store, err := NewLevelDBStore(path)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	key := "user:bob"
	value := []byte("Bob's data")

	// Write
	store.Write(key, value)

	// Verify it exists
	_, err = store.Read(key)
	if err != nil {
		t.Fatal("Key should exist before delete")
	}

	// Delete
	err = store.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify it's gone
	_, err = store.Read(key)
	if err == nil {
		t.Fatal("Key should not exist after delete")
	}

	fmt.Println("✓ Test 3 PASSED: Delete operation successful")
}

// Test 4: Persistence - data survives restart
func TestPersistence(t *testing.T) {
	path := "./test-db-4"
	defer os.RemoveAll(path)

	key := "persistent:key"
	value := []byte("persistent value")

	// Create store, write, and close
	{
		store, err := NewLevelDBStore(path)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}

		err = store.Write(key, value)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}

		store.Close()
	}

	// Reopen store and verify data is still there
	{
		store, err := NewLevelDBStore(path)
		if err != nil {
			t.Fatalf("Failed to reopen store: %v", err)
		}
		defer store.Close()

		readValue, err := store.Read(key)
		if err != nil {
			t.Fatalf("Failed to read after reopen: %v", err)
		}

		if string(readValue) != string(value) {
			t.Errorf("Data mismatch after reopen: expected '%s', got '%s'", value, readValue)
		}
	}

	fmt.Println("✓ Test 4 PASSED: Data persists across restarts")
}

// Benchmark: Write performance
func BenchmarkWrite(b *testing.B) {
	path := "./bench-db"
	defer os.RemoveAll(path)

	store, _ := NewLevelDBStore(path)
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i)
		store.Write(key, []byte("value"))
	}
}

// Benchmark: Read performance
func BenchmarkRead(b *testing.B) {
	path := "./bench-db-read"
	defer os.RemoveAll(path)

	store, _ := NewLevelDBStore(path)
	defer store.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		store.Write(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i%1000)
		store.Read(key)
	}
}
