package kvstore

import "testing"

func TestPutGet(t *testing.T) {
	s := New()

	s.Put("name", "Sonu")

	got, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if got != "Sonu" {
		t.Fatalf("expected 'Sonu', got %q", got)
	}
}

func TestGetMissing(t *testing.T) {
	s := New()

	if _, ok := s.Get("name"); ok {
		t.Fatal("expected missing key to report not-found")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := New()

	s.Put("age", "21")
	s.Put("age", "22")

	got, ok := s.Get("age")
	if !ok {
		t.Fatal("expected key 'age' to exist")
	}
	if got != "22" {
		t.Fatalf("expected overwritten value '22', got %q", got)
	}
	if s.Len() != 1 {
		t.Fatalf("expected len 1, got %d", s.Len())
	}
}

func TestDelete(t *testing.T) {
	s := New()

	s.Put("name", "Sonu")

	if !s.Delete("name") {
		t.Fatal("expected Delete to report key present")
	}
	if _, ok := s.Get("name"); ok {
		t.Fatal("expected key to be gone after Delete")
	}
	if s.Delete("name") {
		t.Fatal("expected second Delete to report key absent")
	}
}

func TestIsolation(t *testing.T) {
	s := New()

	s.Put("a", "1")
	s.Put("b", "2")

	if s.Len() != 2 {
		t.Fatalf("expected len 2, got %d", s.Len())
	}
	s.Delete("a")
	if s.Len() != 1 {
		t.Fatalf("expected len 1 after delete, got %d", s.Len())
	}
	if _, ok := s.Get("b"); !ok {
		t.Fatal("expected 'b' to survive deletion of 'a'")
	}
}