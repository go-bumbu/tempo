package tempo

import "testing"

func TestLimiter_NoLimits(t *testing.T) {
	l := newLimiter()
	rel1, ok := l.tryAcquire("a", 0, "")
	if !ok {
		t.Fatal("acquire with no limits should succeed")
	}
	rel2, ok := l.tryAcquire("a", 0, "")
	if !ok {
		t.Fatal("second acquire with no name limit should succeed")
	}
	rel1()
	rel2()
}

func TestLimiter_NameLimit(t *testing.T) {
	l := newLimiter()
	rel1, ok := l.tryAcquire("a", 1, "")
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	if _, ok := l.tryAcquire("a", 1, ""); ok {
		t.Fatal("second acquire past the name limit should fail")
	}
	rel1()
	rel2, ok := l.tryAcquire("a", 1, "")
	if !ok {
		t.Fatal("acquire after release should succeed")
	}
	rel2()
}

func TestLimiter_GroupExcludesAcrossNames(t *testing.T) {
	l := newLimiter()
	rel1, ok := l.tryAcquire("scan", 0, "files")
	if !ok {
		t.Fatal("first group acquire should succeed")
	}
	if _, ok := l.tryAcquire("reindex", 0, "files"); ok {
		t.Fatal("a second, differently-named task in the same group should fail")
	}
	rel1()
	rel2, ok := l.tryAcquire("reindex", 0, "files")
	if !ok {
		t.Fatal("group acquire after release should succeed")
	}
	rel2()
}

// A failed acquire must not leave a partial reservation. Here the group is full,
// so the acquire must fail without reserving y's name slot.
func TestLimiter_AllOrNothing_NoNameLeakWhenGroupFull(t *testing.T) {
	l := newLimiter()
	relX, ok := l.tryAcquire("x", 0, "g")
	if !ok {
		t.Fatal("holding the group should succeed")
	}
	if _, ok := l.tryAcquire("y", 1, "g"); ok {
		t.Fatal("acquire should fail while the group is full")
	}
	relX() // free the group
	// If y's name slot had leaked to 1 on the failed acquire, this would fail.
	relY, ok := l.tryAcquire("y", 1, "g")
	if !ok {
		t.Fatal("y's name slot leaked on a failed acquire")
	}
	relY()
}

// The mirror case: the name is at capacity, so the acquire must fail without
// reserving the group slot.
func TestLimiter_AllOrNothing_NoGroupLeakWhenNameFull(t *testing.T) {
	l := newLimiter()
	relZ, ok := l.tryAcquire("z", 1, "")
	if !ok {
		t.Fatal("holding name z should succeed")
	}
	if _, ok := l.tryAcquire("z", 1, "g"); ok {
		t.Fatal("acquire should fail while the name is at its limit")
	}
	// If group g had leaked on the failed acquire, this would fail.
	relW, ok := l.tryAcquire("w", 0, "g")
	if !ok {
		t.Fatal("group g leaked on a failed acquire")
	}
	relW()
	relZ()
}

func TestLimiter_ReleaseIsIdempotent(t *testing.T) {
	l := newLimiter()
	// Two holders of "a" at capacity 2.
	h1, ok := l.tryAcquire("a", 2, "")
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	h2, ok := l.tryAcquire("a", 2, "")
	if !ok {
		t.Fatal("second acquire should succeed")
	}
	// At capacity: a third must fail.
	if _, ok := l.tryAcquire("a", 2, ""); ok {
		t.Fatal("third acquire past capacity should fail")
	}
	// Release h1 twice. A non-idempotent release would also free h2's slot.
	h1()
	h1()
	// Exactly one slot should now be free: one acquire succeeds, the next fails.
	r1, ok := l.tryAcquire("a", 2, "")
	if !ok {
		t.Fatal("one slot should be free after releasing a single holder")
	}
	if _, ok := l.tryAcquire("a", 2, ""); ok {
		t.Fatal("a double-release must not free a second slot (release not idempotent)")
	}
	r1()
	h2()
}
