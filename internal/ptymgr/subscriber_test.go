package ptymgr

import "testing"

func TestSubscriberTryPushAndClose(t *testing.T) {
	sub := newSubscriber()
	if !sub.TryPush(outFrame{kind: frameBinary, data: []byte("x")}) {
		t.Fatal("TryPush returned false for empty queue")
	}
	got := <-sub.out
	if got.kind != frameBinary || string(got.data) != "x" {
		t.Fatalf("frame = %#v", got)
	}

	sub.Close()
	sub.Close()
	if sub.TryPush(outFrame{kind: frameText, data: []byte("{}")}) {
		t.Fatal("TryPush after Close returned true")
	}
	if _, ok := <-sub.out; ok {
		t.Fatal("out channel should be closed")
	}
}

func TestSubscriberTryPushFull(t *testing.T) {
	sub := newSubscriber()
	for i := 0; i < subscriberQueueSize; i++ {
		if !sub.TryPush(outFrame{kind: frameBinary, data: []byte("x")}) {
			t.Fatalf("TryPush #%d returned false before full", i)
		}
	}
	if sub.TryPush(outFrame{kind: frameBinary, data: []byte("y")}) {
		t.Fatal("TryPush returned true for full queue")
	}
}
