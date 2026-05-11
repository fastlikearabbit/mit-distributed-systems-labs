package kvraft

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/tester1"
)

func commitTxnOrFatal(t *testing.T, ts *Test, txn *Txn) rpc.TxnReply {
	t.Helper()

	reply, err := txn.Commit()
	ts.Op()
	if err != rpc.OK {
		t.Fatalf("Txn err %v", err)
	}
	return reply
}

func TestTxnBasic4B(t *testing.T) {
	ts := MakeTest(t, "4B transactions basic", 0, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnBasic4B", ts.nservers)
	defer ts.Cleanup()

	ck := ts.MakeRawClerk()
	defer ts.DeleteRawClerk(ck)

	reply := commitTxnOrFatal(t, ts, ck.Txn().
		If(
			rpc.CmpVersion("x", rpc.TxnCompareEqual, 0),
			rpc.CmpVersion("y", rpc.TxnCompareEqual, 0),
		).
		Then(
			rpc.OpPut("x", "1"),
			rpc.OpPut("y", "1"),
			rpc.OpGet("x"),
			rpc.OpGet("y"),
		).
		Else(rpc.OpGet("x")))

	if !reply.Succeeded {
		t.Fatalf("initial transaction failed unexpectedly: %+v", reply)
	}
	if len(reply.Responses) != 4 {
		t.Fatalf("expected 4 transaction responses, got %d", len(reply.Responses))
	}
	if reply.Responses[2].Value != "1" || reply.Responses[2].Version != 1 || reply.Responses[2].Err != rpc.OK {
		t.Fatalf("Get(x) within transaction = %+v, want value 1 version 1", reply.Responses[2])
	}
	if reply.Responses[3].Value != "1" || reply.Responses[3].Version != 1 || reply.Responses[3].Err != rpc.OK {
		t.Fatalf("Get(y) within transaction = %+v, want value 1 version 1", reply.Responses[3])
	}

	reply = commitTxnOrFatal(t, ts, ck.Txn().
		If(rpc.CmpVersion("x", rpc.TxnCompareEqual, 0)).
		Then(rpc.OpPut("x", "bad"), rpc.OpPut("z", "bad")).
		Else(rpc.OpGet("x")))

	if reply.Succeeded {
		t.Fatalf("stale compare transaction succeeded unexpectedly: %+v", reply)
	}
	if len(reply.Responses) != 1 || reply.Responses[0].Value != "1" || reply.Responses[0].Version != 1 {
		t.Fatalf("else branch response = %+v, want current x value/version", reply.Responses)
	}

	ts.CheckGet(ck, "x", "1", 1)
	ts.CheckGet(ck, "y", "1", 1)
	if _, _, err := ck.Get("z"); err != rpc.ErrNoKey {
		t.Fatalf("failed transaction wrote z; Get(z) err %v", err)
	}
}

func TestTxnConcurrentAtomic4B(t *testing.T) {
	const (
		nclients  = 5
		perClient = 20
	)

	ts := MakeTest(t, "4B transactions concurrent atomic", nclients, 3, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnConcurrentAtomic4B", ts.nservers)
	defer ts.Cleanup()

	ck := ts.MakeRawClerk()
	defer ts.DeleteRawClerk(ck)

	if err := ck.Put("a", "0", 0); err != rpc.OK {
		t.Fatalf("initial Put(a) err %v", err)
	}
	ts.Op()
	if err := ck.Put("b", "0", 0); err != rpc.OK {
		t.Fatalf("initial Put(b) err %v", err)
	}
	ts.Op()

	var wg sync.WaitGroup
	errCh := make(chan string, nclients)
	for cli := 0; cli < nclients; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()

			ck := ts.MakeRawClerk()
			defer ts.DeleteRawClerk(ck)

			for done := 0; done < perClient; {
				read, err := ck.Txn().Then(rpc.OpGet("a"), rpc.OpGet("b")).Commit()
				ts.Op()
				if err != rpc.OK {
					errCh <- fmt.Sprintf("client %d read txn err %v", cli, err)
					return
				}
				if !read.Succeeded || len(read.Responses) != 2 {
					errCh <- fmt.Sprintf("client %d read txn reply %+v", cli, read)
					return
				}
				if read.Responses[0].Err != rpc.OK || read.Responses[1].Err != rpc.OK {
					errCh <- fmt.Sprintf("client %d read missing key responses %+v", cli, read.Responses)
					return
				}
				if read.Responses[0].Value != read.Responses[1].Value {
					errCh <- fmt.Sprintf("client %d saw split pair %+v", cli, read.Responses)
					return
				}

				oldValue := read.Responses[0].Value
				oldN, convErr := strconv.Atoi(oldValue)
				if convErr != nil {
					errCh <- fmt.Sprintf("client %d could not parse %q: %v", cli, oldValue, convErr)
					return
				}
				newValue := strconv.Itoa(oldN + 1)

				write, err := ck.Txn().
					If(
						rpc.CmpValue("a", rpc.TxnCompareEqual, oldValue),
						rpc.CmpValue("b", rpc.TxnCompareEqual, oldValue),
					).
					Then(
						rpc.OpPut("a", newValue),
						rpc.OpPut("b", newValue),
						rpc.OpGet("a"),
						rpc.OpGet("b"),
					).
					Commit()
				ts.Op()
				if err != rpc.OK {
					errCh <- fmt.Sprintf("client %d write txn err %v", cli, err)
					return
				}
				if !write.Succeeded {
					continue
				}
				if len(write.Responses) != 4 ||
					write.Responses[2].Value != newValue ||
					write.Responses[3].Value != newValue {
					errCh <- fmt.Sprintf("client %d write txn reply %+v", cli, write)
					return
				}
				done++
			}
		}(cli)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	final := strconv.Itoa(nclients * perClient)
	finalVersion := rpc.Tversion(nclients*perClient + 1)
	ts.CheckGet(ck, "a", final, finalVersion)
	ts.CheckGet(ck, "b", final, finalVersion)
}
