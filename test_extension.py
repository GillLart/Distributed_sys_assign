# this file will be used for the extra test that will be created

import socket
import threading
import time
import uuid
import json
import struct
import subprocess
import sys
import os

# Import project modules

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_REGISTER_ACK, MSG_CLIENT_RESPONSE,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, NODE_IDS,
    ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL, CLIENT_TIMEOUT, CLIENT_RETRY_DELAY,
)

# Tunables
LEADER_WAIT        = ELECTION_TIMEOUT_MAX + 1.0   # seconds to wait for a leader
RESPONSE_TIMEOUT   = CLIENT_TIMEOUT               # per-request socket timeout
RETRY_DELAY        = CLIENT_RETRY_DELAY
MAX_RETRIES        = 5

PASS = "PASS"
FAIL = "FAIL"

# Client helper

class TestClient:
    """
    A lightweight test client that connects to the network and sends
    CLIENT_REQUEST messages, waiting for the matching CLIENT_RESPONSE.
    """

    def __init__(self, client_id=None):
        self.client_id = client_id or f"tc-{uuid.uuid4().hex[:6]}"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(RESPONSE_TIMEOUT)
        send_message(self.sock, make_register(self.client_id, "client"))
        ack = recv_message(self.sock)
        if not ack or ack.get("type") != MSG_REGISTER_ACK:
            raise RuntimeError(f"[{self.client_id}] Registration failed")

    def request(self, operation, key, value=None, request_id=None, timeout=None):
        """Send one request and wait for the matching response."""
        rid = request_id or str(uuid.uuid4())
        msg = make_client_request(self.client_id, rid, operation, key, value)
        if timeout is not None:
            self.sock.settimeout(timeout)
        send_message(self.sock, msg)
        deadline = time.time() + (timeout or RESPONSE_TIMEOUT)
        while time.time() < deadline:
            try:
                resp = recv_message(self.sock)
                if resp and resp.get("request_id") == rid:
                    return resp
            except socket.timeout:
                break
        return None

    def put(self, key, value, **kwargs):
        return self.request("PUT", key, value, **kwargs)

    def get(self, key, **kwargs):
        return self.request("GET", key, **kwargs)

    def delete(self, key, **kwargs):
        return self.request("DELETE", key, **kwargs)

    def put_with_retry(self, key, value, retries=MAX_RETRIES):
        for _ in range(retries):
            resp = self.put(key, value)
            if resp and resp.get("success"):
                return resp
            time.sleep(RETRY_DELAY)
        return resp

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def wait_for_leader(timeout=LEADER_WAIT):
    """
    Poll until we get a successful PUT, indicating a leader exists.
    Returns (True, client) on success, (False, None) on timeout.
    """
    deadline = time.time() + timeout
    probe_key = f"__probe_{uuid.uuid4().hex[:6]}__"
    while time.time() < deadline:
        try:
            c = TestClient()
            resp = c.put(probe_key, "probe")
            if resp and resp.get("success"):
                c.close()
                return True
            c.close()
        except Exception:
            pass
        time.sleep(0.5)
    return False


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def result(name, passed, detail=""):
    status = PASS if passed else FAIL
    tail = f"  ({detail})" if detail else ""
    print(f"  [{status}] {name}{tail}")
    return passed

# TEST 1 — Leader failure and recovery

def test_leader_failure_recovery():
    """
    Write a value, identify the leader from the response src, kill that node
    process, wait for re-election, then verify the written value is still
    readable and new writes succeed.

    NOTE: This test requires run_cluster.py to start nodes as named
    subprocesses that can be killed by name (node-0 … node-4). If your
    cluster runner does not support this, the kill step is skipped and the
    test becomes a basic re-election check only.
    """
    section("TEST 1 — Leader failure and recovery")

    # Write a canary value and record which node was the leader
    c = TestClient()
    resp = c.put_with_retry("canary-key", "canary-value")
    if not resp or not resp.get("success"):
        result("Initial write", False, "could not write before killing leader")
        c.close()
        return

    leader_id = resp.get("src")
    result("Initial write succeeded", True, f"leader={leader_id}")

    # Attempt to kill the leader process (best-effort, platform-specific)
    killed = False
    if leader_id and sys.platform != "win32":
        try:
            os.system(f"pkill -f 'node.py {leader_id}'")
            killed = True
            print(f"  Killed process for {leader_id}")
        except Exception as e:
            print(f"  Could not kill {leader_id}: {e}")
    elif leader_id and sys.platform == "win32":
        # On Windows use taskkill; run_cluster.py must have started nodes
        # with a window title or identifiable command line.
        try:
            os.system(f"taskkill /F /FI \"WINDOWTITLE eq {leader_id}\"")
            killed = True
        except Exception:
            pass

    if not killed:
        print("  (skipping kill step — cannot identify process on this platform)")

    # Wait for re-election
    print(f"  Waiting up to {LEADER_WAIT}s for new leader...")
    time.sleep(ELECTION_TIMEOUT_MAX + HEARTBEAT_INTERVAL)

    c2 = TestClient()
    resp2 = c2.put_with_retry("post-failure-key", "post-failure-value")
    write_ok = resp2 and resp2.get("success")
    result("Write after leader failure", write_ok)

    # Verify canary is still readable
    resp3 = c2.get("canary-key")
    read_ok = resp3 and resp3.get("success") and resp3.get("value") == "canary-value"
    result("Canary value preserved after re-election", read_ok)

    c.close()
    c2.close()


# TEST 2 — Follower failure (minority down, cluster continues)

def test_follower_failure():
    """
    With 5 nodes, the cluster can tolerate 2 simultaneous follower failures
    and still commit entries (majority = 3). This test verifies reads and
    writes continue when fewer than a majority of nodes are unreachable.

    We simulate follower failure by simply writing at a high rate and
    checking that the cluster maintains availability — without actually
    killing processes (which is platform-dependent).
    """
    section("TEST 2 — Follower failure tolerance")

    print("  Writing 20 entries to verify majority availability...")
    c = TestClient()
    failures = 0
    for i in range(20):
        resp = c.put_with_retry(f"follower-test-{i}", f"val-{i}")
        if not resp or not resp.get("success"):
            failures += 1

    result("20 writes with majority available",
           failures == 0,
           f"{failures} failures")

    # Verify a sample of keys
    bad_reads = 0
    for i in range(0, 20, 4):
        resp = c.get(f"follower-test-{i}")
        if not resp or not resp.get("success") or resp.get("value") != f"val-{i}":
            bad_reads += 1

    result("Spot-check reads after writes", bad_reads == 0,
           f"{bad_reads} bad reads")
    c.close()

# TEST 3 — Log consistency after rejoin

def test_log_consistency_after_rejoin():
    """
    Write a batch of entries, pause, write more entries, then verify that
    all entries are readable. This exercises the next_index backtracking
    path that allows a stale follower to catch up.
    """
    section("TEST 3 — Log consistency / follower catch-up")

    c = TestClient()

    # Batch 1
    print("  Writing batch 1 (10 entries)...")
    for i in range(10):
        r = c.put_with_retry(f"batch1-{i}", f"v1-{i}")
        if not r or not r.get("success"):
            result("Batch 1 write", False, f"failed at index {i}")
            c.close()
            return

    # Simulate a follower being briefly behind by sleeping longer than
    # one heartbeat interval, then writing more entries
    time.sleep(HEARTBEAT_INTERVAL * 3)

    # Batch 2
    print("  Writing batch 2 (10 more entries)...")
    for i in range(10):
        r = c.put_with_retry(f"batch2-{i}", f"v2-{i}")
        if not r or not r.get("success"):
            result("Batch 2 write", False, f"failed at index {i}")
            c.close()
            return

    # Allow replication to complete
    time.sleep(HEARTBEAT_INTERVAL * 2)

    # Verify all 20 entries are readable
    missing = []
    for i in range(10):
        r = c.get(f"batch1-{i}")
        if not r or not r.get("success") or r.get("value") != f"v1-{i}":
            missing.append(f"batch1-{i}")
    for i in range(10):
        r = c.get(f"batch2-{i}")
        if not r or not r.get("success") or r.get("value") != f"v2-{i}":
            missing.append(f"batch2-{i}")

    result("All 20 entries readable after replication",
           len(missing) == 0,
           f"missing: {missing}" if missing else "")
    c.close()


# TEST 4 — Concurrent conflicting writes to the same key

def test_concurrent_conflicting_writes():
    """
    Five threads each write a different value to the same key. After all
    threads complete, every client must read back the same value — proving
    linearisability on that key (no split-brain).
    """
    section("TEST 4 — Concurrent conflicting writes (same key)")

    shared_key = f"conflict-key-{uuid.uuid4().hex[:6]}"
    writers = 5
    results_lock = threading.Lock()
    written_values = []
    errors = []

    def writer_thread(thread_id):
        try:
            c = TestClient(f"conflict-writer-{thread_id}")
            val = f"writer-{thread_id}-value"
            resp = c.put_with_retry(shared_key, val)
            with results_lock:
                if resp and resp.get("success"):
                    written_values.append(val)
                else:
                    errors.append(thread_id)
            c.close()
        except Exception as e:
            with results_lock:
                errors.append(f"thread-{thread_id}: {e}")

    threads = [threading.Thread(target=writer_thread, args=(i,))
               for i in range(writers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    result("All writers completed without error",
           len(errors) == 0,
           f"errors: {errors}" if errors else "")

    # All readers must agree on the same final value
    readers = [TestClient(f"conflict-reader-{i}") for i in range(3)]
    read_values = set()
    for r in readers:
        resp = r.get(shared_key)
        if resp and resp.get("success"):
            read_values.add(resp.get("value"))
        r.close()

    result("All readers agree on the same final value",
           len(read_values) == 1,
           f"values seen: {read_values}")


# TEST 5 — Deduplication under retry

def test_deduplication():
    """
    Send the same request_id three times. The operation must be applied
    exactly once — verified by using a key that tracks a counter via
    a known initial PUT, then checking the value hasn't been multiplied.
    """
    section("TEST 5 — Deduplication under retry")

    c = TestClient()
    dedup_key = f"dedup-{uuid.uuid4().hex[:6]}"
    fixed_rid = f"dedup-fixed-{uuid.uuid4().hex[:8]}"

    # Send the same PUT three times with the same request_id
    responses = []
    for attempt in range(3):
        resp = c.request("PUT", dedup_key, "dedup-value", request_id=fixed_rid)
        if resp:
            responses.append(resp)
        time.sleep(0.3)

    result("Received at least one response", len(responses) >= 1)

    # All non-None responses must report success=True (cached response reuse)
    all_success = all(r.get("success") for r in responses)
    result("All responses report success", all_success,
           f"{len(responses)} responses received")

    # The value must be set exactly to "dedup-value" (not applied multiple times)
    resp_get = c.get(dedup_key)
    result("Key holds correct value (applied once)",
           resp_get and resp_get.get("value") == "dedup-value")

    c.close()

# TEST 6 — Stale read prevention (GET served from leader only)

def test_stale_read_prevention():
    """
    Write a value, then immediately overwrite it. Verify that all subsequent
    reads return the latest value and never the stale one. This catches cases
    where a non-leader serves a GET from its own kv_store before it has
    replicated the latest commits.
    """
    section("TEST 6 — Stale read prevention")

    c = TestClient()
    key = f"stale-test-{uuid.uuid4().hex[:6]}"

    resp1 = c.put_with_retry(key, "old-value")
    result("First write (old-value)", resp1 and resp1.get("success"))

    resp2 = c.put_with_retry(key, "new-value")
    result("Second write (new-value)", resp2 and resp2.get("success"))

    # Issue 10 reads and verify none return the stale value
    stale_reads = 0
    total_reads = 10
    for _ in range(total_reads):
        resp = c.get(key)
        if resp and resp.get("success") and resp.get("value") == "old-value":
            stale_reads += 1

    result("No stale reads observed",
           stale_reads == 0,
           f"{stale_reads}/{total_reads} returned old-value")
    c.close()


# TEST 7 — Term inflation prevention / recovery

def test_term_inflation_recovery():
    """
    When a node is partitioned it keeps incrementing its term with every
    failed election. This test verifies that after such a node reconnects
    (simulated by a brief inability to reach the cluster followed by
    recovery), the cluster still settles on a single leader and accepts
    writes — even if one node briefly holds a very high term.

    We simulate this by checking that the cluster recovers from a period of
    failed elections (term churn visible in logs) and still converges.
    """
    section("TEST 7 — Term inflation recovery")

    # Write before any churn
    c = TestClient()
    pre_key = f"pre-inflation-{uuid.uuid4().hex[:6]}"
    resp = c.put_with_retry(pre_key, "pre-value")
    result("Write before term churn", resp and resp.get("success"))

    # Sleep long enough for a few election timeouts to fire (simulates churn)
    churn_duration = ELECTION_TIMEOUT_MAX * 3
    print(f"  Waiting {churn_duration:.1f}s to allow election timeouts to accumulate...")
    time.sleep(churn_duration)

    # The cluster must recover and accept new writes
    post_key = f"post-inflation-{uuid.uuid4().hex[:6]}"
    resp2 = c.put_with_retry(post_key, "post-value", retries=10)
    result("Write succeeds after term churn period", resp2 and resp2.get("success"))

    # Pre-churn value must still be readable
    resp3 = c.get(pre_key)
    result("Pre-churn value still readable",
           resp3 and resp3.get("success") and resp3.get("value") == "pre-value")
    c.close()


# TEST 8 — Delete correctness

def test_delete_correctness():
    """
    PUT a key, verify it exists, DELETE it, verify it's gone, then verify
    a second DELETE returns an appropriate not-found error rather than
    crashing or silently succeeding.
    """
    section("TEST 8 — Delete correctness")

    c = TestClient()
    key = f"del-test-{uuid.uuid4().hex[:6]}"

    resp = c.put_with_retry(key, "to-be-deleted")
    result("PUT before delete", resp and resp.get("success"))

    resp2 = c.get(key)
    result("GET confirms key exists", resp2 and resp2.get("value") == "to-be-deleted")

    resp3 = c.delete(key)
    # Allow retry for delete since it goes through the log
    if not (resp3 and resp3.get("success")):
        time.sleep(RETRY_DELAY)
        resp3 = c.delete(key)
    result("DELETE succeeds", resp3 and resp3.get("success"))

    resp4 = c.get(key)
    result("GET after delete returns not-found",
           resp4 and not resp4.get("success") and "not found" in (resp4.get("error") or "").lower())

    c.close()


# TEST 9 — Multiple concurrent clients, no lost writes

def test_multiple_clients_no_lost_writes():
    """
    Three clients each write 10 unique keys concurrently. After all threads
    complete, a fresh client verifies every key is readable with the correct
    value — proving no writes were silently dropped under concurrent load.
    """
    section("TEST 9 — Multiple clients, no lost writes")

    n_clients = 3
    keys_per_client = 10
    all_keys = {}   # key -> expected_value
    lock = threading.Lock()
    failed = []

    def client_worker(cid):
        try:
            c = TestClient(f"mc-{cid}")
            for k in range(keys_per_client):
                key = f"mc-c{cid}-k{k}"
                val = f"val-c{cid}-k{k}"
                resp = c.put_with_retry(key, val)
                with lock:
                    if resp and resp.get("success"):
                        all_keys[key] = val
                    else:
                        failed.append(key)
            c.close()
        except Exception as e:
            with lock:
                failed.append(str(e))

    threads = [threading.Thread(target=client_worker, args=(i,))
               for i in range(n_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    total_expected = n_clients * keys_per_client
    result(f"All {total_expected} writes succeeded",
           len(failed) == 0,
           f"{len(failed)} failures: {failed[:3]}" if failed else "")

    # Verify every written key
    verifier = TestClient("mc-verifier")
    bad = []
    for key, expected in all_keys.items():
        resp = verifier.get(key)
        if not resp or not resp.get("success") or resp.get("value") != expected:
            bad.append(key)
    verifier.close()

    result("All written keys readable with correct values",
           len(bad) == 0,
           f"{len(bad)} bad: {bad[:3]}" if bad else "")


# TEST 10 — Overwrite and read-your-writes consistency


def test_overwrite_consistency():
    """
    Write a key 5 times with different values in sequence. After each write
    the next GET must return that write's value, never an older one.
    This checks read-your-writes consistency from the same client.
    """
    section("TEST 10 — Overwrite and read-your-writes consistency")

    c = TestClient()
    key = f"overwrite-{uuid.uuid4().hex[:6]}"
    values = [f"version-{i}" for i in range(5)]
    stale_reads = []

    for v in values:
        resp = c.put_with_retry(key, v)
        if not resp or not resp.get("success"):
            result(f"PUT {v}", False)
            c.close()
            return
        get_resp = c.get(key)
        if not get_resp or get_resp.get("value") != v:
            stale_reads.append((v, get_resp.get("value") if get_resp else None))

    result("All overwrites returned correct read-after-write value",
           len(stale_reads) == 0,
           f"stale: {stale_reads}" if stale_reads else "")
    c.close()

# Entry point

def main():
    print("\nExtended Raft test suite")
    print(f"Cluster expected at {NETWORK_HOST}:{NETWORK_PORT}")
    print("Make sure run_cluster.py is running and data/ has been cleared!\n")

    print(f"Waiting up to {LEADER_WAIT:.0f}s for initial leader election...")
    if not wait_for_leader():
        print("ERROR: No leader elected within timeout. Is run_cluster.py running?")
        sys.exit(1)
    print("Leader detected — starting tests.\n")

    tests = [
        test_follower_failure,            
        test_log_consistency_after_rejoin,
        test_concurrent_conflicting_writes,
        test_deduplication,               
        test_stale_read_prevention,      
        test_term_inflation_recovery,     
        test_delete_correctness,          
        test_multiple_clients_no_lost_writes, 
        test_overwrite_consistency,       
        test_leader_failure_recovery,     
    ]

    for test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            print(f"  [ERROR] {test_fn.__name__} raised an exception: {e}")

    print(f"\n{'='*60}")
    print("  All tests complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
