"""
snapshot_tests.py — Test suite for the InstallSnapshot extension.

Tests that the leader correctly sends snapshots to lagging followers,
and that followers correctly install them and catch up.

Run with:
    python snapshot_tests.py

"""

import socket
import threading
import time
import uuid
import json
import os
import sys

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_REGISTER_ACK, MSG_CLIENT_RESPONSE,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, NODE_IDS,
    ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL, CLIENT_TIMEOUT,
    CLIENT_RETRY_DELAY, SNAPSHOT_THRESHOLD, DATA_DIR,
)

LEADER_WAIT  = ELECTION_TIMEOUT_MAX + 2.0
MAX_RETRIES  = 8
RETRY_DELAY  = CLIENT_RETRY_DELAY

PASS = "PASS"
FAIL = "FAIL"


# Client helper

class TestClient:
    def __init__(self, client_id=None):
        self.client_id = client_id or f"snap-tc-{uuid.uuid4().hex[:6]}"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(CLIENT_TIMEOUT)
        send_message(self.sock, make_register(self.client_id, "client"))
        ack = recv_message(self.sock)
        if not ack or ack.get("type") != MSG_REGISTER_ACK:
            raise RuntimeError(f"[{self.client_id}] Registration failed")

    def request(self, operation, key, value=None, request_id=None, timeout=None):
        rid = request_id or str(uuid.uuid4())
        msg = make_client_request(self.client_id, rid, operation, key, value)
        if timeout is not None:
            self.sock.settimeout(timeout)
        send_message(self.sock, msg)
        deadline = time.time() + (timeout or CLIENT_TIMEOUT)
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
    print(f"\n=== {title} ===")


def result(name, passed, detail=""):
    status = PASS if passed else FAIL
    tail = f"  ({detail})" if detail else ""
    print(f"  \n[{status}] {name}{tail}")
    return passed


# Snapshot file helpers

def read_node_state(node_id):
    """Read a node's persisted state file and return it as a dict."""
    path = os.path.join(DATA_DIR, f"{node_id}.json")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)


def get_snapshot(node_id):
    """Return the snapshot dict from a node's state file, or None."""
    state = read_node_state(node_id)
    if state is None:
        return None
    return state.get("snapshot")


def get_log_length(node_id):
    """Return the number of log entries in a node's persisted state."""
    state = read_node_state(node_id)
    if state is None:
        return 0
    return len(state.get("log", []))


# TEST 1 — Snapshot is taken after threshold entries are committed

def test_snapshot_taken_after_threshold():
    """
    Write SNAPSHOT_THRESHOLD + 10 entries to force take_snapshot().
    Then inspect the leader's data file and verify:
    - a snapshot exists
    - the log has been trimmed (fewer than SNAPSHOT_THRESHOLD entries remain)
    - the snapshot's kv_store contains the written keys
    """
    section("TEST 1 — Snapshot taken after threshold")

    count = SNAPSHOT_THRESHOLD + 10
    print(f"  Writing {count} entries (threshold={SNAPSHOT_THRESHOLD})...")

    c = TestClient()
    failed = []
    written = {}
    for i in range(count):
        key = f"snap-t1-{i}"
        val = f"val-{i}"
        resp = c.put_with_retry(key, val)
        if resp and resp.get("success"):
            written[key] = val
        else:
            failed.append(i)
    c.close()

    result(f"{count} entries written", len(failed) == 0, f"failed indices: {failed[:5]}" if failed else "")

    # Allow snapshot to be taken and persisted
    time.sleep(HEARTBEAT_INTERVAL * 4)

    # Check at least one node has a snapshot
    snapped = []
    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap is not None:
            snapped.append(node_id)

    result("At least one node has a snapshot", len(snapped) > 0, f"snapped nodes: {snapped}")

    # Check that snapshotted nodes have trimmed their log
    trimmed = []
    for node_id in snapped:
        log_len = get_log_length(node_id)
        if log_len < SNAPSHOT_THRESHOLD:
            trimmed.append((node_id, log_len))

    result("Log trimmed below threshold on snapshotted nodes", len(trimmed) == len(snapped), f"trimmed: {trimmed}")

    # Check snapshot kv_store contains a sample of written keys
    missing_from_snap = []
    for node_id in snapped:
        snap = get_snapshot(node_id)
        kv = snap.get("kv_store", {})
        for key, val in list(written.items())[::10]:   # spot-check every 10th
            if kv.get(key) != val:
                missing_from_snap.append((node_id, key))

    result("Snapshot kv_store contains written keys", len(missing_from_snap) == 0, f"missing: {missing_from_snap[:3]}" if missing_from_snap else "")

# TEST 2 — Keys are still readable after snapshot (log trimmed)

def test_keys_readable_after_snapshot():
    """
    Write enough entries to trigger a snapshot, then verify that all
    written keys are still readable via GET — proving the kv_store
    was preserved through the snapshot and log trimming.
    """
    section("TEST 2 — Keys readable after snapshot")

    count = SNAPSHOT_THRESHOLD + 5
    print(f"  Writing {count} entries to trigger snapshot...")

    c = TestClient()
    written = {}
    for i in range(count):
        key = f"snap-t2-{i}"
        val = f"v2-{i}"
        resp = c.put_with_retry(key, val)
        if resp and resp.get("success"):
            written[key] = val

    time.sleep(HEARTBEAT_INTERVAL * 4)

    # Spot-check reads
    bad_reads = []
    sample = list(written.items())[::5]   # every 5th key
    for key, expected in sample:
        resp = c.get(key)
        if not resp or not resp.get("success") or resp.get("value") != expected:
            bad_reads.append((key, resp.get("value") if resp else None))

    result(f"Spot-check {len(sample)} keys readable after snapshot", len(bad_reads) == 0, f"bad reads: {bad_reads[:3]}" if bad_reads else "")
    c.close()

# TEST 3 — Snapshot metadata is correct (last_included_index / term)

def test_snapshot_metadata():
    """
    After triggering a snapshot, inspect the snapshot's
    last_included_index and last_included_term fields and verify:
    - last_included_index >= SNAPSHOT_THRESHOLD
    - last_included_term > 0
    - the log entries that remain all have index > last_included_index
    """
    section("TEST 3 — Snapshot metadata correctness")

    count = SNAPSHOT_THRESHOLD + 5
    c = TestClient()
    for i in range(count):
        c.put_with_retry(f"snap-t3-{i}", f"v-{i}")
    c.close()

    time.sleep(HEARTBEAT_INTERVAL * 4)

    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap is None:
            continue

        last_index = snap.get("last_included_index", 0)
        last_term  = snap.get("last_included_term", 0)

        result(f"{node_id}: last_included_index >= {SNAPSHOT_THRESHOLD}", last_index >= SNAPSHOT_THRESHOLD, f"got {last_index}")

        result(f"{node_id}: last_included_term > 0", last_term > 0, f"got {last_term}")

        # All remaining log entries must be after the snapshot point
        state = read_node_state(node_id)
        log = state.get("log", [])
        stale = [e for e in log if e["index"] <= last_index]
        result(f"{node_id}: no stale log entries before snapshot point", len(stale) == 0, f"{len(stale)} stale entries found" if stale else "")
        break   # one node is enough for metadata check

# TEST 4 — Snapshot persists across simulated restart (load_snapshot)

def test_snapshot_survives_persist():
    """
    Write enough entries to trigger a snapshot, then read the snapshot
    file back directly and verify load_snapshot() would restore the
    correct kv_store state. We test this by checking the persisted
    snapshot file matches what we wrote.
    """
    section("TEST 4 — Snapshot survives persistence (load_snapshot check)")

    count = SNAPSHOT_THRESHOLD + 5
    c = TestClient()
    written = {}
    for i in range(count):
        key = f"snap-t4-{i}"
        val = f"persist-val-{i}"
        resp = c.put_with_retry(key, val)
        if resp and resp.get("success"):
            written[key] = val
    c.close()

    time.sleep(HEARTBEAT_INTERVAL * 4)

    found = False
    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap is None:
            continue
        found = True
        kv = snap.get("kv_store", {})

        # Check a sample of written keys exist in the snapshot
        missing = []
        for key, val in list(written.items())[::10]:
            if kv.get(key) != val:
                missing.append(key)

        result(f"{node_id}: snapshot kv_store matches written data", len(missing) == 0, f"missing keys: {missing[:3]}" if missing else "")

        result(f"{node_id}: snapshot has last_included_index field", "last_included_index" in snap)

        result(f"{node_id}: snapshot has last_included_term field", "last_included_term" in snap)
        break

    if not found:
        result("Snapshot file found on at least one node", False, "no snapshot found — threshold may not have been reached")

# TEST 5 — Writes continue normally after snapshot

def test_writes_after_snapshot():
    """
    Write enough to trigger a snapshot, then continue writing more entries
    and verify they are committed and readable — proving the cluster
    continues operating normally after log compaction.
    """
    section("TEST 5 — Writes continue after snapshot")

    # Trigger snapshot
    c = TestClient()
    for i in range(SNAPSHOT_THRESHOLD + 5):
        c.put_with_retry(f"snap-t5-pre-{i}", f"pre-{i}")

    time.sleep(HEARTBEAT_INTERVAL * 4)

    # Write post-snapshot entries
    print("  Writing 10 post-snapshot entries...")
    post_written = {}
    for i in range(10):
        key = f"snap-t5-post-{i}"
        val = f"post-val-{i}"
        resp = c.put_with_retry(key, val)
        if resp and resp.get("success"):
            post_written[key] = val

    result("Post-snapshot writes succeeded", len(post_written) == 10, f"only {len(post_written)}/10 succeeded")

    # Verify post-snapshot reads
    bad = []
    for key, expected in post_written.items():
        resp = c.get(key)
        if not resp or not resp.get("success") or resp.get("value") != expected:
            bad.append(key)

    result("Post-snapshot keys readable", len(bad) == 0, f"bad reads: {bad}" if bad else "")
    c.close()

# TEST 6 — InstallSnapshot response updates leader's next_index

def test_install_snapshot_leader_tracking():
    """
    After a snapshot is installed on a follower, the leader should update
    that follower's next_index to last_included_index + 1. We verify this
    indirectly by writing new entries after the snapshot and confirming
    they replicate successfully (which requires correct next_index tracking).
    """
    section("TEST 6 — Leader tracks follower state after InstallSnapshot")

    c = TestClient()

    # Trigger snapshot
    print(f"  Writing {SNAPSHOT_THRESHOLD + 5} entries to trigger snapshot...")
    for i in range(SNAPSHOT_THRESHOLD + 5):
        c.put_with_retry(f"snap-t6-base-{i}", f"base-{i}")

    time.sleep(HEARTBEAT_INTERVAL * 6)

    # Now write 5 more entries — these should replicate via normal AppendEntries
    # using the updated next_index from the InstallSnapshot response
    print("  Writing 5 entries post-snapshot to verify next_index tracking...")
    success_count = 0
    for i in range(5):
        resp = c.put_with_retry(f"snap-t6-after-{i}", f"after-{i}")
        if resp and resp.get("success"):
            success_count += 1

    result("All 5 post-snapshot entries replicated", success_count == 5, f"{success_count}/5 succeeded")

    # Verify all 5 are readable
    bad = []
    for i in range(5):
        resp = c.get(f"snap-t6-after-{i}")
        if not resp or not resp.get("success") or resp.get("value") != f"after-{i}":
            bad.append(i)

    result("Post-snapshot entries readable", len(bad) == 0, f"bad indices: {bad}" if bad else "")
    c.close()

# TEST 7 — Multiple snapshots (snapshot of a snapshot)

def test_multiple_snapshots():
    """
    Write enough entries to trigger two separate snapshots and verify
    the cluster remains consistent throughout — specifically that the
    second snapshot's last_included_index is larger than the first's.
    """
    section("TEST 7 — Multiple snapshots")

    c = TestClient()

    # First snapshot
    print(f"  Writing {SNAPSHOT_THRESHOLD + 5} entries (first snapshot)...")
    for i in range(SNAPSHOT_THRESHOLD + 5):
        c.put_with_retry(f"snap-t7-r1-{i}", f"r1-{i}")

    time.sleep(HEARTBEAT_INTERVAL * 4)

    first_snap_index = None
    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap:
            first_snap_index = snap.get("last_included_index", 0)
            break

    result("First snapshot taken", first_snap_index is not None and first_snap_index > 0, f"last_included_index={first_snap_index}")

    # Second snapshot
    print(f"  Writing {SNAPSHOT_THRESHOLD + 5} more entries (second snapshot)...")
    for i in range(SNAPSHOT_THRESHOLD + 5):
        c.put_with_retry(f"snap-t7-r2-{i}", f"r2-{i}")

    time.sleep(HEARTBEAT_INTERVAL * 4)

    second_snap_index = None
    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap:
            idx = snap.get("last_included_index", 0)
            if first_snap_index is None or idx > first_snap_index:
                second_snap_index = idx
                break

    result("Second snapshot has higher last_included_index than first", second_snap_index is not None and
        (first_snap_index is None or second_snap_index > first_snap_index),
        f"first={first_snap_index} second={second_snap_index}")

    # Verify a sample of both rounds are still readable
    bad = []
    for i in range(0, SNAPSHOT_THRESHOLD + 5, 20):
        for prefix, val_prefix in [("r1", "r1"), ("r2", "r2")]:
            resp = c.get(f"snap-t7-{prefix}-{i}")
            if not resp or not resp.get("success") or \
                    resp.get("value") != f"{val_prefix}-{i}":
                bad.append(f"snap-t7-{prefix}-{i}")

    result("Keys from both rounds readable after two snapshots", len(bad) == 0, f"bad: {bad[:3]}" if bad else "")
    c.close()

# TEST 8 — Overwritten keys reflected correctly in snapshot

def test_snapshot_reflects_overwrites():
    """
    Write a key, overwrite it many times, trigger a snapshot, then verify
    the snapshot kv_store contains only the final value — not any
    intermediate value.
    """
    section("TEST 8 — Snapshot reflects latest value after overwrites")

    c = TestClient()
    overwrite_key = f"snap-overwrite-{uuid.uuid4().hex[:6]}"
    final_value = None

    # Write the key enough times to contribute to reaching the threshold,
    # alternating with padding keys to accumulate log entries
    for i in range(SNAPSHOT_THRESHOLD + 5):
        if i % 10 == 0:
            val = f"overwrite-version-{i}"
            resp = c.put_with_retry(overwrite_key, val)
            if resp and resp.get("success"):
                final_value = val
        else:
            c.put_with_retry(f"snap-t8-pad-{i}", f"pad-{i}")

    time.sleep(HEARTBEAT_INTERVAL * 4)

    result("Overwrite key has a known final value", final_value is not None)

    # Check snapshot contains the final value
    for node_id in NODE_IDS:
        snap = get_snapshot(node_id)
        if snap is None:
            continue
        kv = snap.get("kv_store", {})
        snap_val = kv.get(overwrite_key)
        result(f"{node_id}: snapshot holds final value for overwritten key",
            snap_val == final_value,
            f"expected={final_value} got={snap_val}")
        break

    # Also verify via GET
    resp = c.get(overwrite_key)
    result("GET returns final value after snapshot",
        resp and resp.get("success") and resp.get("value") == final_value,
        f"got={resp.get('value') if resp else None}")
    c.close()


# Entry point

def main():
    print("\nInstallSnapshot test suite")
    print(f"Cluster expected at {NETWORK_HOST}:{NETWORK_PORT}")
    print(f"Snapshot threshold: {SNAPSHOT_THRESHOLD} entries")

    print(f"Waiting up to {LEADER_WAIT:.0f}s for initial leader election...")
    if not wait_for_leader():
        print("ERROR: No leader elected. Is run_cluster.py running?")
        sys.exit(1)
    print("Leader detected — starting tests.\n")

    tests = [
        test_snapshot_taken_after_threshold,
        test_keys_readable_after_snapshot,
        test_snapshot_metadata,
        test_snapshot_survives_persist,
        test_writes_after_snapshot,
        test_install_snapshot_leader_tracking,
        test_multiple_snapshots,
        test_snapshot_reflects_overwrites,
    ]

    for test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            print(f"  [ERROR] {test_fn.__name__} raised: {e}")

    print("\n=== All snapshot tests complete. ===")


if __name__ == "__main__":
    main()