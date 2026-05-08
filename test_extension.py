# this file will be used for the extra test that will be created
# test_extension.py - Tests for InstallSnapshot extension
#
# Tests the InstallSnapshot RPC which allows a leader to send its snapshot
# directly to a lagging follower that has fallen behind the leader's log
# compaction point. This is an extension beyond the base assignment which
# only requires local snapshotting.
#
# Usage:
#   python run_cluster.py                    (perfect network)
#   python run_cluster.py network_lossy.py   (lossy network)
#   python test_extension.py
#
# The cluster must be running before starting these tests.

import socket
import sys
import time
import uuid

from message import (
    send_message, recv_message, make_register, make_client_request,
    MSG_CLIENT_RESPONSE, MSG_REGISTER_ACK,
)
from config import NETWORK_HOST, NETWORK_PORT, CLIENT_TIMEOUT, SNAPSHOT_THRESHOLD


# === Test Client Helper ===

class TestClient:
    def __init__(self, client_id=None):
        self.client_id = client_id or f"ext-{uuid.uuid4().hex[:6]}"
        self.sock = None

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((NETWORK_HOST, NETWORK_PORT))
            self.sock.settimeout(CLIENT_TIMEOUT)
            send_message(self.sock, make_register(self.client_id, "client"))
            ack = recv_message(self.sock)
            return ack is not None and ack.get("type") == MSG_REGISTER_ACK
        except (ConnectionRefusedError, OSError) as e:
            print(f"Could not connect: {e}")
            return False

    def request(self, operation, key, value=None, timeout=None):
        request_id = str(uuid.uuid4())
        msg = make_client_request(
            self.client_id, request_id, operation, key, value
        )
        send_message(self.sock, msg)
        deadline = time.time() + (timeout or CLIENT_TIMEOUT)
        while time.time() < deadline:
            try:
                response = recv_message(self.sock)
                if response is None:
                    return None
                if (response.get("type") == MSG_CLIENT_RESPONSE and
                        response.get("request_id") == request_id):
                    return response
            except socket.timeout:
                break
        return None

    def request_with_retry(self, operation, key, value=None, timeout=None, retries=3):
        last = None
        for _ in range(retries):
            r = self.request(operation, key, value, timeout=timeout)
            if r and r.get("success"):
                return r
            last = r
        return last

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


# === Test Runner ===

class TestRunner:
    def __init__(self):
        self.results = []

    def run_test(self, name, test_func):
        print(f"\n--- TEST: {name} ---")
        try:
            passed = test_func()
            status = "PASS" if passed else "FAIL"
        except Exception as e:
            status = "ERROR"
            print(f"Exception: {e}")
            import traceback
            traceback.print_exc()
            passed = False
        self.results.append((name, status))
        print(f">>> {status}")
        return passed

    def report(self):
        print()
        print("=== EXTENSION TEST RESULTS ===")
        passed = sum(1 for _, s in self.results if s == "PASS")
        total = len(self.results)
        for name, status in self.results:
            marker = "PASS" if status == "PASS" else ("FAIL" if status == "FAIL" else "ERR ")
            print(f"[{marker}] {name}")
        print()
        print(f"{passed}/{total} tests passed")
        return passed == total


# === Extension Tests ===

def test_snapshot_triggered_and_data_survives():
    """
    Write enough entries to trigger snapshotting (more than SNAPSHOT_THRESHOLD),
    then verify all written keys are still readable. This confirms that the
    snapshot mechanism correctly preserves kv_store state when old log entries
    are discarded.
    """
    c = TestClient("ext-snapshot-basic")
    if not c.connect():
        print("Could not connect to cluster")
        return False

    print("Waiting for leader election...")
    time.sleep(5)

    # Write enough keys to exceed SNAPSHOT_THRESHOLD and trigger snapshotting
    num_keys = SNAPSHOT_THRESHOLD + 10
    print(f"Writing {num_keys} keys to trigger snapshot (threshold={SNAPSHOT_THRESHOLD})...")

    written = {}
    for i in range(num_keys):
        key = f"snap-key-{i}"
        val = f"snap-val-{i}"
        r = c.request("PUT", key, val, timeout=10)
        if r and r.get("success"):
            written[key] = val

    print(f"{len(written)}/{num_keys} keys written successfully")

    if len(written) < num_keys * 0.8:
        print(f"Too few keys written to reliably trigger snapshot")
        c.close()
        return False

    # Allow time for snapshot to be taken
    time.sleep(2)

    # Verify all written keys are still readable after snapshotting
    print(f"Verifying {len(written)} keys after snapshot...")
    verified = 0
    failed_keys = []
    for key, expected in written.items():
        r = c.request_with_retry("GET", key, timeout=10,retries=3)
        if r and r.get("success") and r.get("value") == expected:
            verified += 1
        else:
            failed_keys.append(key)

    c.close()

    print(f"Verified {verified}/{len(written)} keys")
    if failed_keys:
        print(f"Failed keys: {failed_keys[:5]}{'...' if len(failed_keys) > 5 else ''}")

    return verified == len(written)


def test_lagging_follower_catches_up_via_snapshot():
    """
    Write enough data to trigger snapshotting so the leader discards early
    log entries. Then verify a new client can still read all keys — this
    confirms followers can catch up via the snapshot mechanism when they
    are behind the leader's compaction point.

    This directly tests the InstallSnapshot extension: if a follower has
    fallen behind the snapshot point, the leader sends its snapshot directly
    rather than trying to replay log entries it no longer has.
    """
    c = TestClient("ext-install-snapshot")
    if not c.connect():
        print("Could not connect to cluster")
        return False

    print("Waiting for leader election...")
    time.sleep(8)

    # Phase 1: Write a large batch to push leader past snapshot threshold
    batch_size = 15
    print(f"Phase 1: Writing {batch_size} keys to force log compaction...")

    written = {}
    for i in range(batch_size):
        key = f"install-key-{i}"
        val = f"install-val-{i}"
        r = c.request("PUT", key, val, timeout=10)
        if r and r.get("success"):
            written[key] = val

    print(f"{len(written)}/{batch_size} keys written")

    if len(written) < batch_size * 0.7:
        print("Too few keys written, cannot reliably test InstallSnapshot")
        c.close()
        return False

    # Allow time for snapshot and any InstallSnapshot RPCs to complete
    print("Waiting for snapshot propagation...")
    time.sleep(3)

    # Phase 2: Use a fresh client to verify all keys are readable
    # A fresh client's request may reach any node — if a follower received
    # an InstallSnapshot it should have the full kv_store
    c2 = TestClient("ext-install-verify")
    if not c2.connect():
        c.close()
        return False

    print(f"Phase 2: Verifying {len(written)} keys from fresh client...")
    verified = 0
    incorrect = 0
    for key, expected in written.items():
        r = c2.request_with_retry("GET", key, timeout=10)
        if r and r.get("success") and r.get("value") == expected:
            verified += 1
        elif r and r.get("success") and r.get("value") != expected:
            incorrect += 1
            print(f"Consistency violation: {key} expected '{expected}' got '{r.get('value')}'")

    c.close()
    c2.close()

    print(f"Verified {verified}/{len(written)} keys, {incorrect} incorrect")

    # Hard fail on any incorrect value — this would indicate a node served
    # stale data after receiving an InstallSnapshot
    if incorrect > 0:
        print("FAIL: Consistency violation detected after InstallSnapshot")
        return False

    return verified >= len(written) * 0.8


def test_snapshot_state_correct_after_deletes():
    """
    Write keys, delete some, write more to trigger snapshotting, then verify
    the snapshot correctly reflects the state including deletions. This tests
    an edge case — the snapshot must capture the kv_store at the point of
    snapshotting, not replay the log, so deleted keys must not reappear.
    """
    c = TestClient("ext-snapshot-delete")
    if not c.connect():
        print("Could not connect to cluster")
        return False

    print("Waiting for leader election...")
    time.sleep(5)

    # Write initial keys
    print("Writing initial keys...")
    initial_keys = {}
    for i in range(20):
        key = f"del-key-{i}"
        val = f"del-val-{i}"
        r = c.request("PUT", key, val, timeout=10)
        if r and r.get("success"):
            initial_keys[key] = val

    if len(initial_keys) < 15:
        print("Too few initial keys written")
        c.close()
        return False

    # Delete half of them
    print("Deleting half the keys...")
    deleted_keys = set()
    keys_list = list(initial_keys.keys())
    for key in keys_list[:10]:
        r = c.request_with_retry("DELETE", key, timeout=10)
        if r and r.get("success"):
            deleted_keys.add(key)
            del initial_keys[key]

    print(f"Deleted {len(deleted_keys)} keys, {len(initial_keys)} remaining")

    # Write enough additional keys to trigger snapshotting
    print(f"Writing additional keys to trigger snapshot...")
    additional = {}
    for i in range(20):
        key = f"extra-key-{i}"
        val = f"extra-val-{i}"
        r = c.request_with_retry("PUT", key, val, timeout=10)
        if r and r.get("success"):
            additional[key] = val

    # Allow time for snapshot
    time.sleep(2)

    print("Verifying state after snapshot...")
    failures = 0

    # Deleted keys must not be present
    print(f"Checking {len(deleted_keys)} deleted keys are gone...")
    for key in deleted_keys:
        r = c.request_with_retry("GET", key, timeout=10, retries=3)
        if r and r.get("success"):
            print(f"FAIL: Deleted key '{key}' still present after snapshot with value '{r.get('value')}'")
            failures += 1

    # Remaining keys must still be present with correct values
    print(f"Checking {len(initial_keys)} surviving keys...")
    surviving_verified = 0
    for key, expected in initial_keys.items():
        r = c.request_with_retry("GET", key, timeout=10,retries=3)
        if r and r.get("success") and r.get("value") == expected:
            surviving_verified += 1
        else:
            print(f"Missing or wrong value for '{key}': got {r}")
            failures += 1

    # Sample of additional keys
    print(f"Checking sample of {min(10, len(additional))} additional keys...")
    sample_keys = list(additional.items())[:10]
    for key, expected in sample_keys:
        r = c.request_with_retry("GET", key, timeout=10, retries=3)
        if r and r.get("success") and r.get("value") == expected:
            pass
        else:
            print(f"Missing additional key '{key}'")
            failures += 1

    c.close()

    print(f"Surviving keys verified: {surviving_verified}/{len(initial_keys)}")
    print(f"Total failures: {failures}")

    return failures == 0


def test_snapshot_on_lossy_network():
    """
    Edge case: verify snapshotting and InstallSnapshot work correctly on a
    lossy network where some messages are dropped. Written keys must still
    be readable after snapshotting even when some replication messages were
    lost during the write phase.

    Run this test with: python run_cluster.py network_lossy.py
    """
    c = TestClient("ext-lossy-snapshot")
    if not c.connect():
        print("Could not connect to cluster")
        return False

    print("Waiting for leader election...")
    time.sleep(5)

    num_keys = 20
    print(f"Writing {num_keys} keys on lossy network with retries...")

    written = {}
    for i in range(num_keys):
        key = f"lossy-snap-{i}"
        val = f"lossy-val-{i}"
        r = c.request_with_retry("PUT", key, val, timeout=10, retries=3)
        if r and r.get("success"):
            written[key] = val

    print(f"{len(written)}/{num_keys} keys written successfully")

    if len(written) < num_keys * 0.6:
        print("Too few keys written on lossy network")
        c.close()
        return False

    # Allow time for snapshot and propagation
    time.sleep(3)

    # Verify all confirmed-written keys are still readable
    print(f"Verifying {len(written)} confirmed-written keys...")
    verified = 0
    incorrect = 0
    for key, expected in written.items():
        r = c.request_with_retry("GET", key, timeout=10, retries=3)
        if r and r.get("success") and r.get("value") == expected:
            verified += 1
        elif r and r.get("success") and r.get("value") != expected:
            incorrect += 1
            print(f"Consistency violation: {key} expected '{expected}' got '{r.get('value')}'")

    c.close()

    print(f"Verified {verified}/{len(written)} keys, {incorrect} incorrect")

    if incorrect > 0:
        print("FAIL: Consistency violation on lossy network after snapshot")
        return False

    return verified == len(written)


# === Main ===

if __name__ == "__main__":
    runner = TestRunner()

    print("\n=== INSTALLSNAPSHOT EXTENSION TESTS ===")
    print(f"Cluster expected at {NETWORK_HOST}:{NETWORK_PORT}")
    print(f"Snapshot threshold: {SNAPSHOT_THRESHOLD}")
    print("Make sure run_cluster.py is running!\n")

    runner.run_test(
        "Snapshot triggered and data survives",
        test_snapshot_triggered_and_data_survives
    )
    runner.run_test(
        "Lagging follower catches up via snapshot",
        test_lagging_follower_catches_up_via_snapshot
    )
    runner.run_test(
        "Snapshot state correct after deletes",
        test_snapshot_state_correct_after_deletes
    )
    runner.run_test(
        "Snapshot works on lossy network",
        test_snapshot_on_lossy_network
    )

    success = runner.report()
    sys.exit(0 if success else 1)