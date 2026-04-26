import socket
import threading
import time
import random
import json
import os
import sys

from message import (
    send_message, recv_message, make_register, make_client_response, make_request_vote_response,
    MSG_REGISTER_ACK, MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE,
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE,
    MSG_CLIENT_REQUEST,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, CLUSTER_SIZE, NODE_IDS,
    HEARTBEAT_INTERVAL, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX,
    SNAPSHOT_THRESHOLD, DATA_DIR,
)


# === Raft Roles ===
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


class RaftNode:
    """
    A single Raft node that connects to the network.

    Architecture:
        - Connects to the central network via TCP
        - Receives messages via _receive_loop
        - Election timeouts and heartbeats driven by _timer_loop
        - You implement the Raft logic in the TODO methods below

    State (all initialised for you):
        Raft persistent state:
            current_term, voted_for, log

        Raft volatile state:
            commit_index, last_applied, role, leader_id

        Leader-only state:
            next_index, match_index, votes_received

        Application state:
            kv_store
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.sock = None
        self.lock = threading.Lock()
        self.sequence_number = 0

        # === Raft Persistent State ===
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # === Raft Volatile State ===
        self.commit_index = 0
        self.last_applied = 0
        self.role = FOLLOWER
        self.leader_id = None

        # === Leader-Only State ===
        self.next_index = {}
        self.match_index = {}
        self.votes_received = set()

        # === Election Timing ===
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()

        # === Application State Machine ===
        self.kv_store = {}

        # === Load Persisted State ===
        self.load_state()

    def _random_election_timeout(self):
        """Generate a random election timeout."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    # CONNECTION & MESSAGE HANDLING (do not modify)

    def start(self):
        """Connect to the network and start the node."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(0.1)

        # Register with the network
        send_message(self.sock, make_register(self.node_id, "node"))
        ack = recv_message(self.sock)
        if not ack or ack.get("type") != MSG_REGISTER_ACK:
            print(f"[{self.node_id}] Registration failed")
            return

        print(f"[{self.node_id}] Registered with network as {self.role}")

        # Start background threads
        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._timer_loop, daemon=True).start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.node_id}] Shutting down")
            self.sock.close()

    def _receive_loop(self):
        """Continuously receive and dispatch messages from the network."""
        while True:
            try:
                msg = recv_message(self.sock)
                if msg is None:
                    print(f"[{self.node_id}] Connection to network lost")
                    break
                self._dispatch(msg)
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                print(f"[{self.node_id}] Connection error")
                break

    def _dispatch(self, msg):
        """Route incoming messages to the appropriate handler."""
        msg_type = msg.get("type")

        if msg_type == MSG_APPEND_ENTRIES:
            self.handle_append_entries(msg)
        elif msg_type == MSG_APPEND_ENTRIES_RESPONSE:
            self.handle_append_entries_response(msg)
        elif msg_type == MSG_REQUEST_VOTE:
            self.handle_request_vote(msg)
        elif msg_type == MSG_REQUEST_VOTE_RESPONSE:
            self.handle_request_vote_response(msg)
        elif msg_type == MSG_CLIENT_REQUEST:
            self.handle_client_request(msg)
        # Ignore unknown message types silently

    def _send(self, msg):
        """
        Send a message through the network.

        All messages go through the central network, which routes them
        based on the 'dst' field.
        """
        try:
            send_message(self.sock, msg)
        except (BrokenPipeError, OSError):
            pass

    # TIMER LOOP (do not modify)

    def _timer_loop(self):
        """
        Periodic timer that drives election timeouts and heartbeats.

        Runs every 100ms. The lock is held before calling start_election()
        or send_heartbeats().
        """
        while True:
            time.sleep(0.1)

            with self.lock:
                now = time.time()
                elapsed = now - self.last_heartbeat_time

                if self.role == LEADER:
                    if elapsed >= HEARTBEAT_INTERVAL:
                        self.send_heartbeats()
                        self.last_heartbeat_time = now
                else:
                    if elapsed >= self.election_timeout:
                        self.start_election()
                        self.last_heartbeat_time = now
                        self.election_timeout = self._random_election_timeout()

    # RAFT LEADER ELECTION

    def start_election(self):
        # TODO: Implement election start
        self.current_term += 1
        self.role = CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}  # Vote for self
        self.leader_id = None

        print(f"[{self.node_id}] Starting election for term {self.current_term}")

        last_index = self._get_last_log_index()
        last_term = self._get_last_log_term()   

        for node_id in NODE_IDS:
            # so request isn't sent to self
            if node_id == self.node_id:
                continue
            msg = {
                "type": MSG_REQUEST_VOTE,
                "src": self.node_id,
                "dst": node_id,
                "term": self.current_term,
                "last_log_index": last_index,
                "last_log_term": last_term,
            }
            #for debugging purposes, print the message being sent
            print(
                f"[{self.node_id}] Sending REQUEST_VOTE to {node_id} "
                # last index and last term should currently always be 0 as log appends haven't been implimented
                f"term={self.current_term} last_index={last_index} last_term={last_term}"
            )
            #send message to other nodes through the network router
            self._send(msg)
        #pass

    def handle_request_vote(self, msg):

        if msg['term'] > self.current_term:
            self.current_term = msg['term']
            self.role = FOLLOWER
            vote_granted = False # Reject vote
            self.voted_for = None # Reset vote for the new term

            my_last_term = self._get_last_log_term
            my_last_index = self._get_last_log_index

            candidate_last_term = msg['last_log_term']
            candidate_last_index = msg['last_log_index']

            # Checking for higher term, then if equal term checking for longer index
            log_check = (candidate_last_term > my_last_term) or \
            (candidate_last_term == my_last_term) and (candidate_last_index >= my_last_index)


        # If had not voted yet or already voted for a specfic candidate 
        if msg['term'] == self.current_term and (self.voted_for is None or self.voted_for == msg['src']) and \
        log_check:
            # Grant the vote
            vote_granted = True
            self.voted_for = msg['src']
            print(f"[{self.node_id}] Voting FOR {msg['src']} in term {self.current_term} ")
        
            # Success response
            response = make_request_vote_response (
                self.node_id,
                msg['src'],
                self.current_term,
                success = vote_granted
            )
        else:
            # Reject the vote
            print(f"[{self.node_id}] Rejecting vote for {msg['src']} in term {self.current_term}")

            response = make_request_vote_response (
                self.node_id,
                msg['src'],
                self.current_term,
                success = False
            )

        self._send(response)


    def handle_request_vote_response(self, msg):
        # To care about the votes, make sure are candiate
        if self.role != CANDIDATE:
            return
        
        # Only count the vote if it's for the current election year
        if  msg['term'] == self.current_term and msg['vote_granted']:
            self.votes_received.add(msg['src'])

            print(f"[{self.node_id}] Vote recieved from {msg['src']}. Total votes: {len(self.votes_received)}")
        
        # Calculate majority
        total_nodes = len(NODE_IDS)
        majority_votes = (total_nodes // 2) + 1

        # Finding out the newly appointed leader
        if len(self.votes_received) >= majority_votes:
            self.role = LEADER
            print(f"[{self.node_id}] I am now the LEADER for term {self.current_term}")

        print(f"[{self.node_id}] Received vote from {msg['src']} granted={msg['vote_granted']}")

    # RAFT LOG REPLICATION

    def send_heartbeats(self):
        # TODO: Implement heartbeats / log replication

        for peer_id in NODE_IDS:
            if peer_id != self.node_id:
                msg = {
                "type": MSG_APPEND_ENTRIES,
                "src": self.node_id,
                "dst": peer_id,
                "term": self.current_term,
                "entries": [],
                "timestamp": time.time(),
                "sequence": self.sequence_number
                }
                self._send(msg)
            self.sequence_number += 1
        

    def handle_append_entries(self, msg):
        # TODO: Implement AppendEntries handling
        # Check that the previous log entry matches (same index and term) before accepting new entries. If the check fails, respond with `success=False`. If it passes, append the new entries to the log and update `commit_index` if the leader's commit index is higher (from breif))
        with self.lock:
            if msg['term'] < self.current_term:
                # set the responce to false if term is outdated
                response = {
                    "type": MSG_APPEND_ENTRIES_RESPONSE,
                    "src": self.node_id,    
                    "dst": msg['src'],
                    "term": self.current_term,
                    "success": False
                }
            else:
                # handle append entries
                self.leader_id = msg['src']
                response = {
                    "type": MSG_APPEND_ENTRIES_RESPONSE,
                    "src": self.node_id,
                    "dst": msg['src'],
                    "term": self.current_term,
                    "success": True
                }
        self._send(response)

    def handle_append_entries_response(self, msg):
        # TODO: Implement AppendEntries response handling
        # part 1 doesnt have anything for this method put the next oats will
        # could add a print the response for debugging purposes?
        # part 3 will need to handle log replication and commit index updates here
        self.send_heartbeats()

        if msg['term'] > self.current_term:
            self.role = FOLLOWER
        pass

    # CLIENT REQUEST HANDLING

    def handle_client_request(self, msg):
        # TODO: Implement client request handling
        #For now, operations do not go through the Raft log, it will be done in part 3 (following the brief)
        #For now, just respond to the client for debugging purposes 
        if self.role != LEADER:
            # Redirect client to the current leader if not the leader
            response = make_client_response(
                self.node_id,
                msg['src'],
                success=False,
                error="Not the leader",
                leader_hint=self.leader_id
            )
            self._send(response)
            return
        else:
            # do a get and put operation on the kv store for debugging purposes
            operation = msg['operation']
            key = msg['key']    
            value = msg['value']
            if operation == "PUT":
                self.kv_store[key] = value
                response = make_client_response(
                    self.node_id,
                    msg['src'],
                    request_id=msg.get("request_id"),
                    success = True,
                )
            elif operation == "GET":
                value = self.kv_store.get(key)
                response = make_client_response(
                    self.node_id,
                    msg['src'],
                    request_id=msg.get("request_id"),
                    success=True,
                    value=value
                )
            self._send(response)


    # STATE MACHINE APPLICATION

    def apply_committed(self):
        # TODO: Implement state machine application
        pass

    # CHECKPOINTING / SNAPSHOTTING (Part 3)

    def take_snapshot(self):
        # TODO (Part 3): Implement snapshotting
        pass

    def load_snapshot(self, snapshot_data):
        # TODO (Part 3): Implement snapshot loading
        pass

    # STATE PERSISTENCE (Part 3)

    def save_state(self):
        # TODO (Part 3): Implement state persistence
        pass

    def load_state(self):
        # TODO (Part 3): Implement state loading
        pass

    # HELPER METHODS 

    def _get_last_log_index(self):
        """Return the index of the last log entry, or 0 if log is empty."""
        return self.log[-1]["index"] if self.log else 0

    def _get_last_log_term(self):
        """Return the term of the last log entry, or 0 if log is empty."""
        return self.log[-1]["term"] if self.log else 0

    def _get_log_term(self, index):
        """Return the term of the log entry at the given index, or 0."""
        for entry in self.log:
            if entry["index"] == index:
                return entry["term"]
        return 0

    def _get_log_entry(self, index):
        """Return the log entry at the given index, or None."""
        for entry in self.log:
            if entry["index"] == index:
                return entry
        return None

    def _get_log_slice(self, from_index):
        """Return all log entries from from_index onward (inclusive)."""
        return [e for e in self.log if e["index"] >= from_index]

    def _step_down(self, new_term):
        """Revert to follower state with a new term."""
        self.current_term = new_term
        self.role = FOLLOWER
        self.voted_for = None
        self.leader_id = None

# ENTRY POINT

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python node.py <node-id>")
        print(f"  Valid node IDs: {NODE_IDS}")
        sys.exit(1)

    node_id = sys.argv[1]
    if node_id not in NODE_IDS:
        print(f"Invalid node ID '{node_id}'. Must be one of: {NODE_IDS}")
        sys.exit(1)

    node = RaftNode(node_id)
    node.start()
