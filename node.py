import socket
import threading
import time
import random
import json
import os
import sys

from message import (
    make_append_entries, make_request_vote, send_message, recv_message, make_register, make_client_response, make_request_vote_response,
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

        msg = make_request_vote(
            self.node_id, 
            self.current_term, 
            last_index, 
            last_term)
        #for debugging purposes, print the message being sent
        print(
            f"[{self.node_id}] Sending REQUEST_VOTE to {self.current_term} "
            # last index and last term should currently always be 0 as log appends haven't been implimented
            f"term={self.current_term} last_index={last_index} last_term={last_term}"
        )
        #send message to other nodes through the network router
        self._send(msg)
        #pass

    def handle_request_vote(self, msg):
        #thie is the version that doesn't cause a error
        if msg['term'] > self.current_term:
            self._step_down(msg['term'])
            self.election_timeout = self._random_election_timeout()
            #vote_granted = False # Reject vote

        my_last_term = self._get_last_log_term()
        my_last_index = self._get_last_log_index()

        candidate_last_term = msg['last_log_term']
        candidate_last_index = msg['last_log_index']

        # Checking for higher term, then if equal term checking for longer index
        log_check = (candidate_last_term > my_last_term or (
            candidate_last_term == my_last_term and 
            candidate_last_index >= my_last_index
            )
        )

        # If had not voted yet or already voted for a specfic candidate 
        if msg['term'] == self.current_term and (self.voted_for is None or self.voted_for == msg['src']) and log_check:
            # Grant the vote
            self.voted_for = msg['src']
            self.save_state()
            print(f"[{self.node_id}] Voting FOR {msg['src']} in term {self.current_term} ")
        
            # Success response
            response = make_request_vote_response (
                self.node_id,
                msg['src'],
                self.current_term,
                success = True
            )
        else:
            # Reject the vote
            print(f"[{self.node_id}] Rejecting vote for {msg['src']} in term {self.current_term}")

            response = make_request_vote_response (
                self.node_id,
                msg['src'],
                self.current_term,
                False
            )

        self._send(response)


    def handle_request_vote_response(self, msg):
        if msg['term'] > self.current_term:
            self._step_down(msg['term'])
            self.election_timeout = self._random_election_timeout() 
            return
        # To care about the votes, make sure are candiate
        if self.role != CANDIDATE:
            return
        
        # Only count the vote if it's for the current election year
        if  msg['term'] == self.current_term and msg['success']:
            self.votes_received.add(msg['src'])

            print(f"[{self.node_id}] Vote recieved from {msg['src']}. Total votes: {len(self.votes_received)}")
        
        # Calculate majority
        total_nodes = len(NODE_IDS)
        majority_votes = (total_nodes // 2) + 1

        # Finding out the newly appointed leader
        if len(self.votes_received) >= majority_votes:
            self.role = LEADER
            self.leader_id = self.node_id
            self.last_heartbeat_time = time.time()
            # incrament the next index for each follower to be one more than the last log index 
            # (as the leader would try to send new entries starting from there)
            next_log_index = self._get_last_log_index() + 1
            for peer_id in NODE_IDS:
                if peer_id != self.node_id:
                    self.next_index[peer_id] = next_log_index
                    self.match_index[peer_id] = 0

            print(f"[{self.node_id}] I am now the LEADER for term {self.current_term}")

            self.send_heartbeats()  # Send initial heartbeats immediately upon election
            return
        print(f"[{self.node_id}] Received vote from {msg['src']} granted={msg['success']}")

    # RAFT LOG REPLICATION

    def send_heartbeats(self):
        # TODO: Implement heartbeats / log replication

        for peer_id in NODE_IDS:
            if peer_id == self.node_id:
                # Don't send heartbeats to self
                continue
            # Determine what to send
            next_index = self.next_index.get(peer_id, 1)
            # get the log entries from the next index onward (if any)
            entries_to_send = self.log[next_index-1:]
            
            # Determine the previous point
            prev_log_index = next_index - 1

            #  previoud log term
            if prev_log_index > 0:
                prev_log_term = self._get_log_term(prev_log_index)
            else:  
                # At the very beginning of the log
                prev_log_term = 0

            msg = {
            "type": MSG_APPEND_ENTRIES,
            "src": self.node_id,
            "dst": peer_id,
            "term": self.current_term,
            "entries": entries_to_send,
            "timestamp": time.time(),
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "leader_commit": self.commit_index
            }
            
            self._send(msg)
        

    def handle_append_entries(self, msg):
        # TODO: Implement AppendEntries handling
        # Reset election timeout when receiving a valid heartbeat. For now, you do not need to handle log entries or consistency checks (those are added in Part 2)
        # part 2: Check that the previous log entry matches (same index and term) before accepting new entries. If the check fails, respond with `success=False`. If it passes, append the new entries to the log and update `commit_index` if the leader's commit index is higher 
        with self.lock:
            if msg.get('term') < self.current_term:
                # set the response to false if term is outdated
                response = {
                    "type": MSG_APPEND_ENTRIES_RESPONSE,
                    "src": self.node_id,    
                    "dst": msg['src'],
                    "term": self.current_term,
                    "success": False
                }
                self._send(response)
                return
            # # Reset election timeout when valid heartbeat recived
            self.current_term = msg.get('term')
            self.role = FOLLOWER
            self.leader_id = msg['src']
            self.last_heartbeat_time = time.time() #
            # Reset election timeoutis randomised so that one node times out earlier and becomes a candidite first the next election. 
            self.election_timeout = self._random_election_timeout()

            prev_log_index = msg.get('prev_log_index',0)
            prev_log_term = msg.get('prev_log_term',0)
            entries = msg.get('entries', [])
            leader_commit = msg.get('leader_commit', 0)

            # check previous log entry matches
            if prev_log_index > 0:
                # also added check for if follower log is empty
                local_prev_entry = self._get_log_entry(prev_log_index)
                if local_prev_entry is None or local_prev_entry["term"] != prev_log_term:
                    # Previous log entry does not match, reject the AppendEntries
                    response = {
                        "type": MSG_APPEND_ENTRIES_RESPONSE,
                        "src": self.node_id,
                        "dst": msg['src'],
                        "term": self.current_term,
                        "success": False,
                        "match_index": self._get_last_log_index()
                    }
                    self._send(response)
                    return
            # Append new entries to the log
            for entry in entries: 
                existing = self._get_log_entry(entry['index'])
                if existing is not None and existing['term'] != entry['term']:
                        # Conflict detected, delete the existing entry and all that follow it
                        self.log = [e for e in self.log if e['index'] < entry['index']]
                        
                if self._get_log_entry(entry['index']) is None:
                    self.log.append(entry)
                    self.save_state()

            # Update commit index if leader's commit index is higher
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._get_last_log_index())
                # Apply any newly committed entries to the state machine
                self.apply_committed() 
                
            response = {
                "type": MSG_APPEND_ENTRIES_RESPONSE,
                "src": self.node_id,
                "dst": msg['src'],
                "term": self.current_term,
                "success": True,
                "match_index": self._get_last_log_index()
            }
        self._send(response)

    def handle_append_entries_response(self, msg):
        # TODO: Implement AppendEntries response handling
        if msg['term'] > self.current_term:
            self._step_down(msg['term'])
            self.election_timeout= self._random_election_timeout()
            return

        if self.role != LEADER:
            return
        follower_id = msg['src']
        # added a bit of backtracking to handle failed append. simple not fully implemeted
        if not msg.get('success', False):
            self.next_index[follower_id] = max(1, self.next_index.get(follower_id, 1) - 1)
            return
        
        #sucessful then followers match up to the index
        follower_match_index = msg.get('match_index', 0)
        self.match_index[follower_id] = follower_match_index
        self.next_index[follower_id] = follower_match_index + 1

        # Check if there are any new entries that can be committed
        majority = (len(NODE_IDS) // 2) + 1

        for index in range(self.commit_index + 1, self._get_last_log_index() + 1):
            entry = self._get_log_entry(index)
            if entry is None:
                continue
            #only commit entries from current term
            if entry['term'] != self.current_term:
                continue
            # this should be the leader's own log, so it counts as a match
            count = 1  

            for peer_id in NODE_IDS:
                if peer_id == self.node_id:
                    continue
                if self.match_index.get(peer_id, 0) >= index:
                    count += 1

            if count >= majority:
                self.commit_index = index
        self.apply_committed()

    # CLIENT REQUEST HANDLING

    def handle_client_request(self, msg):
        # TODO: Implement client request handling
        if not hasattr(self, 'client_response_cache'):
            # can't add req cache in init so putting it here (using it to store the response for each request id)
            self.client_response_cache = {}

        if not hasattr(self, "pending_requests"):
            self.pending_requests = set()

        request_id = msg.get("request_id")
        client_id = msg["src"]
        cache_key = (client_id, request_id)
        # Duplication handleing, return cached respnce if seen before
        if cache_key  in self.client_response_cache:
            self._send(self.client_response_cache[cache_key])
            return
        # If current request is already in the log but not committed yet, do not append it again
        if cache_key in self.pending_requests:
            return
        # got rid of the check for if not leader, as the brief said to ignore that for part 2 and just respond to the client for debugging purposes. The response is still sent through the network router, but it will be sent even if this node is not the leader.
        # if not the leader forward to a known leader        
        if self.role != LEADER:
            if self.leader_id is not None:
                forward_msg = dict(msg)  
                forward_msg['dst'] = self.leader_id
                self._send(forward_msg)
                return
            
            response = make_client_response(
                self.node_id,
                msg['src'],  
                request_id=msg.get("request_id"),
                success=False,
                error="Not the leader",
                leader_hint=self.leader_id
            )
            #adding responce to cache before sending 
            self.client_response_cache[request_id] = response
            self._send(response)
            return
        
        # do a get and put operation on the kv store for debugging purposes
        operation = msg.get('operation', '').upper()
        key = msg.get('key')  
        if  operation == "PUT":
            value = msg.get('value')
            # changed self.kv_store[key] = value to append to the log
            entry = {
                "index": self._get_last_log_index() + 1,
                "term": self.current_term,
                "command":{
                    "operation": operation,
                    "key": key,
                    "value": value
                },
                "client_id": msg['src'],
                "request_id": msg.get("request_id")
            }
            self.pending_requests.add(cache_key)
            self.log.append(entry)
            self.save_state()
            self.send_heartbeats()
            return
            """
            response = make_client_response(
                self.node_id,
                msg['src'],
                request_id=msg.get("request_id"),
                success = True,
                value = value
            )

            """
        elif operation == "GET":
            if key in self.kv_store:
                value = self.kv_store[key]
                response = make_client_response(
                    self.node_id,
                    msg['src'],
                    request_id=msg.get("request_id"),
                    success=True,
                    value=self.kv_store[key]
                )
                print("RESPONSE:", response)
            else:
                response = make_client_response(
                    self.node_id,
                    msg['src'],
                    request_id=msg.get("request_id"),
                    success=False,
                    error=f"Key '{key}' not found"
                )
            print("RESPONSE:", response)
            self.client_response_cache[cache_key] = response
            self._send(response)
            return

        elif operation == "DELETE":
            # also changed this to read from the log 
                #del self.kv_store[key]
                entry = {
                    "index": self._get_last_log_index() + 1,
                    "term": self.current_term,
                    "command":{
                        "operation": "DELETE",
                        "key": key,
                        "value": None
                    },
                    "client_id": msg['src'],
                    "request_id": msg.get("request_id")
                }
                self.pending_requests.add(cache_key)
                self.log.append(entry)
                self.save_state()
                self.send_heartbeats()
                return

                """
                response = make_client_response(
                    self.node_id,
                    msg['src'],
                    request_id=msg.get("request_id"),
                    success=True,
                    value=None,
                    client_id=msg['src'],
                    request_id=msg.get("request_id")
                )
                print("RESPONSE:", response)
                """

        response = make_client_response(
            self.node_id,
            msg['src'],
            request_id=msg.get("request_id"),
            success=False,
            error=f"Unknown operation: {msg.get('operation')}"
        )
        self.client_response_cache[request_id] = response
        print(f"[{self.node_id}] SENDING RESPONSE:", response)
        self._send(response)


    # STATE MACHINE APPLICATION

    def apply_committed(self):
        # TODO: Implement state machine application
        if not hasattr(self, "client_response_cache"):
            self.client_response_cache = {}

        if not hasattr(self, "pending_requests"):
            self.pending_requests = set()

        # Loop through last applied and commit index
        while self.last_applied < self.commit_index:
        #for i in range(self.last_applied + 1, self.commit_index + 1):
            self.last_applied += 1
            entry = self._get_log_entry(self.last_applied)
            if entry is None:
                # This should not happen, but just in case
                print(f"[{self.node_id}] No log entry found at index {self.last_applied}")
                #skip
                continue

            cmd = entry.get('command', {})
            # Pull the data from this specific log entry
            operation = cmd.get('operation', '').upper()
            key = cmd.get('key')
            value = cmd.get('value')

            # Apply this entry's data to the key store
            # handling for put/ overwrites
            if operation == "PUT":
                self.kv_store[key] = value
                success = True
                error = None
                responce_value = value

            # handleing for deletes (if the key exists)
            elif operation == "DELETE":
                #check if the key exists before trying to delete it
                existed = key in self.kv_store
                self.kv_store.pop(key, None)
                success = existed
                error = None if existed else f"Key '{key}' not found"
                responce_value = None   

            # handleing for unknown operations
            else:
                success = False
                error = f"Unknown operation: {operation}"
                responce_value = None

            request_id = entry.get("request_id")
            client_id = entry.get("client_id")
            # If leader, send response using this entry's metadata
            if client_id is not None and request_id is not None:
                response = make_client_response(
                    self.node_id,
                    # Use the ID form the log entry 
                    client_id,
                    request_id = request_id,
                    success = success,
                    value = responce_value,
                    error = error
                )
                cache_key = (client_id, request_id)
                self.client_response_cache[cache_key] = response
                self.pending_requests.discard(cache_key)
                self._send(response)
        # To update the pointer  
        # self.last_applied = self.commit_index

    # CHECKPOINTING / SNAPSHOTTING (Part 3)

    def take_snapshot(self):
        # TODO (Part 3): Implement snapshotting
        #if nothing to snapshot then return
        if self.last_applied == 0:
            return
        
        last_included_index = self.last_applied
        last_included_term = self._get_log_term(last_included_index)

        snap = {
            "kv_store": self.kv_store.copy(),
            "last_included_index": last_included_index,
            #to preserve log consistency after old log entries are deleted
            "last_included_term": last_included_term
        }
        #keep just the logs after the snapshot 
        self.log = [
            entry for entry in self.log if entry['index'] > last_included_index
            ]
        
        self.snapshot=snap
        self.save_state()
        

    def load_snapshot(self, snapshot_data):
        # TODO (Part 3): Implement snapshot loading
        # is theres no snapshot data then return
        if snapshot_data is None:
            return

        # Update the key-value store
        self.kv_store = snapshot_data.get("kv_store", {}).copy()
        last_included_index = snapshot_data.get("last_included_index", 0)

        self.last_applied = last_included_index
        self.current_term = last_included_index

        self.snapshot = snapshot_data

    # STATE PERSISTENCE (Part 3)

    def save_state(self):
        # TODO (Part 3): Implement state persistence

        # Build the dictionary
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
            "snapshot": getattr(self, "snapshot", None)
        }

        os.makedirs(DATA_DIR, exist_ok=True)

        # Define paths
        final_path = f"data/{self.node_id}.json"
        temp_path = final_path + ".tmp"

        # Write to temp file
        with open(temp_path, 'w') as f:
            json.dump(state, f)

        # To instantly swap between both files
        os.replace(temp_path, final_path)

    def load_state(self):
        # TODO (Part 3): Implement state loading

        # Build the file path
        file_path = f"data/{self.node_id}.json"
        
        # Check if the file exists
        if os.path.exists(file_path):
            # Open and read the file
            with open(file_path, 'r') as f:
                data = json.load(f) # Turn text into a python dictionary
        else:
            return # Return nothing if file does not exist

        # Update the variables
        self.current_term = data.get('current_term', 0)
        self.voted_for = data.get('voted_for')
        self.log = data.get('log', [])
        self.snapshot = data.get('snapshot', None)

        if self.snapshot is not None:
            self.load_snapshot(self.snapshot)
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
        self.save_state

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
