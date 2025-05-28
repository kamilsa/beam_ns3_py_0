import beam_ns3
import random

# --- Constants ---
NUM_SUBNETS = 32
PEERS_PER_SUBNET = 1024 # Regular peers per subnet that send signatures

# --- Global State for App Logic and Setup ---
# These will be populated during setup
global_aggregator_idx = -1
# role_map: peer_idx -> {"role": "global_aggregator" | "subnet_aggregator" | "peer", ... other details}
role_map = {}

# For global aggregator to track received subnet aggregations
all_subnet_aggregations_received = set()  # Stores subnet_ids from which aggregation is received

wire = beam_ns3.WireProps(bitrate=(96 << 14), delay_ms=10)  # Standard wire properties

# --- Node and Network Setup ---
beam_ns3.print2("Starting network setup...")

# 1. Create Global Aggregator
global_aggregator_idx = beam_ns3.add_peer()
role_map[global_aggregator_idx] = {"role": "global_aggregator"}
beam_ns3.print2(f"Global Aggregator: Peer {global_aggregator_idx}")

# 2. Create Core Router
core_router_node_idx = beam_ns3.add_router() # NS-3 router indices are different from peer indices
beam_ns3.print2(f"Core Router: Router Node {core_router_node_idx}")

# 3. Wire Global Aggregator to Core Router
beam_ns3.wire_peer(global_aggregator_idx, core_router_node_idx, wire)
beam_ns3.print2(f"Wired Global Aggregator Peer {global_aggregator_idx} to Core Router Node {core_router_node_idx}")

# 4. Create Subnet Routers and connect to Core Router
subnet_router_node_indices = []
for i in range(NUM_SUBNETS):
    sr_node_idx = beam_ns3.add_router()
    subnet_router_node_indices.append(sr_node_idx)
    beam_ns3.wire_router(sr_node_idx, core_router_node_idx, wire)
    beam_ns3.print2(f"Subnet Router {i}: Router Node {sr_node_idx}, wired to Core Router Node {core_router_node_idx}")

# 5. Create Subnets: Subnet Aggregators and Peers
for s_idx in range(NUM_SUBNETS):
    beam_ns3.print2(f"Setting up Subnet {s_idx}...")
    current_subnet_router_node_idx = subnet_router_node_indices[s_idx]
    subnet_threshold = (PEERS_PER_SUBNET * 2 // 3) + 1

    # Create Subnet Aggregator for this subnet
    sa_peer_idx = beam_ns3.add_peer()
    role_map[sa_peer_idx] = {
        "role": "subnet_aggregator",
        "subnet_id": s_idx,
        "threshold": subnet_threshold,
        "global_aggregator_idx": global_aggregator_idx  # Provide global agg idx
    }
    beam_ns3.wire_peer(sa_peer_idx, current_subnet_router_node_idx, wire)
    beam_ns3.print2(f"  Subnet {s_idx} Aggregator: Peer {sa_peer_idx}, wired to Subnet Router Node {current_subnet_router_node_idx}")

    # Create Regular Peers for this subnet
    for p_num in range(PEERS_PER_SUBNET):
        p_peer_idx = beam_ns3.add_peer()
        role_map[p_peer_idx] = {
            "role": "peer",
            "subnet_id": s_idx,
            "my_subnet_aggregator_idx": sa_peer_idx
        }
        beam_ns3.wire_peer(p_peer_idx, current_subnet_router_node_idx, wire)
        # Optional: print progress for large number of peers
        # if p_num > 0 and p_num % 200 == 0:
        #     beam_ns3.print2(f"    Peer {p_num} (Global ID {p_peer_idx}) in Subnet {s_idx} wired.")

    beam_ns3.print2(f"  Subnet {s_idx} setup complete. Aggregator: Peer {sa_peer_idx}, Peers: {PEERS_PER_SUBNET}, Threshold: {subnet_threshold}")

beam_ns3.print2("Network setup finished.")

# --- App Class Definition ---
class App(beam_ns3.App):
    def __init__(self, index):
        super().__init__(index)
        my_role_info = role_map.get(self.index, {})
        self.role = my_role_info.get("role", "unknown")

        if self.role == "peer":
            self.my_subnet_aggregator_idx = my_role_info.get("my_subnet_aggregator_idx", -1)
            self.subnet_id = my_role_info.get("subnet_id", -1)
        elif self.role == "subnet_aggregator":
            self.subnet_id = my_role_info.get("subnet_id", -1)
            self.threshold = my_role_info.get("threshold", 0)
            self.global_aggregator_idx = my_role_info.get("global_aggregator_idx", -1)
            self.signatures_collected_count = 0
            self.subnet_aggregation_sent_flag = False
        elif self.role == "global_aggregator":
            # The count will be based on the length of the global `all_subnet_aggregations_received` set
            pass # No specific init state other than role

    async def on_start(self):
        # beam_ns3.print2(f"Peer {self.index} (Role: {self.role}, Subnet: {getattr(self, 'subnet_id', 'N/A')}) starting.")
        if self.role == "peer":
            if self.my_subnet_aggregator_idx != -1:
                # Stagger sends to avoid overwhelming the subnet aggregator initially
                await beam_ns3.co_sleep_us(random.randint(10 * 1000, 100 * 1000))
                # beam_ns3.print2(f"Peer {self.index} (Subnet {self.subnet_id}) connecting to Subnet Agg {self.my_subnet_aggregator_idx}")
                self.connect(self.my_subnet_aggregator_idx)
                # beam_ns3.print2(f"Peer {self.index} sending signature to Subnet Agg {self.my_subnet_aggregator_idx}")
                self.send(self.my_subnet_aggregator_idx, b"signature")
        # Subnet aggregators and Global aggregator primarily react to messages.

    async def on_message(self, from_peer_idx, message):
        # beam_ns3.print2(f"Peer {self.index} (Role: {self.role}) received message from {from_peer_idx}: {message[:30]}...")

        if self.role == "subnet_aggregator":
            if message.startswith(b"signature"):
                self.signatures_collected_count += 1
                # Optional: Log every signature or only milestones
                # beam_ns3.print2(f"Subnet Agg {self.index} (Subnet {self.subnet_id}) collected {self.signatures_collected_count}/{self.threshold} sigs.")

                if self.signatures_collected_count >= self.threshold and not self.subnet_aggregation_sent_flag:
                    self.subnet_aggregation_sent_flag = True
                    # Simulate some aggregation work before sending
                    await beam_ns3.co_sleep_us(random.randint(50 * 1000, 150 * 1000))
                    beam_ns3.print2(f"Subnet Agg {self.index} (Subnet {self.subnet_id}) threshold reached. Sending aggregation to Global Agg {self.global_aggregator_idx}.")
                    self.connect(self.global_aggregator_idx)
                    subnet_agg_message = f"subnet_aggregation:{self.subnet_id}".encode()
                    self.send(self.global_aggregator_idx, subnet_agg_message)

        elif self.role == "global_aggregator":
            if message.startswith(b"subnet_aggregation:"):
                try:
                    parts = message.split(b":")
                    if len(parts) > 1:
                        received_subnet_id_str = parts[1].decode()
                        received_subnet_id = int(received_subnet_id_str)

                        if received_subnet_id not in all_subnet_aggregations_received:
                            all_subnet_aggregations_received.add(received_subnet_id)
                            current_received_count = len(all_subnet_aggregations_received)

                            beam_ns3.print2(f"Global Agg {self.index} received aggregation from Subnet {received_subnet_id}. Total distinct: {current_received_count}/{NUM_SUBNETS}.")

                            if current_received_count == NUM_SUBNETS:
                                beam_ns3.print2(f"SUCCESS: Global aggregator {self.index} received all {NUM_SUBNETS} subnet aggregations. Stopping simulation.")
                                beam_ns3.stop()
                        # else:
                        #     beam_ns3.print2(f"Global Agg {self.index} received DUPLICATE aggregation from Subnet {received_subnet_id}.")
                    else:
                        beam_ns3.print2(f"Global Agg {self.index} received malformed subnet_aggregation message: {message}")
                except ValueError:
                    beam_ns3.print2(f"Global Agg {self.index} error parsing subnet_id from message: {message}")
                except Exception as e:
                    beam_ns3.print2(f"Global Agg {self.index} error processing message '{message}': {e}")

# --- Simulation Run ---
total_peer_nodes_created = len(role_map)
beam_ns3.print2(f"Total peer nodes created and configured: {total_peer_nodes_created}")
beam_ns3.print2(f"Starting simulation. Global Aggregator Peer {global_aggregator_idx} needs {NUM_SUBNETS} subnet aggregations.")
# Timeout needs to be sufficient for all messages and processing.
# 1 (GA) + 10 (SA) + 10*1024 (Peers) = 10251 peers.
# Each peer sends to SA, SA sends to GA.
# Max hops: Peer -> SubnetRouter -> CoreRouter -> SubnetRouter -> SA (for connect, if SA is far, but here SA is on same subnet router)
# Peer -> SR -> SA (signature)
# SA -> SR -> CoreRouter -> GA (subnet aggregation)
# Delays: 10ms per hop. Random processing/sleep delays up to ~250ms (0.1+0.15)
# Total time could be significant.
beam_ns3.run(App, timeout_sec=600) # Increased timeout for the larger simulation
beam_ns3.print2("Simulation run initiated. Check logs for progress and completion.")
