import beam_ns3
import random
import math

# --- Constants ---
NUM_SUBNETS = 20  # As per user request
PEERS_PER_SUBNET = 1024  # As per user request
NUM_BACKBONE_ROUTERS = 5  # From provided topology script

# --- Global State ---
# role_map will store the role and specific info for each node index
# e.g., role_map[node_idx] = {"role": "peer", "subnet_id": 0, "my_subnet_aggregator_idx": sa_idx}
#      role_map[node_idx] = {"role": "subnet_aggregator", "subnet_id": 0, "threshold": T, "global_aggregator_idx": ga_idx}
#      role_map[node_idx] = {"role": "global_aggregator"}
role_map = {}
global_aggregator_idx = -1  # Will be set by setup_network

# --- Network property generators (from user prompt) ---
def get_realistic_latency(distance_category):
    """Return latency based on simulated distance"""
    base_latencies = {
        "local": (1, 5),      # 1-5ms for same datacenter/city
        "regional": (5, 25),  # 5-25ms for same region
        "continental": (20, 80),  # 20-80ms cross-country
        "intercontinental": (80, 200)  # 80-200ms international
    }
    min_ms, max_ms = base_latencies[distance_category]
    return random.uniform(min_ms, max_ms)

def get_realistic_bandwidth(connection_type):
    """Return bandwidth based on connection type"""
    # Values in Mbps, will be converted to NS-3 format
    bandwidths = {
        "backbone": (10000, 100000),  # 10-100 Gbps backbone
        "datacenter": (1000, 10000),  # 1-10 Gbps datacenter
        "business": (100, 1000),      # 100 Mbps - 1 Gbps business
        "consumer": (10, 100)         # 10-100 Mbps consumer
    }
    min_mbps, max_mbps = bandwidths[connection_type]
    mbps = random.uniform(min_mbps, max_mbps)
    return int(mbps * (1 << 20))  # Convert to bps for NS-3

class App(beam_ns3.App):
    def __init__(self, index):
        super().__init__(index)
        self.my_role_info = role_map.get(self.index)
        if not self.my_role_info:
            # This should not happen if setup_network populates role_map correctly for all app instances
            self.print(f"FATAL: Role info not found for index {self.index} in role_map.")
            raise ValueError(f"Role info not found for index {self.index}")

        self.role = self.my_role_info["role"]

        if self.role == "peer":
            self.subnet_id = self.my_role_info["subnet_id"]
            self.my_subnet_aggregator_idx = self.my_role_info["my_subnet_aggregator_idx"]
        elif self.role == "subnet_aggregator":
            self.subnet_id = self.my_role_info["subnet_id"]
            self.threshold = self.my_role_info["threshold"]
            # Store the target global aggregator's index
            self.global_aggregator_idx_target = self.my_role_info["global_aggregator_idx"]
            self.signatures_received_from_peers = set() # Stores indices of peers who sent signatures
            self.aggregation_sent_to_global = False
        elif self.role == "global_aggregator":
            self.subnet_aggregations_received = set() # Stores subnet_ids from which aggregations are received
            self.expected_subnet_aggregations = NUM_SUBNETS
        else:
            self.print(f"FATAL: Unknown role '{self.role}' for index {self.index}.")
            raise ValueError(f"Unknown role: {self.role}")

    async def on_start(self):
        self.print(f"Node {self.index} starting with role: {self.role}")
        if self.role == "peer":
            self.print(f"Peer in subnet {self.subnet_id} (Node {self.index}) generating signature for SA {self.my_subnet_aggregator_idx}")
            # Simulate some work / random delay before sending
            await beam_ns3.co_sleep_us(random.randint(1000, 100000)) # 1ms to 100ms
            signature_content = f"signature_peer_{self.index}_subnet_{self.subnet_id}"
            self.connect(self.my_subnet_aggregator_idx)
            self.send(self.my_subnet_aggregator_idx, signature_content.encode())
            self.print(f"Peer {self.index} sent signature to SA {self.my_subnet_aggregator_idx}")
        elif self.role == "subnet_aggregator":
            self.print(f"Subnet Aggregator for subnet {self.subnet_id} (Node {self.index}) waiting for {self.threshold} signatures.")
        elif self.role == "global_aggregator":
            self.print(f"Global Aggregator (Node {self.index}) waiting for {self.expected_subnet_aggregations} subnet aggregations.")

    async def on_message(self, sender_index, message):
        try:
            message_str = message.decode()
        except UnicodeDecodeError:
            self.print(f"Node {self.index} ({self.role}) received undecodable message from {sender_index}")
            return

        self.print(f"Node {self.index} ({self.role}) received message from {sender_index}: '{message_str[:70]}{'...' if len(message_str) > 70 else ''}'")

        if self.role == "subnet_aggregator":
            if message_str.startswith("signature_peer_"):
                sender_info = role_map.get(sender_index)
                if sender_info and sender_info["role"] == "peer" and sender_info.get("subnet_id") == self.subnet_id:
                    if sender_index not in self.signatures_received_from_peers:
                        self.signatures_received_from_peers.add(sender_index)
                        self.print(f"SA for subnet {self.subnet_id} (Node {self.index}) received signature from peer {sender_index}. Total distinct signatures: {len(self.signatures_received_from_peers)}/{self.threshold}")

                        if len(self.signatures_received_from_peers) >= self.threshold and not self.aggregation_sent_to_global:
                            self.print(f"SA for subnet {self.subnet_id} (Node {self.index}) collected {len(self.signatures_received_from_peers)} signatures (threshold: {self.threshold}). Creating and sending aggregation to GA {self.global_aggregator_idx_target}.")
                            aggregation_content = f"subnet_aggregation_subnet_{self.subnet_id}"
                            self.connect(self.global_aggregator_idx_target)
                            self.send(self.global_aggregator_idx_target, aggregation_content.encode())
                            self.aggregation_sent_to_global = True
                            self.print(f"SA for subnet {self.subnet_id} (Node {self.index}) sent aggregation to GA {self.global_aggregator_idx_target}.")
                    # else:
                        # self.print(f"SA for subnet {self.subnet_id} (Node {self.index}) received duplicate signature from peer {sender_index}.")
                else:
                    self.print(f"SA for subnet {self.subnet_id} (Node {self.index}) received signature from unexpected sender {sender_index} or sender not in this subnet. Sender info: {sender_info}")

        elif self.role == "global_aggregator":
            if message_str.startswith("subnet_aggregation_subnet_"):
                try:
                    parts = message_str.split("_")
                    received_subnet_id = int(parts[-1])

                    sender_info = role_map.get(sender_index)
                    if sender_info and sender_info["role"] == "subnet_aggregator" and sender_info.get("subnet_id") == received_subnet_id:
                        if received_subnet_id not in self.subnet_aggregations_received:
                            self.subnet_aggregations_received.add(received_subnet_id)
                            self.print(f"GA (Node {self.index}) received aggregation from SA for subnet {received_subnet_id} (Sender {sender_index}). Total distinct aggregations: {len(self.subnet_aggregations_received)}/{self.expected_subnet_aggregations}")

                            if len(self.subnet_aggregations_received) == self.expected_subnet_aggregations:
                                self.print(f"GA (Node {self.index}) received all {self.expected_subnet_aggregations} subnet aggregations. Stopping simulation.")
                                beam_ns3.stop()
                        # else:
                            # self.print(f"GA (Node {self.index}) received duplicate aggregation for subnet {received_subnet_id} from sender {sender_index}.")
                    else:
                        self.print(f"GA (Node {self.index}) received aggregation for subnet {received_subnet_id} from unexpected sender {sender_index} or mismatched SA. Sender info: {sender_info}")
                except (IndexError, ValueError) as e:
                    self.print(f"GA (Node {self.index}) could not parse subnet_id from message: '{message_str}'. Error: {e}")

def setup_network():
    global global_aggregator_idx # Ensure assignment to the global variable
    beam_ns3.print2("Starting realistic network setup...")

    # Create Global Aggregator
    _ga_idx_temp = beam_ns3.add_peer()
    global_aggregator_idx = _ga_idx_temp # Assign to the module-level global variable
    role_map[global_aggregator_idx] = {"role": "global_aggregator"}
    beam_ns3.print2(f"Created Global Aggregator: Node {global_aggregator_idx}")

    # Create backbone routers
    backbone_router_indices = []
    if NUM_BACKBONE_ROUTERS > 0:
        for i in range(NUM_BACKBONE_ROUTERS):
            backbone_idx = beam_ns3.add_router()
            backbone_router_indices.append(backbone_idx)
            beam_ns3.print2(f"Created backbone router {i}: Node {backbone_idx}")

        # Connect backbone routers in a mesh topology
        for i in range(NUM_BACKBONE_ROUTERS):
            for j in range(i + 1, NUM_BACKBONE_ROUTERS):
                wire_props_bb = beam_ns3.WireProps(
                    int(get_realistic_bandwidth("backbone")),
                    int(get_realistic_latency("continental"))
                )
                beam_ns3.wire_router(backbone_router_indices[i], backbone_router_indices[j], wire_props_bb)
                beam_ns3.print2(f"Connected backbone routers {backbone_router_indices[i]}-{backbone_router_indices[j]}")
    else:
        beam_ns3.print2("No backbone routers to create.")

    # Wire Global Aggregator to a random backbone router (if any)
    if backbone_router_indices:
        global_agg_router_connection_idx = random.choice(backbone_router_indices)
        wire_props_ga_conn = beam_ns3.WireProps(
            int(get_realistic_bandwidth("datacenter")),
            int(get_realistic_latency("local"))
        )
        beam_ns3.wire_peer(global_aggregator_idx, global_agg_router_connection_idx, wire_props_ga_conn)
        beam_ns3.print2(f"Connected Global Aggregator {global_aggregator_idx} to backbone router {global_agg_router_connection_idx}")
    else:
        # If no backbone, GA is isolated or needs direct connections later.
        # For this setup, it implies SAs might need to connect differently if no backbone path.
        # However, the logic below connects regional routers to backbone.
        beam_ns3.print2(f"Global Aggregator {global_aggregator_idx} not connected to any backbone router (none exist).")


    # Create regional routers
    # Ensure at least 1 region if NUM_SUBNETS > 0, to avoid division by zero or empty lists later.
    num_regions = max(1, NUM_SUBNETS // 8) if NUM_SUBNETS > 0 else 0
    regional_router_indices = []
    if num_regions > 0 :
        for i in range(num_regions):
            regional_idx = beam_ns3.add_router()
            regional_router_indices.append(regional_idx)

            if backbone_router_indices:
                num_bb_connections = random.randint(1, min(2, len(backbone_router_indices)))
                connected_backbones = random.sample(backbone_router_indices, num_bb_connections)

                for backbone_idx_for_regional_conn in connected_backbones:
                    wire_props_regional_to_bb = beam_ns3.WireProps(
                        int(get_realistic_bandwidth("datacenter")),
                        int(get_realistic_latency("regional"))
                    )
                    beam_ns3.wire_router(regional_idx, backbone_idx_for_regional_conn, wire_props_regional_to_bb)
                beam_ns3.print2(f"Created regional router {i} (Node {regional_idx}), connected to {num_bb_connections} backbone(s).")
            else:
                beam_ns3.print2(f"Created regional router {i} (Node {regional_idx}), but no backbone routers to connect to.")
    else:
        beam_ns3.print2("No regional routers to create (NUM_SUBNETS is 0 or too small for the formula and no fallback).")

    # Create subnet routers and connect to regional routers
    subnet_router_indices = []
    if NUM_SUBNETS > 0:
        for i in range(NUM_SUBNETS):
            subnet_router_idx = beam_ns3.add_router()
            subnet_router_indices.append(subnet_router_idx)

            if regional_router_indices:
                # Distribute subnet routers among regional routers
                region_for_subnet_router_idx = regional_router_indices[i % len(regional_router_indices)]

                wire_props_subnet_to_regional = beam_ns3.WireProps(
                    int(get_realistic_bandwidth("business")),
                    int(get_realistic_latency("local" if i % 2 == 0 else "regional"))
                )
                beam_ns3.wire_router(subnet_router_idx, region_for_subnet_router_idx, wire_props_subnet_to_regional)
                beam_ns3.print2(f"Created subnet router {i} (Node {subnet_router_idx}), connected to regional router {region_for_subnet_router_idx}")
            else:
                # If no regional routers, how do subnets connect? This implies a problem with earlier logic or constants.
                # For now, they will be isolated unless connected directly to something else.
                beam_ns3.print2(f"Created subnet router {i} (Node {subnet_router_idx}), but no regional routers to connect to.")
    else:
        beam_ns3.print2("No subnets to create.")


    # Create subnet aggregators and peers for each subnet
    for s_idx in range(NUM_SUBNETS):
        beam_ns3.print2(f"Setting up Subnet {s_idx}...")
        if s_idx >= len(subnet_router_indices):
            beam_ns3.print2(f"Error: Subnet router for subnet {s_idx} not found/created. Skipping this subnet's peers and SA.")
            continue
        current_subnet_router_idx = subnet_router_indices[s_idx]

        # Threshold for this subnet: 2/3 of its peers + 1
        subnet_threshold = (PEERS_PER_SUBNET * 2 // 3) + 1

        # Create Subnet Aggregator for this subnet
        sa_peer_idx = beam_ns3.add_peer()
        role_map[sa_peer_idx] = {
            "role": "subnet_aggregator",
            "subnet_id": s_idx,
            "threshold": subnet_threshold,
            "global_aggregator_idx": global_aggregator_idx # Target GA
        }
        beam_ns3.print2(f"  Created Subnet Aggregator for subnet {s_idx}: Node {sa_peer_idx} (Threshold: {subnet_threshold}, GA Target: {global_aggregator_idx})")

        wire_props_sa_to_subnet_router = beam_ns3.WireProps(
            int(get_realistic_bandwidth("business")),
            int(get_realistic_latency("local"))
        )
        beam_ns3.wire_peer(sa_peer_idx, current_subnet_router_idx, wire_props_sa_to_subnet_router)

        # Create regular peers for this subnet
        for p_num in range(PEERS_PER_SUBNET):
            p_peer_idx = beam_ns3.add_peer()
            role_map[p_peer_idx] = {
                "role": "peer",
                "subnet_id": s_idx,
                "my_subnet_aggregator_idx": sa_peer_idx # This peer's SA
            }

            connection_quality = "business" if random.random() < 0.3 else "consumer" # 30% business, 70% consumer
            wire_props_peer_to_subnet_router = beam_ns3.WireProps(
                int(get_realistic_bandwidth(connection_quality)),
                int(get_realistic_latency("local"))
            )
            beam_ns3.wire_peer(p_peer_idx, current_subnet_router_idx, wire_props_peer_to_subnet_router)

            # Log sparsely for large numbers of peers
            if p_num % max(1, (PEERS_PER_SUBNET // 10)) == 0 or p_num == PEERS_PER_SUBNET -1 :
                 beam_ns3.print2(f"    Added peer {p_num+1}/{PEERS_PER_SUBNET} (Node {p_peer_idx}) to subnet {s_idx}, connected to SA {sa_peer_idx}")

    beam_ns3.print2("Realistic network topology setup complete.")

    # Diagnostic: Print role counts
    counts = {"peer": 0, "subnet_aggregator": 0, "global_aggregator": 0, "unknown": 0}
    for node_info in role_map.values():
        counts[node_info.get("role", "unknown")] += 1
    beam_ns3.print2(f"Role map counts: Peers={counts['peer']}, SubnetAggregators={counts['subnet_aggregator']}, GlobalAggregators={counts['global_aggregator']}")
    expected_peers = NUM_SUBNETS * PEERS_PER_SUBNET
    expected_sas = NUM_SUBNETS
    expected_gas = 1 if global_aggregator_idx != -1 else 0
    if counts['peer'] != expected_peers or counts['subnet_aggregator'] != expected_sas or counts['global_aggregator'] != expected_gas :
        beam_ns3.print2(f"WARNING: Role map counts mismatch! Expected P:{expected_peers}, SA:{expected_sas}, GA:{expected_gas}")


# --- Main simulation execution ---
if __name__ == "__main__":
    beam_ns3.print2(f"Simulation starting with NUM_SUBNETS={NUM_SUBNETS}, PEERS_PER_SUBNET={PEERS_PER_SUBNET}")

    setup_network()

    if not role_map:
        beam_ns3.print2("Error: role_map is empty after network setup. Cannot create App instances.")
    elif global_aggregator_idx == -1 or global_aggregator_idx not in role_map:
        beam_ns3.print2(f"Error: Global aggregator index {global_aggregator_idx} is invalid or not found in role_map. Cannot reliably run simulation.")
    else:
        # Pass the App class itself and a timeout to beam_ns3.run
        # CPy in beam_ns3.py will instantiate App for each peer.
        # Using a timeout of 1 hour (3600 seconds) as a safeguard.
        # The simulation should stop earlier via beam_ns3.stop().
        beam_ns3.print2(f"Starting simulation with App factory. Timeout set to 3600 seconds.")
        beam_ns3.run(App, timeout_sec=3600)
        beam_ns3.print2("Simulation finished (either by beam_ns3.stop() or timeout).")

    beam_ns3.print2("Script execution complete.")

