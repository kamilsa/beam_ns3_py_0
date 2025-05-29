#pragma once

// NS-3 module includes for network simulation functionality
#include <ns3/applications-module.h>
#include <ns3/core-module.h>
#include <ns3/internet-module.h>
#include <ns3/network-module.h>
#include <ns3/point-to-point-helper.h>
#include <ns3/mpi-interface.h> // Added MPI module
#include <ns3/mpi-module.h>    // Added MPI module

// Custom assertion macro that provides better error information than standard assert
#define assert2(c)                                       \
  if (not(c)) {                                          \
    printf("assert2(%s) %s:%d", #c, __FILE__, __LINE__); \
    abort();                                             \
  }

namespace beam_ns3 {
  // Basic type definitions for convenience
  using Index = uint32_t;                   // Node/peer identifier type
  using SocketPtr = ns3::Ptr<ns3::Socket>;  // Smart pointer to NS-3 socket
  using Message = std::vector<uint8_t>;     // Message payload type
  using TimerId = uint32_t;                 // Timer identifier type

  // Default port used for all UDP connections
  const uint16_t kPort = 10000;

  // Properties for network links/wires between nodes
  struct WireProps {
    uint64_t bitrate;       // Link bandwidth in bits per second
    uint32_t delay_ms;      // Link latency in milliseconds
  };

  // Interface for C++ to Python callbacks
  // Implemented in Python to handle simulation events
  struct CPy {
    virtual ~CPy() = default;
    virtual void on_start(Index peer) = 0;                                  // Called when a peer starts
    virtual void on_message(Index peer, Index from_peer, Message message) = 0;  // Called when a peer receives a message
    virtual void on_timer(TimerId timer_id) = 0;                            // Called when a timer expires
  };

  // Utility for encoding 32-bit integers in big-endian format
  struct encode_u32_be {
    std::array<uint8_t, 4> bytes;
    encode_u32_be(uint32_t n)
        : bytes{(uint8_t)(n >> 24),
                (uint8_t)(n >> 16),
                (uint8_t)(n >> 8),
                (uint8_t)n} {}
  };

  // Utility for decoding 32-bit integers from big-endian format
  uint32_t decode_u32_be(const Message &bytes, size_t &at) {
    uint32_t v = ((uint8_t)bytes.at(at) << 24)
               | ((uint8_t)bytes.at(at + 1) << 16)
               | ((uint8_t)bytes.at(at + 2) << 8) | (uint8_t)bytes.at(at + 3);
    at += 4;
    return v;
  }

  // Utility to append one container to another
  void append(Message &l, auto &&r) {
    l.insert(l.end(), r.begin(), r.end());
  }

  // Forward declaration for Application class
  struct Application;

  // Main simulation state container
  struct Simulation {
    Simulation() {
      // Initialize IP address pool for the simulation
      address_helper_.SetBase("10.1.1.0", "255.255.255.0");
    }

    // Get Application instance for a given peer index
    ns3::Ptr<Application> application(Index index) const;

    ns3::InternetStackHelper internet_stack_;     // Helper for setting up internet stack on nodes
    ns3::Ipv4AddressHelper address_helper_;       // Helper for assigning IP addresses
    ns3::NodeContainer peers_;                    // Container for peer nodes
    ns3::NodeContainer routers_;                  // Container for router nodes
    ns3::ApplicationContainer applications_;      // Container for applications
    std::unordered_map<Index, ns3::Ipv4Address> ips_;  // Map from peer index to IP address
    std::unordered_map<ns3::Ipv4Address, Index, ns3::Ipv4AddressHash> ip_index_;  // Reverse lookup from IP to index
    CPy *cpy_;                                   // Pointer to Python callback interface
  };

  // Global simulation state
  Simulation simulation;

  // NS-3 application implementation for peers
  struct Application : public ns3::Application {
    // Required by NS-3 to register this application type
    static ns3::TypeId GetTypeId() {
      static ns3::TypeId tid = ns3::TypeId("Application")
                                   .SetParent<ns3::Application>()
                                   .AddConstructor<Application>();
      return tid;
    }

    // Called by NS-3 when application starts
    void StartApplication() override {
      listen();  // Start listening for incoming UDP packets
      simulation.cpy_->on_start(index_);  // Notify Python layer that peer has started
    }

    // Setup UDP listener socket
    void listen() {
      udp_socket_ = makeSocket();
      udp_socket_->Bind(ns3::InetSocketAddress{
          ns3::Ipv4Address::GetAny(),
          kPort,
      });
      udp_socket_->SetRecvCallback(
          ns3::MakeCallback(&Application::pollRead, this));
    }

    // Connect to another peer by index (for UDP, just store the peer's information)
    void connect(Index index) {
      // For UDP, no connection is needed, just store the peer's IP
      // We'll use it when sending messages
      connected_peers_.insert(index);
    }

    // Send a message to another peer
    void send(Index index, const Message &message) {
      assert2(index != index_);  // Can't send to self
      if (connected_peers_.find(index) == connected_peers_.end()) {
        return;  // Not connected to this peer
      }

      // Create message with length prefix (4 bytes) + payload
      Message message2;
      message2.reserve(4 + message.size());
      append(message2, encode_u32_be(message.size()).bytes);
      append(message2, message);

      // Create and send NS-3 packet
      auto packet = ns3::Create<ns3::Packet>((const uint8_t *)message2.data(),
                                           message2.size());
      ns3::InetSocketAddress remote(simulation.ips_.at(index), kPort);
      udp_socket_->SendTo(packet, 0, remote);
    }

    // Handle incoming data on the socket
    void pollRead(SocketPtr socket) {
      ns3::Address from;
      auto packet = socket->RecvFrom(from);
      if (!packet) {
        return;  // No data received
      }

      // Extract sender's IP address and look up the corresponding peer index
      ns3::InetSocketAddress address = ns3::InetSocketAddress::ConvertFrom(from);
      ns3::Ipv4Address ipAddr = address.GetIpv4();

      // Find the peer index for this IP address
      auto it = simulation.ip_index_.find(ipAddr);
      if (it == simulation.ip_index_.end()) {
        return;  // Unknown sender
      }

      Index sender_index = it->second;

      // Extract packet data
      auto n = packet->GetSize();
      auto &buffer = buffers_[sender_index];
      uint32_t old_size = buffer.size();
      buffer.resize(old_size + n);
      packet->CopyData(buffer.data() + old_size, n);

      // Process complete messages in the buffer
      while (buffer.size() >= 4) {
        size_t at = 0;
        auto size = decode_u32_be(buffer, at);  // Read message length
        if (buffer.size() < at + size) {
          break;  // Not enough data for complete message
        }

        // Extract and deliver message
        Message message;
        message.assign(buffer.begin() + at, buffer.begin() + at + size);
        at += size;
        buffer.erase(buffer.begin(), buffer.begin() + at);  // Remove processed data
        simulation.cpy_->on_message(index_, sender_index, message);
      }
    }

    // Create a new UDP socket
    SocketPtr makeSocket() {
      auto socket = ns3::Socket::CreateSocket(
          GetNode(), ns3::TypeId::LookupByName("ns3::UdpSocketFactory"));
      return socket;
    }

    Index index_;                                       // This peer's index
    SocketPtr udp_socket_;                              // UDP socket for sending/receiving
    std::unordered_set<Index> connected_peers_;         // Peers we are connected to
    std::unordered_map<Index, Message> buffers_;        // Message reassembly buffers for each peer
  };

  // Implementation of application lookup method
  ns3::Ptr<Application> Simulation::application(Index index) const {
    return applications_.Get(index)->GetObject<Application>();
  }

  // Create a new peer node and return its index
  Index add_peer() {
    Index index = simulation.peers_.GetN();
    simulation.peers_.Create(1);
    auto node = simulation.peers_.Get(index);
    simulation.internet_stack_.Install(node);
    simulation.applications_.Add(
        ns3::ApplicationHelper{Application::GetTypeId()}.Install(node));
    simulation.application(index)->index_ = index;
    return index;
  }

  // Create a new router node and return its index
  Index add_router() {
    Index index = simulation.routers_.GetN();
    simulation.routers_.Create(1);
    simulation.internet_stack_.Install(simulation.routers_.Get(index));
    return index;
  }

  // Helper function to create a network link between two nodes
  auto _wire(ns3::Ptr<ns3::Node> node1,
             ns3::Ptr<ns3::Node> node2,
             const WireProps &wire) {
    ns3::PointToPointHelper helper;
    helper.SetDeviceAttribute("DataRate", ns3::DataRateValue{wire.bitrate});
    helper.SetChannelAttribute(
        "Delay", ns3::TimeValue{ns3::MilliSeconds(wire.delay_ms)});
    auto interfaces =
        simulation.address_helper_.Assign(helper.Install(node1, node2));
    simulation.address_helper_.NewNetwork();
    return interfaces.GetAddress(0);
  }

  // Connect a peer to a router with given link properties
  void wire_peer(Index peer, Index router, const WireProps &wire) {
    assert2(not simulation.ips_.contains(peer));
    auto ip = _wire(
        simulation.peers_.Get(peer), simulation.routers_.Get(router), wire);
    simulation.ips_.emplace(peer, ip);
    simulation.ip_index_.emplace(ip, peer);
  }

  // Connect two routers with given link properties
  void wire_router(Index router1, Index router2, const WireProps &wire) {
    _wire(simulation.routers_.Get(router1),
          simulation.routers_.Get(router2),
          wire);
  }

  // Initiate a UDP connection from one peer to another
  void socket_connect(Index peer1, Index peer2) {
    simulation.application(peer1)->connect(peer2);
  }

  // Send a message from one peer to another
  void socket_send(Index peer1, Index peer2, const Message &message) {
    simulation.application(peer1)->send(peer2, message);
  }

  // Schedule a timer callback after specified microseconds
  void sleep(uint64_t us, TimerId timer_id) {
    ns3::Simulator::Schedule(ns3::MicroSeconds(us), [timer_id] {
      simulation.cpy_->on_timer(timer_id);
    });
  }

  // Run the simulation with Python callbacks
  void run(CPy *cpy, uint32_t timeout_sec) {
    // Setup routing tables
    ns3::Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    // Start applications at time 0
    simulation.applications_.Start(ns3::Seconds(0));

    // Set simulation end time
    ns3::Simulator::Stop(ns3::Seconds(timeout_sec));

    // Store Python callback interface
    simulation.cpy_ = cpy;

    // Run simulation with exception handling
    try {
      ns3::Simulator::Run();
    } catch (std::exception &e) {
      printf("catch exception %s\n", e.what());
      exit(-1);
    } catch (...) {
      printf("catch ... %s\n",
             __cxxabiv1::__cxa_current_exception_type()->name());
      exit(-1);
    }

    // Cleanup
    simulation.cpy_ = nullptr;
    ns3::Simulator::Destroy();
  }
}  // namespace beam_ns3

