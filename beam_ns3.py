import beam_ns3_install

from ns import ns
import cppyy
import sys
import os

cppyy.include("beam_ns3.hpp")
beam_ns3 = cppyy.gbl.beam_ns3

# Store the original add_peer and add_router functions
_original_add_peer = beam_ns3.add_peer
_original_add_router = beam_ns3.add_router

def add_peer():
    """MPI-aware function to add a peer.
       Only fully creates the peer on the designated MPI rank."""
    index = beam_ns3.simulation.peers_.GetN() # Get potential index

    # Create the node object on all ranks to keep node counts consistent.
    # NS-3's NodeContainer (peers_) needs to be consistent across ranks.
    beam_ns3.simulation.peers_.Create(1)

    if is_node_local(index):
        # This rank is responsible for this node
        node = beam_ns3.simulation.peers_.Get(index)
        beam_ns3.simulation.internet_stack_.Install(node)
        app_helper = ns.ApplicationHelper(beam_ns3.Application.GetTypeId())
        beam_ns3.simulation.applications_.Add(app_helper.Install(node))
        beam_ns3.simulation.application(index).index_ = index
        if mpi_enabled:
            print(f"MPI Rank {mpi_rank}: Created local peer {index}")
    elif mpi_enabled:
        # Other ranks just acknowledge the peer for indexing purposes
        print(f"MPI Rank {mpi_rank}: Registered remote peer {index}")
    return index

def add_router():
    """MPI-aware function to add a router.
       Only fully creates the router on the designated MPI rank."""
    index = beam_ns3.simulation.routers_.GetN() # Get potential index

    # Create the node object on all ranks for consistency.
    beam_ns3.simulation.routers_.Create(1)

    if is_node_local(index):
        # This rank is responsible for this router
        router_node = beam_ns3.simulation.routers_.Get(index)
        beam_ns3.simulation.internet_stack_.Install(router_node)
        if mpi_enabled:
            print(f"MPI Rank {mpi_rank}: Created local router {index}")
    elif mpi_enabled:
        print(f"MPI Rank {mpi_rank}: Registered remote router {index}")
    return index

# Update the reexport list and globals to use the new MPI-aware functions
reexport = ["WireProps", "wire_peer", "wire_router"]
globals().update({k: getattr(beam_ns3, k) for k in reexport})
globals()['add_peer'] = add_peer
globals()['add_router'] = add_router

# MPI support variables
mpi_enabled = False
mpi_rank = 0
mpi_size = 1

# Initialize MPI if available
def init_mpi():
    global mpi_enabled, mpi_rank, mpi_size
    try:
        # Check if running with mpirun or mpiexec
        if "mpirun" in sys.argv[0] or \
           "mpiexec" in sys.argv[0] or \
           os.environ.get('OMPI_COMM_WORLD_SIZE') or \
           os.environ.get('PMI_SIZE'):

            if hasattr(ns, 'MpiInterface'):
                ns.MpiInterface.Enable(ns.MpiInterface.NORMAL_SIMULATOR) # Use NORMAL_SIMULATOR
                mpi_enabled = True
                mpi_rank = ns.MpiInterface.GetSystemId()
                mpi_size = ns.MpiInterface.GetSize()
                print(f"MPI enabled: Rank {mpi_rank} of {mpi_size}")
                return True
            else:
                print("MPI interface not available in NS-3 installation.")
                return False
        else:
            print("Not running under mpirun/mpiexec, MPI not initialized.")
            return False
    except Exception as e:
        print(f"Failed to initialize MPI: {e}")
        return False

# Distribute node responsibility across MPI processes
def is_node_local(node_id):
    """Determine if this node should be simulated by the current MPI rank"""
    if not mpi_enabled or mpi_size <= 1:
        return True
    return node_id % mpi_size == mpi_rank

def cleanup_mpi():
    """Cleanup MPI resources"""
    if mpi_enabled:
        # NS-3's MPI interface handles cleanup in Simulator::Destroy()
        # Additional cleanup can be added here if needed
        print(f"MPI Rank {mpi_rank}: Cleaning up MPI resources (handled by NS-3)")


def catch(f):
    def w(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print(e)
            exit(-1)

    return w


class App:
    def __init__(self, index):
        self.index = index

    def on_start(self):
        pass

    def on_message(self, index, message):
        pass

    def connect(self, index):
        beam_ns3.socket_connect(self.index, index)

    def send(self, index, message):
        message = bytearray(message)
        beam_ns3.socket_send(self.index, index, message)

    def print(self, *args):
        print2(f"peer {self.index}:", *args)


class CPy(beam_ns3.CPy):
    app = None
    next_timer_id = 0
    timers = dict()

    def __init__(self):
        super().__init__()
        self.apps = dict()

    @catch
    def on_start(self, peer):
        app = CPy.app(peer)
        self.apps[peer] = app
        co_run(app.on_start())

    @catch
    def on_message(self, peer, from_peer, message):
        message = bytearray([ord(x) for x in message])
        app = self.apps[peer]
        co_run(app.on_message(from_peer, message))

    @catch
    def on_timer(self, timer_id):
        cb = CPy.timers.pop(timer_id)
        cb()


def run(app, timeout_sec):
    # Initialize MPI if it's available and we are running with mpirun/mpiexec
    init_mpi()

    CPy.app = app
    beam_ns3.run(CPy(), timeout_sec)

    # Cleanup MPI resources
    cleanup_mpi()


def stop():
    ns.Simulator.Stop()


# TODO: Now() returns 0 after Destroy()
def now_us():
    return ns.Simulator.Now().GetMicroSeconds()


def now_ms():
    return now_us() // 1000


def sleep_us(us, cb):
    timer_id = CPy.next_timer_id
    CPy.next_timer_id += 1
    CPy.timers[timer_id] = cb
    beam_ns3.sleep(int(us), timer_id)


def co_run(co):
    if co is None:
        return
    try:
        us = co.send(None)
        sleep_us(us, lambda: co_run(co))
    except StopIteration:
        pass


class co_sleep_us:
    def __init__(self, us):
        self.us = us

    def __await__(self):
        yield self.us


def print2(*args):
    print(f"{now_ms():4}ms: ", *args)

