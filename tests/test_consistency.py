import pytest
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.node.node import Node
from splitbrain.coordinator.coordinator import Coordinator
from splitbrain.consistency.strong import StrongConsistency
from splitbrain.consistency.quorum import QuorumConsistency
from splitbrain.consistency.eventual import EventualConsistency

@pytest.fixture
def environment():
    clock = SimulatedClock()
    engine = EventEngine(clock)
    network = Network(engine)
    # Using 3 nodes
    nodes = [Node(f"n{i}", network, clock) for i in range(1, 4)]
    node_ids = [n.node_id for n in nodes]
    coordinator = Coordinator(engine, network, nodes)
    
    strong = StrongConsistency(engine, network, node_ids)
    quorum = QuorumConsistency(engine, network, node_ids)
    eventual = EventualConsistency(engine, network, nodes, gossip_interval_ms=10)
    
    coordinator.register_strategy("strong", strong)
    coordinator.register_strategy("quorum", quorum)
    coordinator.register_strategy("eventual", eventual)
    
    return clock, engine, network, coordinator

def test_strong_consistency(environment):
    clock, engine, network, coordinator = environment
    
    results = {}
    def write_cb(success):
        results["write"] = success
        
    def read_cb(value, success):
        results["read"] = value
        
    coordinator.write("k1", "v1", "strong", "w1", write_cb)
    engine.run(until=clock.current_time + 3000)
    
    assert results.get("write") is True
    
    coordinator.read("k1", "strong", "r1", read_cb)
    engine.run(until=clock.current_time + 3000)
    
    assert results.get("read") == "v1"

def test_strong_consistency_partition(environment):
    clock, engine, network, coordinator = environment
    
    results = {}
    def write_cb(success):
        results["write"] = success
        
    network.add_partition("coordinator", "n3") # n3 is unreachable
    
    coordinator.write("k1", "v1", "strong", "w1", write_cb)
    engine.run(until=clock.current_time + 3000)
    
    # Write should fail (timeout) because not all nodes can ACK
    assert results.get("write") is False

def test_quorum_consistency_partition(environment):
    clock, engine, network, coordinator = environment
    
    results = {}
    def write_cb(success):
        results["write"] = success
        
    network.add_partition("coordinator", "n3") # n3 is unreachable
    
    coordinator.write("k1", "v1", "quorum", "w1", write_cb)
    engine.run(until=clock.current_time + 3000)
    
    # Write should succeed because n1 and n2 (majority) can ACK
    assert results.get("write") is True

def test_eventual_consistency_gossip(environment):
    clock, engine, network, coordinator = environment
    
    results = {}
    def write_cb(success):
        results["write"] = success
        
    # Write to one random node
    coordinator.write("k1", "v1", "eventual", "w1", write_cb)
    # Run just enough to get the ACK
    engine.run(until=clock.current_time + 5)
    
    assert results.get("write") is True
    
    # Check that not all nodes have the value yet (unless they were the randomly chosen one)
    # Give gossip some time to run
    engine.run(until=clock.current_time + 200)
    
    # Now all nodes should have the value
    for node in coordinator.nodes:
        assert node.get_state().get("k1")[0] == "v1"
