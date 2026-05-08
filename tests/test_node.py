import pytest
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.node.node import Node
from splitbrain.core.message import Message, MessageType

@pytest.fixture
def environment():
    clock = SimulatedClock()
    engine = EventEngine(clock)
    network = Network(engine)
    node1 = Node("n1", network, clock)
    return clock, engine, network, node1

def test_node_write_and_read(environment):
    clock, engine, network, node1 = environment
    
    # Send Write
    msg_write = Message(msg_id="w1", src="client", dst="n1", msg_type=MessageType.WRITE_REQ, key="k1", value="v1", version=1)
    network.send(msg_write)
    engine.run()
    
    assert node1.get_state()["k1"] == ("v1", 1)
    
    # We should also capture the ACK sent back to client
    received_msgs = []
    network.register_node("client", lambda msg: received_msgs.append(msg))
    
    # Send Read
    msg_read = Message(msg_id="r1", src="client", dst="n1", msg_type=MessageType.READ_REQ, key="k1")
    network.send(msg_read)
    engine.run()
    
    assert len(received_msgs) == 1
    resp = received_msgs[0]
    assert resp.msg_type == MessageType.READ_RESP
    assert resp.value == "v1"
    assert resp.version == 1

def test_node_gossip_merge(environment):
    clock, engine, network, node1 = environment
    
    # Initialize with local state
    node1._store["k1"] = ("v1_local", 1)
    node1._store["k2"] = ("v2_local", 2)
    
    # Incoming gossip payload
    gossip_payload = {
        "k1": ("v1_remote", 2),  # Higher version, should overwrite
        "k2": ("v2_remote", 1),  # Lower version, should be ignored
        "k3": ("v3_remote", 3)   # New key, should be added
    }
    
    gossip_msg = Message(msg_id="g1", src="n2", dst="n1", msg_type=MessageType.GOSSIP, key="gossip", payload=gossip_payload)
    network.send(gossip_msg)
    engine.run()
    
    state = node1.get_state()
    assert state["k1"] == ("v1_remote", 2)
    assert state["k2"] == ("v2_local", 2)
    assert state["k3"] == ("v3_remote", 3)
