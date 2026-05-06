import pytest
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.engine import EventEngine
from splitbrain.core.message import Message, MessageType
from splitbrain.network.network import Network

@pytest.fixture
def engine():
    clock = SimulatedClock()
    return EventEngine(clock)

@pytest.fixture
def network(engine):
    return Network(engine)

def test_network_latency(engine, network):
    network.set_latency(base_ms=50, jitter_ms=0)
    
    received_msgs = []
    network.register_node("nodeA", lambda msg: received_msgs.append((engine.clock.current_time, msg)))
    
    msg = Message(msg_id="1", src="nodeB", dst="nodeA", msg_type=MessageType.READ_REQ, key="k1")
    network.send(msg)
    
    engine.run()
    
    assert len(received_msgs) == 1
    delivery_time, received_msg = received_msgs[0]
    assert delivery_time == 50
    assert received_msg.msg_id == "1"

def test_network_drop_rate(engine, network):
    network.set_latency(base_ms=10, jitter_ms=0)
    network.set_drop_rate(1.0)  # Drop everything
    
    received_msgs = []
    network.register_node("nodeA", lambda msg: received_msgs.append(msg))
    
    msg = Message(msg_id="1", src="nodeB", dst="nodeA", msg_type=MessageType.READ_REQ, key="k1")
    network.send(msg)
    
    engine.run()
    
    assert len(received_msgs) == 0  # Should be dropped

def test_network_partition(engine, network):
    network.set_latency(base_ms=10, jitter_ms=0)
    
    received_msgs = []
    network.register_node("nodeA", lambda msg: received_msgs.append(msg))
    
    network.add_partition("nodeA", "nodeB")
    
    # Message from B to A should be blocked
    msg = Message(msg_id="1", src="nodeB", dst="nodeA", msg_type=MessageType.READ_REQ, key="k1")
    network.send(msg)
    
    engine.run()
    
    assert len(received_msgs) == 0
    
    # Remove partition and try again
    network.remove_partition("nodeA", "nodeB")
    msg2 = Message(msg_id="2", src="nodeB", dst="nodeA", msg_type=MessageType.READ_REQ, key="k1")
    network.send(msg2)
    
    engine.run()
    
    assert len(received_msgs) == 1
    assert received_msgs[0].msg_id == "2"
