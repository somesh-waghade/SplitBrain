import random
from typing import Callable, Any, Dict, List
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.node.node import Node
from splitbrain.core.message import Message, MessageType
from splitbrain.core.event import Event, EventType

class EventualConsistency:
    """
    Writes require only 1 ACK. Reads contact 1 node (may be stale).
    Uses background gossip to propagate state.
    """
    def __init__(self, engine: EventEngine, network: Network, nodes: List[Node], gossip_interval_ms: int = 100):
        self.engine = engine
        self.network = network
        self.nodes = nodes
        self.node_ids = [n.node_id for n in nodes]
        self.gossip_interval_ms = gossip_interval_ms
        
        self.requests: Dict[str, Dict[str, Any]] = {}
        
        # Start gossip background process
        self._schedule_gossip_tick()

    def read(self, key: str, req_id: str, callback: Callable[[Any, bool], None]) -> None:
        self.requests[req_id] = {
            "type": "read",
            "callback": callback,
            "completed": False
        }
        
        # Pick one random node to read from
        target_node = random.choice(self.node_ids)
        msg = Message(msg_id=req_id, src="coordinator", dst=target_node, msg_type=MessageType.READ_REQ, key=key)
        self.network.send(msg)

    def write(self, key: str, value: Any, req_id: str, callback: Callable[[bool], None]) -> None:
        version = self.engine.clock.current_time
        
        self.requests[req_id] = {
            "type": "write",
            "callback": callback,
            "completed": False
        }
        
        # Pick one random node to write to
        target_node = random.choice(self.node_ids)
        msg = Message(msg_id=req_id, src="coordinator", dst=target_node, msg_type=MessageType.WRITE_REQ, key=key, value=value, version=version)
        self.network.send(msg)

    def receive(self, message: Message) -> None:
        req_state = self.requests.get(message.msg_id)
        if not req_state or req_state["completed"]:
            return

        if message.msg_type == MessageType.READ_RESP and req_state["type"] == "read":
            req_state["completed"] = True
            req_state["callback"](message.value, True)

        elif message.msg_type == MessageType.WRITE_ACK and req_state["type"] == "write":
            req_state["completed"] = True
            req_state["callback"](True)

    def _schedule_gossip_tick(self):
        """Schedule the next background gossip event."""
        def gossip_callback(event: Event):
            self._perform_gossip()
            self._schedule_gossip_tick()
            
        event = Event(
            timestamp=self.engine.clock.current_time + self.gossip_interval_ms,
            event_type=EventType.GOSSIP_TICK,
            callback=gossip_callback
        )
        self.engine.schedule(event)

    def _perform_gossip(self):
        """Randomly pair nodes to exchange state."""
        # Simple gossip: each node picks one other random node to send its state to
        for src_node in self.nodes:
            # We skip picking a destination here to avoid complex state tracking in the coordinator.
            # Instead, the coordinator can directly pull state and push it, OR it can send a message.
            # Let's do it via message to simulate network delay/partition properly.
            dst_id = random.choice([n for n in self.node_ids if n != src_node.node_id])
            state_snapshot = src_node.get_state()
            
            msg = Message(
                msg_id=f"gossip_{self.engine.clock.current_time}_{src_node.node_id}",
                src=src_node.node_id,
                dst=dst_id,
                msg_type=MessageType.GOSSIP,
                key="gossip", # dummy key
                payload=state_snapshot
            )
            self.network.send(msg)
