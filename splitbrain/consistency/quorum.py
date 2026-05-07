from typing import Callable, Any, Dict, List
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.core.message import Message, MessageType
from splitbrain.core.event import Event, EventType

class QuorumConsistency:
    """
    Requires acknowledgment from a MAJORITY of replicas.
    """
    def __init__(self, engine: EventEngine, network: Network, node_ids: List[str], timeout_ms: int = 2000):
        self.engine = engine
        self.network = network
        self.node_ids = node_ids
        self.quorum_size = (len(node_ids) // 2) + 1
        self.timeout_ms = timeout_ms
        
        self.requests: Dict[str, Dict[str, Any]] = {}

    def read(self, key: str, req_id: str, callback: Callable[[Any, bool], None]) -> None:
        self.requests[req_id] = {
            "type": "read",
            "acks": set(),
            "responses": [],
            "callback": callback,
            "completed": False
        }
        
        for node_id in self.node_ids:
            msg = Message(msg_id=req_id, src="coordinator", dst=node_id, msg_type=MessageType.READ_REQ, key=key)
            self.network.send(msg)
            
        self._schedule_timeout(req_id)

    def write(self, key: str, value: Any, req_id: str, callback: Callable[[bool], None]) -> None:
        version = self.engine.clock.current_time
        
        self.requests[req_id] = {
            "type": "write",
            "acks": set(),
            "callback": callback,
            "completed": False
        }
        
        for node_id in self.node_ids:
            msg = Message(msg_id=req_id, src="coordinator", dst=node_id, msg_type=MessageType.WRITE_REQ, key=key, value=value, version=version)
            self.network.send(msg)
            
        self._schedule_timeout(req_id)

    def receive(self, message: Message) -> None:
        req_state = self.requests.get(message.msg_id)
        if not req_state or req_state["completed"]:
            return

        if message.msg_type == MessageType.READ_RESP and req_state["type"] == "read":
            req_state["acks"].add(message.src)
            req_state["responses"].append(message)
            if len(req_state["acks"]) >= self.quorum_size:
                req_state["completed"] = True
                highest_resp = max(req_state["responses"], key=lambda x: x.version)
                req_state["callback"](highest_resp.value, True)

        elif message.msg_type == MessageType.WRITE_ACK and req_state["type"] == "write":
            req_state["acks"].add(message.src)
            if len(req_state["acks"]) >= self.quorum_size:
                req_state["completed"] = True
                req_state["callback"](True)

    def _schedule_timeout(self, req_id: str):
        def timeout_callback(event: Event):
            req_state = self.requests.get(req_id)
            if req_state and not req_state["completed"]:
                req_state["completed"] = True
                if req_state["type"] == "read":
                    req_state["callback"](None, False)
                else:
                    req_state["callback"](False)
                    
        event = Event(
            timestamp=self.engine.clock.current_time + self.timeout_ms,
            event_type=EventType.CLIENT_REQUEST,
            callback=timeout_callback
        )
        self.engine.schedule(event)
