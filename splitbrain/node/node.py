from typing import Any, Tuple, Dict
from splitbrain.core.message import Message, MessageType
from splitbrain.network.network import Network
from splitbrain.core.clock import SimulatedClock

class Node:
    """
    A single replica in the distributed key-value store.
    Handles read/write requests and merges state via gossip.
    """
    def __init__(self, node_id: str, network: Network, clock: SimulatedClock):
        self.node_id = node_id
        self.network = network
        self.clock = clock
        # store format: {key: (value, version)}
        self._store: Dict[str, Tuple[Any, int]] = {}
        
        self.network.register_node(self.node_id, self.receive)

    def receive(self, message: Message):
        """Dispatch incoming messages based on type."""
        if message.msg_type == MessageType.READ_REQ:
            self._handle_read_req(message)
        elif message.msg_type == MessageType.WRITE_REQ:
            self._handle_write_req(message)
        elif message.msg_type == MessageType.GOSSIP:
            self._handle_gossip(message)

    def _handle_read_req(self, message: Message):
        """Handle a read request from the coordinator."""
        value, version = self._store.get(message.key, (None, 0))
        resp = Message(
            msg_id=message.msg_id,
            src=self.node_id,
            dst=message.src,
            msg_type=MessageType.READ_RESP,
            key=message.key,
            value=value,
            version=version,
            timestamp=self.clock.current_time
        )
        self.network.send(resp)

    def _handle_write_req(self, message: Message):
        """Handle a write request from the coordinator."""
        # Using the coordinator's provided version (or timestamp) as the new version.
        # Alternatively, the node could generate it, but usually the coordinator drives it.
        # For our simulation, we'll let the coordinator dictate the new version to ensure consistency.
        
        # If the incoming version is strictly greater, update.
        current_val, current_version = self._store.get(message.key, (None, 0))
        if message.version > current_version:
            self._store[message.key] = (message.value, message.version)
        
        ack = Message(
            msg_id=message.msg_id,
            src=self.node_id,
            dst=message.src,
            msg_type=MessageType.WRITE_ACK,
            key=message.key,
            version=self._store[message.key][1], # Respond with what we actually have
            timestamp=self.clock.current_time
        )
        self.network.send(ack)

    def _handle_gossip(self, message: Message):
        """Handle background state synchronization (Last-Write-Wins)."""
        gossip_store = message.payload  # Dict[str, Tuple[Any, int]]
        if not isinstance(gossip_store, dict):
            return

        for key, (g_value, g_version) in gossip_store.items():
            current_val, current_version = self._store.get(key, (None, 0))
            if g_version > current_version:
                self._store[key] = (g_value, g_version)
            elif g_version == current_version and current_val is None:
                 # Resolve conflict if necessary, though versions should ideally be monotonic
                 self._store[key] = (g_value, g_version)

    def get_state(self) -> Dict[str, Tuple[Any, int]]:
        """Return a snapshot of the current state (used for metrics)."""
        return dict(self._store)
