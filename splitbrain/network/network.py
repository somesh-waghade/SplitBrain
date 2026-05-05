import random
from typing import Callable
from splitbrain.core.engine import EventEngine
from splitbrain.core.event import Event, EventType
from splitbrain.core.message import Message

class Network:
    """
    Simulates the network layer, responsible for message delivery,
    latency injection, packet drops, and network partitions.
    """
    def __init__(self, engine: EventEngine):
        self.engine = engine
        self._latency_base_ms = 0
        self._latency_jitter_ms = 0
        self._drop_rate = 0.0
        
        # Partitions are represented as a set of sets (or frozensets) of size 2
        # e.g., {frozenset({'node1', 'node2'})} means node1 and node2 cannot communicate
        self._partitions: set[frozenset[str]] = set()
        
        # A registry of node endpoints (node_id -> delivery callback)
        self._endpoints: dict[str, Callable[[Message], None]] = {}

    def register_node(self, node_id: str, delivery_callback: Callable[[Message], None]):
        """Register a node to receive messages on the network."""
        self._endpoints[node_id] = delivery_callback

    def set_latency(self, base_ms: int, jitter_ms: int = 0):
        """Configure latency model."""
        self._latency_base_ms = base_ms
        self._latency_jitter_ms = jitter_ms

    def set_drop_rate(self, probability: float):
        """Configure random packet drop probability (0.0 to 1.0)."""
        self._drop_rate = max(0.0, min(1.0, probability))

    def add_partition(self, node_a: str, node_b: str):
        """Block communication between node_a and node_b."""
        self._partitions.add(frozenset([node_a, node_b]))

    def remove_partition(self, node_a: str, node_b: str):
        """Restore communication between node_a and node_b."""
        self._partitions.discard(frozenset([node_a, node_b]))

    def clear_partitions(self):
        """Remove all network partitions."""
        self._partitions.clear()

    def is_reachable(self, src: str, dst: str) -> bool:
        """Check if communication is possible between src and dst."""
        return frozenset([src, dst]) not in self._partitions

    def send(self, message: Message):
        """
        Send a message through the network.
        It evaluates drop probability, checks partitions, calculates latency,
        and schedules a DELIVER_MESSAGE event in the EventEngine.
        """
        # 1. Check if destination exists
        if message.dst not in self._endpoints and message.dst != "client":
            # For simplicity, if dst is "client" and it's not registered, we just drop it or log it.
            # In a real system, the client might be handled separately by the coordinator.
            # We'll allow the coordinator to register a pseudo-node for the client if needed.
            pass

        # 2. Check network partitions
        if not self.is_reachable(message.src, message.dst):
            return  # Message blocked by partition

        # 3. Check packet drop probability
        if self._drop_rate > 0.0 and random.random() < self._drop_rate:
            return  # Message dropped

        # 4. Calculate latency
        jitter = random.randint(0, self._latency_jitter_ms)
        delay = self._latency_base_ms + jitter
        delivery_time = self.engine.clock.current_time + delay

        # 5. Schedule delivery event
        def deliver_callback(event: Event):
            msg = event.payload
            if msg.dst in self._endpoints:
                self._endpoints[msg.dst](msg)

        event = Event(
            timestamp=delivery_time,
            event_type=EventType.DELIVER_MESSAGE,
            callback=deliver_callback,
            payload=message
        )
        self.engine.schedule(event)
