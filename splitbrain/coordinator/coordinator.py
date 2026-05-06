from typing import Protocol, Callable, Any, Dict, List
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.node.node import Node
from splitbrain.core.message import Message

class ConsistencyStrategy(Protocol):
    def read(self, key: str, req_id: str, callback: Callable[[Any, bool], None]) -> None:
        """
        Initiate a read request.
        callback should be called with (value, success)
        """
        ...

    def write(self, key: str, value: Any, req_id: str, callback: Callable[[bool], None]) -> None:
        """
        Initiate a write request.
        callback should be called with (success,)
        """
        ...
        
    def receive(self, message: Message) -> None:
        """Handle incoming messages related to ongoing requests."""
        ...

class Coordinator:
    """
    Handles client requests and coordinates reads and writes across nodes
    using the specified consistency strategy.
    """
    def __init__(self, engine: EventEngine, network: Network, nodes: List[Node]):
        self.engine = engine
        self.network = network
        self.nodes = nodes
        self.node_ids = [n.node_id for n in nodes]
        
        # Strategy instances (e.g., strong, quorum, eventual)
        self.strategies: Dict[str, ConsistencyStrategy] = {}
        
        # Register the coordinator on the network to receive responses
        self.network.register_node("coordinator", self.receive)

    def register_strategy(self, name: str, strategy: ConsistencyStrategy):
        """Register a consistency model strategy."""
        self.strategies[name] = strategy

    def receive(self, message: Message):
        """Dispatch incoming messages to the active strategies."""
        # Simple dispatch: broadcast to all strategies, they will ignore msgs for req_ids they don't own.
        for strategy in self.strategies.values():
            strategy.receive(message)

    def read(self, key: str, consistency_model: str, req_id: str, callback: Callable[[Any, bool], None]):
        """Initiate a read using a specific consistency model."""
        if consistency_model not in self.strategies:
            raise ValueError(f"Unknown consistency model: {consistency_model}")
        self.strategies[consistency_model].read(key, req_id, callback)

    def write(self, key: str, value: Any, consistency_model: str, req_id: str, callback: Callable[[bool], None]):
        """Initiate a write using a specific consistency model."""
        if consistency_model not in self.strategies:
            raise ValueError(f"Unknown consistency model: {consistency_model}")
        self.strategies[consistency_model].write(key, value, req_id, callback)
