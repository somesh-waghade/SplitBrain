from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

class MessageType(Enum):
    """Types of messages exchanged between nodes."""
    READ_REQ = auto()
    READ_RESP = auto()
    WRITE_REQ = auto()
    WRITE_ACK = auto()
    GOSSIP = auto()
    GOSSIP_ACK = auto()

@dataclass
class Message:
    """
    Represents a network message sent between nodes or client and coordinator.
    """
    msg_id: str
    src: str
    dst: str
    msg_type: MessageType
    key: str
    value: Optional[Any] = None
    timestamp: Optional[int] = None
    version: int = 0
    payload: Any = None
