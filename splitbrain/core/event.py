from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Any, Optional

class EventType(Enum):
    """Types of events that can occur in the simulation."""
    DELIVER_MESSAGE = auto()
    CLIENT_REQUEST = auto()
    GOSSIP_TICK = auto()
    PARTITION_START = auto()
    PARTITION_HEAL = auto()

@dataclass(order=True)
class Event:
    """
    Represents an event in the simulation.
    Events are ordered by timestamp first. The id ensures stable sorting
    for events with the same timestamp.
    """
    timestamp: int
    event_id: int = field(init=False)
    event_type: EventType = field(compare=False)
    callback: Callable[['Event'], None] = field(compare=False)
    payload: Any = field(default=None, compare=False)

    # Class-level counter for stable tie-breaking
    _id_counter: int = field(default=0, init=False, repr=False, compare=False)

    def __post_init__(self):
        self.event_id = Event._id_counter
        Event._id_counter += 1
