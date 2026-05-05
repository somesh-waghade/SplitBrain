import heapq
from typing import Optional
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.event import Event

class EventEngine:
    """
    Core event engine that drives the deterministic simulation.
    It maintains a min-heap of events ordered by timestamp.
    """
    def __init__(self, clock: SimulatedClock):
        self.clock = clock
        self._events: list[Event] = []

    def schedule(self, event: Event):
        """Schedule an event to be processed in the future."""
        if event.timestamp < self.clock.current_time:
            raise ValueError("Cannot schedule event in the past")
        heapq.heappush(self._events, event)

    def step(self) -> bool:
        """
        Process the next event in the queue.
        Returns True if an event was processed, False if queue is empty.
        """
        if not self._events:
            return False

        # Pop the next event
        event = heapq.heappop(self._events)
        
        # Advance clock to event time
        self.clock.set_time(event.timestamp)
        
        # Execute the event callback
        event.callback(event)
        
        return True

    def run(self, until: Optional[int] = None):
        """
        Run the simulation until the queue is empty, or until
        the simulated clock reaches the 'until' timestamp.
        """
        while self._events:
            # Peek at next event time
            next_time = self._events[0].timestamp
            if until is not None and next_time > until:
                # Advance clock to 'until' time and stop processing
                self.clock.set_time(until)
                break
            
            self.step()
