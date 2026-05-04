class SimulatedClock:
    """
    A deterministic simulated clock that tracks time in milliseconds.
    """
    def __init__(self):
        self._current_time = 0

    @property
    def current_time(self) -> int:
        """Get the current simulated time in milliseconds."""
        return self._current_time

    def advance(self, delta_ms: int):
        """Advance the simulated clock by delta_ms."""
        if delta_ms < 0:
            raise ValueError("Time cannot move backward")
        self._current_time += delta_ms

    def set_time(self, new_time_ms: int):
        """Jump the clock forward to a specific time."""
        if new_time_ms < self._current_time:
            raise ValueError("Time cannot move backward")
        self._current_time = new_time_ms

    def reset(self):
        """Reset the clock to 0."""
        self._current_time = 0
