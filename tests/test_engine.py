import pytest
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.event import Event, EventType
from splitbrain.core.engine import EventEngine

def test_clock_advance():
    clock = SimulatedClock()
    assert clock.current_time == 0
    clock.advance(100)
    assert clock.current_time == 100
    
    with pytest.raises(ValueError):
        clock.advance(-10)

def test_event_engine_ordering():
    clock = SimulatedClock()
    engine = EventEngine(clock)
    
    results = []
    def callback(event: Event):
        results.append(event.payload)

    # Schedule out of order
    engine.schedule(Event(timestamp=300, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="third"))
    engine.schedule(Event(timestamp=100, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="first"))
    engine.schedule(Event(timestamp=200, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="second"))
    
    engine.run()
    
    assert results == ["first", "second", "third"]
    assert clock.current_time == 300

def test_event_engine_run_until():
    clock = SimulatedClock()
    engine = EventEngine(clock)
    
    results = []
    def callback(event: Event):
        results.append(event.payload)

    engine.schedule(Event(timestamp=100, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="1"))
    engine.schedule(Event(timestamp=200, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="2"))
    engine.schedule(Event(timestamp=300, event_type=EventType.CLIENT_REQUEST, callback=callback, payload="3"))
    
    engine.run(until=250)
    
    assert results == ["1", "2"]
    assert clock.current_time == 250
    assert len(engine._events) == 1  # event "3" is still in queue
