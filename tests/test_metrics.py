from splitbrain.metrics.metrics import MetricsEngine, RequestRecord

def test_metrics_latency():
    engine = MetricsEngine()
    engine.log_request(RequestRecord(
        req_id="1", key="k1", operation="write", consistency_model="strong",
        start_time=0, end_time=100, success=True, value_returned=None
    ))
    engine.log_request(RequestRecord(
        req_id="2", key="k1", operation="write", consistency_model="strong",
        start_time=100, end_time=300, success=True, value_returned=None
    ))
    
    latency = engine.compute_latency()
    assert latency["strong"]["mean"] == 150.0

def test_metrics_stale_reads():
    engine = MetricsEngine()
    
    # Fresh read
    engine.log_request(RequestRecord(
        req_id="1", key="k1", operation="read", consistency_model="eventual",
        start_time=0, end_time=100, success=True, value_returned="v2", expected_latest_value="v2"
    ))
    
    # Stale read
    engine.log_request(RequestRecord(
        req_id="2", key="k1", operation="read", consistency_model="eventual",
        start_time=100, end_time=200, success=True, value_returned="v1", expected_latest_value="v2"
    ))
    
    stale_rates = engine.compute_stale_read_rate()
    assert stale_rates["eventual"] == 50.0

def test_metrics_availability():
    engine = MetricsEngine()
    
    engine.log_request(RequestRecord(
        req_id="1", key="k1", operation="write", consistency_model="quorum",
        start_time=0, end_time=100, success=True, value_returned=None
    ))
    
    engine.log_request(RequestRecord(
        req_id="2", key="k1", operation="write", consistency_model="quorum",
        start_time=100, end_time=200, success=False, value_returned=None
    ))
    
    engine.log_request(RequestRecord(
        req_id="3", key="k1", operation="write", consistency_model="quorum",
        start_time=200, end_time=300, success=True, value_returned=None
    ))
    
    avail = engine.compute_availability()
    assert avail["quorum"] == (2/3) * 100.0
