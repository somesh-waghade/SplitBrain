import os
import sys
import yaml
import click
import random
from splitbrain.core.clock import SimulatedClock
from splitbrain.core.engine import EventEngine
from splitbrain.network.network import Network
from splitbrain.node.node import Node
from splitbrain.coordinator.coordinator import Coordinator
from splitbrain.consistency.strong import StrongConsistency
from splitbrain.consistency.quorum import QuorumConsistency
from splitbrain.consistency.eventual import EventualConsistency
from splitbrain.core.event import Event, EventType
from splitbrain.metrics.metrics import MetricsEngine, RequestRecord
from splitbrain.viz.plotter import Plotter

def run_simulation(config_path: str):
    """Run a single experiment config."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    exp_name = config.get("experiment", "experiment")
    nodes_count = config.get("nodes", 3)
    latency_base = config.get("latency_base_ms", 50)
    latency_jitter = config.get("latency_jitter_ms", 10)
    drop_rate = config.get("drop_rate", 0.0)
    num_writes = config.get("num_writes", 100)
    num_reads = config.get("num_reads", 100)
    seed = config.get("seed", 42)
    partition_enabled = config.get("partition", False)
    partition_at = config.get("partition_at_ms", 500)
    heal_at = config.get("heal_at_ms", 2000)

    random.seed(seed)
    
    clock = SimulatedClock()
    engine = EventEngine(clock)
    network = Network(engine)
    network.set_latency(latency_base, latency_jitter)
    network.set_drop_rate(drop_rate)
    
    nodes = [Node(f"node{i}", network, clock) for i in range(1, nodes_count + 1)]
    coordinator = Coordinator(engine, network, nodes)
    
    node_ids = [n.node_id for n in nodes]
    
    # Register strategies
    coordinator.register_strategy("strong", StrongConsistency(engine, network, node_ids))
    coordinator.register_strategy("quorum", QuorumConsistency(engine, network, node_ids))
    coordinator.register_strategy("eventual", EventualConsistency(engine, network, nodes, gossip_interval_ms=100))

    metrics = MetricsEngine()

    # Schedule partition if enabled
    if partition_enabled:
        
        def partition_cb(e):
            # Isolate node1 from other nodes
            for n in node_ids[1:]:
                network.add_partition("node1", n)
            # Isolate node1 from coordinator
            network.add_partition("node1", "coordinator")
            print(f"[{clock.current_time}ms] Partition isolated node1")
                
        engine.schedule(Event(timestamp=partition_at, event_type=EventType.CLIENT_REQUEST, callback=partition_cb))
        
        def heal_cb(e):
            network.clear_partitions()
            print(f"[{clock.current_time}ms] Partition healed")
            
        engine.schedule(Event(timestamp=heal_at, event_type=EventType.CLIENT_REQUEST, callback=heal_cb))

    print(f"Starting experiment: {exp_name} (seed: {seed})")
    print(f"Nodes: {nodes_count}, Latency: {latency_base}±{latency_jitter}ms, Drop: {drop_rate*100}%")

    current_val = 0
    req_counter = 0

    def generate_workload(model: str, start_time: int):
        nonlocal current_val, req_counter
        # We will stagger requests every 10ms
        time_cursor = start_time
        
        for _ in range(num_writes):
            req_id = f"{model}_w{req_counter}"
            current_val += 1
            val_to_write = f"val_{current_val}"
            req_counter += 1
            
            def make_write_cb(r_id, expected_val, t_start):
                def cb(success):
                    metrics.log_request(RequestRecord(
                        req_id=r_id, key="test_key", operation="write", consistency_model=model,
                        start_time=t_start, end_time=clock.current_time, success=success,
                        value_returned=None, expected_latest_value=expected_val
                    ))
                return cb
                
            def trigger_write(e, r_id=req_id, val=val_to_write, t_start=time_cursor):
                coordinator.write("test_key", val, model, r_id, make_write_cb(r_id, val, t_start))
                
            engine.schedule(Event(timestamp=time_cursor, event_type=EventType.CLIENT_REQUEST, callback=trigger_write))
            time_cursor += 20
            
        for _ in range(num_reads):
            req_id = f"{model}_r{req_counter}"
            req_counter += 1
            expected = f"val_{current_val}" # The last written value
            
            def make_read_cb(r_id, expected_val, t_start):
                def cb(val, success):
                    metrics.log_request(RequestRecord(
                        req_id=r_id, key="test_key", operation="read", consistency_model=model,
                        start_time=t_start, end_time=clock.current_time, success=success,
                        value_returned=val, expected_latest_value=expected_val
                    ))
                return cb
                
            def trigger_read(e, r_id=req_id, expected_val=expected, t_start=time_cursor):
                coordinator.read("test_key", model, r_id, make_read_cb(r_id, expected_val, t_start))
                
            engine.schedule(Event(timestamp=time_cursor, event_type=EventType.CLIENT_REQUEST, callback=trigger_read))
            time_cursor += 20
            
        return time_cursor

    # Sequence workloads to avoid overlap
    t = 0
    for model in ["strong", "quorum", "eventual"]:
        t = generate_workload(model, t)
        t += 1000 # wait 1s between models
        
    # Periodically take state snapshots for convergence tracking
    def snapshot_cb(e):
        metrics.record_state_snapshot(clock.current_time, {n.node_id: n.get_state() for n in nodes})
        if clock.current_time < t + 2000:
            engine.schedule(Event(timestamp=clock.current_time + 100, event_type=EventType.CLIENT_REQUEST, callback=snapshot_cb))
            
    engine.schedule(Event(timestamp=0, event_type=EventType.CLIENT_REQUEST, callback=snapshot_cb))

    # Run simulation
    engine.run(until=t + 2000)

    # Print summary
    print("\n--- Results ---")
    summary = metrics.summary_table()
    print(summary.to_string(index=False))
    
    # Generate plots
    out_dir = os.path.join("experiments", "results", exp_name)
    plotter = Plotter(metrics, out_dir)
    plotter.save_all()
    print(f"\nPlots saved to {out_dir}/")

@click.group()
def cli():
    """SplitBrain Consistency Analyzer CLI."""
    pass

@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True), help='Path to experiment config YAML.')
def run_experiment(config):
    """Run a simulation based on a configuration file."""
    run_simulation(config)

@cli.command()
@click.option('--configs', multiple=True, required=True, type=click.Path(exists=True), help='Paths to config YAMLs.')
def compare(configs):
    """Run multiple experiments and compare them (Not implemented fully yet, runs them sequentially)."""
    for config in configs:
        run_simulation(config)
        print("-" * 40)

if __name__ == '__main__':
    cli()
