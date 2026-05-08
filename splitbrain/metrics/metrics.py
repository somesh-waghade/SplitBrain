from dataclasses import dataclass
from typing import List, Dict, Any
import pandas as pd
import numpy as np

@dataclass
class RequestRecord:
    req_id: str
    key: str
    operation: str # "read" or "write"
    consistency_model: str
    start_time: int
    end_time: int
    success: bool
    value_returned: Any
    expected_latest_value: Any = None

class MetricsEngine:
    """
    Collects and computes system-level metrics.
    """
    def __init__(self):
        self.records: List[RequestRecord] = []
        # Timeline of node states for convergence tracking
        self.state_timeline: List[Dict[str, Any]] = []

    def log_request(self, record: RequestRecord):
        self.records.append(record)

    def record_state_snapshot(self, timestamp: int, states: Dict[str, Dict[str, Any]]):
        """Record the state of all nodes at a given timestamp."""
        self.state_timeline.append({
            "timestamp": timestamp,
            "states": states
        })

    def get_records_df(self) -> pd.DataFrame:
        return pd.DataFrame([vars(r) for r in self.records])

    def compute_latency(self) -> Dict[str, Dict[str, float]]:
        """Returns latency stats grouped by consistency model."""
        if not self.records:
            return {}
        
        df = self.get_records_df()
        # Only consider successful requests for latency
        df_success = df[df["success"] == True].copy()
        if df_success.empty:
            return {}
            
        df_success["latency"] = df_success["end_time"] - df_success["start_time"]
        
        stats = {}
        for model in df_success["consistency_model"].unique():
            model_df = df_success[df_success["consistency_model"] == model]
            stats[model] = {
                "mean": model_df["latency"].mean(),
                "p50": model_df["latency"].quantile(0.50),
                "p95": model_df["latency"].quantile(0.95),
                "p99": model_df["latency"].quantile(0.99),
            }
        return stats

    def compute_stale_read_rate(self) -> Dict[str, float]:
        """Returns % of stale reads per consistency model."""
        if not self.records:
            return {}
            
        df = self.get_records_df()
        df_reads = df[(df["operation"] == "read") & (df["success"] == True)]
        
        stats = {}
        for model in df["consistency_model"].unique():
            model_reads = df_reads[df_reads["consistency_model"] == model]
            if model_reads.empty:
                stats[model] = 0.0
                continue
            
            stale_count = sum(1 for _, row in model_reads.iterrows() if row["value_returned"] != row["expected_latest_value"])
            stats[model] = (stale_count / len(model_reads)) * 100.0
            
        return stats

    def compute_availability(self) -> Dict[str, float]:
        """Returns % of successful requests per consistency model."""
        if not self.records:
            return {}
            
        df = self.get_records_df()
        stats = {}
        for model in df["consistency_model"].unique():
            model_reqs = df[df["consistency_model"] == model]
            if model_reqs.empty:
                stats[model] = 0.0
                continue
                
            success_count = model_reqs["success"].sum()
            stats[model] = (success_count / len(model_reqs)) * 100.0
            
        return stats

    def compute_convergence_time(self) -> float:
        """
        Returns the time taken for all nodes to agree on all keys.
        Returns -1 if they never converge.
        """
        # Very simple convergence check: scan timeline backwards
        if not self.state_timeline:
            return 0.0
            
        converged_time = -1
        for snapshot in reversed(self.state_timeline):
            states = snapshot["states"] # node_id -> {key: (val, ver)}
            if not states:
                continue
                
            node_ids = list(states.keys())
            first_node_state = states[node_ids[0]]
            
            all_match = True
            for node_id in node_ids[1:]:
                if states[node_id] != first_node_state:
                    all_match = False
                    break
                    
            if all_match:
                converged_time = snapshot["timestamp"]
            else:
                break # We found the point where they diverged
                
        # Return the earliest time they were converged at the end
        return float(converged_time)

    def summary_table(self) -> pd.DataFrame:
        """Combine all metrics into a single summary DataFrame."""
        latency = self.compute_latency()
        stale = self.compute_stale_read_rate()
        avail = self.compute_availability()
        
        models = set(latency.keys()) | set(stale.keys()) | set(avail.keys())
        
        rows = []
        for m in models:
            lat = latency.get(m, {})
            rows.append({
                "Model": m,
                "Latency (mean)": lat.get("mean", float('nan')),
                "Latency (p95)": lat.get("p95", float('nan')),
                "Stale Reads (%)": stale.get(m, float('nan')),
                "Availability (%)": avail.get(m, float('nan'))
            })
            
        return pd.DataFrame(rows)
