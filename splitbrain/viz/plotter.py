import os
import matplotlib.pyplot as plt
import pandas as pd
from splitbrain.metrics.metrics import MetricsEngine

class Plotter:
    def __init__(self, metrics: MetricsEngine, output_dir: str):
        self.metrics = metrics
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def plot_latency_timeline(self):
        """Line graph of latency over simulated time."""
        df = self.metrics.get_records_df()
        df_success = df[df["success"] == True].copy()
        if df_success.empty:
            return
            
        df_success["latency"] = df_success["end_time"] - df_success["start_time"]
        
        plt.figure(figsize=(10, 6))
        for model in df_success["consistency_model"].unique():
            model_df = df_success[df_success["consistency_model"] == model]
            # Use start_time as the x-axis
            plt.plot(model_df["start_time"], model_df["latency"], marker='o', linestyle='-', label=model, alpha=0.7)
            
        plt.title('Latency over Time')
        plt.xlabel('Simulated Time (ms)')
        plt.ylabel('Latency (ms)')
        plt.legend()
        plt.grid(True)
        plt.savefig(os.path.join(self.output_dir, 'latency_timeline.png'))
        plt.close()

    def plot_stale_reads(self):
        """Bar chart of stale read % per model."""
        stale_rates = self.metrics.compute_stale_read_rate()
        if not stale_rates:
            return
            
        models = list(stale_rates.keys())
        rates = list(stale_rates.values())
        
        plt.figure(figsize=(8, 6))
        bars = plt.bar(models, rates, color=['#ff9999','#66b3ff','#99ff99'])
        plt.title('Stale Reads Percentage')
        plt.ylabel('Stale Reads (%)')
        plt.ylim(0, max(100, max(rates) + 10 if rates else 100))
        
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.1f}%', va='bottom', ha='center')
            
        plt.savefig(os.path.join(self.output_dir, 'stale_reads.png'))
        plt.close()

    def plot_availability(self):
        """Bar chart of availability % per model."""
        avail_rates = self.metrics.compute_availability()
        if not avail_rates:
            return
            
        models = list(avail_rates.keys())
        rates = list(avail_rates.values())
        
        plt.figure(figsize=(8, 6))
        bars = plt.bar(models, rates, color=['#ffcc99','#c2c2f0','#ffb3e6'])
        plt.title('System Availability')
        plt.ylabel('Successful Requests (%)')
        plt.ylim(0, 110) # 0 to 100 with some headroom
        
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.1f}%', va='bottom', ha='center')
            
        plt.savefig(os.path.join(self.output_dir, 'availability.png'))
        plt.close()

    def plot_convergence(self):
        """Plot a step chart showing when all nodes converged."""
        # This one is tricky. Let's just output text for now or a simple visualization
        # We can plot the number of divergent keys over time.
        timeline = self.metrics.state_timeline
        if not timeline:
            return
            
        times = []
        divergent_counts = []
        
        for snapshot in timeline:
            t = snapshot["timestamp"]
            states = snapshot["states"]
            if not states:
                continue
                
            node_ids = list(states.keys())
            first_node_state = states[node_ids[0]]
            
            divergent = 0
            for k, (v, ver) in first_node_state.items():
                for node_id in node_ids[1:]:
                    if states[node_id].get(k) != (v, ver):
                        divergent += 1
                        break # count per key
                        
            times.append(t)
            divergent_counts.append(divergent)
            
        plt.figure(figsize=(10, 6))
        plt.step(times, divergent_counts, where='post')
        plt.title('Divergent Keys Over Time')
        plt.xlabel('Simulated Time (ms)')
        plt.ylabel('Number of Divergent Keys')
        plt.grid(True)
        plt.savefig(os.path.join(self.output_dir, 'convergence.png'))
        plt.close()

    def save_all(self):
        """Generate and save all plots."""
        self.plot_latency_timeline()
        self.plot_stale_reads()
        self.plot_availability()
        self.plot_convergence()
