# SplitBrain Configuration Guide

The `experiments/configs/` directory contains YAML configuration files used to define the conditions for different distributed systems experiments. These files dictate network behavior, the scale of the system, the workload, and failure scenarios like network partitions.

## Included Configurations

The repository comes with the following pre-defined scenarios:

1. **`high_latency.yaml`**: Simulates a geographically distributed network with high base latency and significant jitter (fluctuations in latency).
2. **`packet_loss.yaml`**: Simulates an unreliable network where a percentage of messages are dropped in transit.
3. **`partition.yaml`**: Simulates a split-brain scenario. It isolates a node (or set of nodes) from the rest of the cluster at a specific time and then heals the partition later.

---

## Configuration Variables

Here is a breakdown of all the variables you can define in a `.yaml` configuration file:

### General Settings
* **`experiment`** (`string`): The name of the experiment. This name is used to identify the experiment and name the output directory in `experiments/results/`.
* **`nodes`** (`int`, default: `3`): The total number of nodes in the simulated cluster.
* **`seed`** (`int`, default: `42`): A random seed to ensure the simulation is deterministic and reproducible.

### Network Settings
* **`latency_base_ms`** (`int`, default: `50`): The base network latency in milliseconds.
* **`latency_jitter_ms`** (`int`, default: `10`): The maximum random variation (jitter) added to the base latency. For example, if base is 50 and jitter is 10, actual latency will be between 40ms and 60ms.
* **`drop_rate`** (`float`, default: `0.0`): The probability (from 0.0 to 1.0) that any given message sent across the network is dropped.

### Workload Settings
* **`num_writes`** (`int`, default: `100`): The number of write requests to simulate per consistency model.
* **`num_reads`** (`int`, default: `100`): The number of read requests to simulate per consistency model.

### Failure/Partition Settings
* **`partition`** (`boolean`, default: `false`): Set to `true` to enable network partition simulation.
* **`partition_at_ms`** (`int`, default: `500`): The simulated time (in milliseconds) when the network partition should occur. Requires `partition: true`.
* **`heal_at_ms`** (`int`, default: `2000`): The simulated time (in milliseconds) when the network partition should heal. Requires `partition: true`.

---

## How to Run a Configuration

You can execute any configuration file using the CLI provided by `runner.py`.

### Running a Single Configuration

To run one of the included experiments, use the `run-experiment` command:

```bash
# Run the high latency scenario
python runner.py run-experiment --config experiments/configs/high_latency.yaml

# Run the packet loss scenario
python runner.py run-experiment --config experiments/configs/packet_loss.yaml

# Run the partition scenario
python runner.py run-experiment --config experiments/configs/partition.yaml
```

### Running Multiple Configurations

You can also run multiple scenarios consecutively using the `compare` command:

```bash
python runner.py compare \
  --configs experiments/configs/high_latency.yaml \
  --configs experiments/configs/packet_loss.yaml \
  --configs experiments/configs/partition.yaml
```

Once execution is complete, the results and plots will be saved in the `experiments/results/` directory, organized by the `experiment` name defined in each YAML file.
