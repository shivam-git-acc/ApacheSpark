# README.md

## CS3.304 - Advanced Operating Systems: Assignment 4
### Deadlock Detection using MapReduce (PySpark)

**Submitted by:** Shivam Patel  
**Roll No:** 2035202030

---

## Overview

This assignment implements a scalable deadlock detection algorithm for distributed systems using PySpark's MapReduce framework. The solution analyzes resource allocation logs to construct a Resource Allocation Graph (RAG) and detects cycles through iterative path propagation (maximum cycle length: 25).

---

## Project Structure

```
2035202030_Assignment4/
├── AOS_Assignment4.py    # Main PySpark implementation
├── Report.pdf            # Performance analysis and observations
└── README.md             # This file
```

---

## Environment Setup

### Prerequisites
- Python: 3.10.12
- Apache Spark: 3.4.1
- Java: 11.0.27

### Installation Steps

1. **Create and activate virtual environment:**
```bash
python3 -m venv ~/pyspark_env
source ~/pyspark_env/bin/activate
```

2. **Install dependencies:**
```bash
pip install --upgrade pip
pip install pyspark==3.2.4 findspark
```

3. **Verify installation:**
```bash
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
```

---

## Execution Instructions

### Using SLURM (Cluster Execution)

**Submit job using batch script:**
```bash
sbatch job_submitter.sh
```

**Or run directly with srun:**
```bash
srun --partition=u22 --mem=12G --cpus-per-task=4 \
  spark-submit AOS_Assignment4.py ../test_files/hpc_rag.log 4
```

### Local Testing (Without Cluster)

```bash
python3 AOS_Assignment4.py ../test_files/hpc_rag.log 4
```

**Command format:**
```bash
spark-submit AOS_Assignment4.py <log_file_path> <num_cores>
```

- `<log_file_path>`: Path to input log file (e.g., `../test_files/hpc_rag.log`)
- `<num_cores>`: Number of CPU cores for parallel processing

---

## Implementation Details

### Technical Architecture

The solution implements a **MapReduce-based iterative path propagation algorithm** using PySpark RDDs for distributed deadlock detection.

#### 1. **RAG Edge Construction (Map Phase)**
- **Input parsing:** Regex-based log line parsing extracts timestamp, event type (WAIT/HOLD), process ID, and resource ID
- **Edge generation rules:**
  - `WAIT` events → Process-to-Resource edges (P → R)
  - `HOLD` events → Resource-to-Process edges (R → P)
- **Deduplication:** `reduceByKey()` eliminates duplicate edges while preserving earliest timestamps for timeline analysis

#### 2. **Graph Representation**
- **Adjacency List:** RDD of `(node, [neighbors])` pairs cached for efficient join operations
- **Distributed storage:** Edge timestamps stored as `{(src, dst): datetime}` dictionary for O(1) cycle formation lookup

#### 3. **Cycle Detection via Iterative MapReduce**

**Core Algorithm:**
```
For each iteration (max 25 steps):
  1. MAP: Key paths by last node → (last_node, (start, path))
  2. JOIN: Merge with adjacency list → (node, ((start, path), neighbors))
  3. REDUCE: Extend paths or detect cycles
     - If neighbor == start → emit normalized cycle
     - If neighbor not in path → extend path
  4. FILTER: Separate cycles from active paths
  5. UNION: Accumulate detected cycles with deduplication
```

**Key Parallelism Techniques:**
- **RDD joins** enable distributed neighbor lookup without broadcasting large adjacency data
- **Path deduplication** via `distinct()` prevents redundant computation
- **Early termination** when no paths remain (`isEmpty()` check)

**Cycle Normalization:**
- Rotates cycle to start with lexicographically smallest node
- Ensures identical cycles detected from different starting points are recognized as duplicates
- Example: `(P2, R1, P1, R2)` and `(P1, R2, P2, R1)` → both normalize to `(P1, R2, P2, R1)`

#### 4. **Performance Optimizations**
- **Caching strategy:** Frequently accessed RDDs (edges, adjacency) cached in memory
- **Lazy evaluation:** Spark's DAG optimization delays computation until `collect()` or actions
- **Partition-aware processing:** Workload distributed across specified number of cores

---

## Dataset Information

**Input:** `hpc_rag.log` (up to 100,000 records tested)  
**Format:** `TIMESTAMP LEVEL PROCESS_ID RESOURCE_ID`

**Example:**
```
2025-10-25 00:28:37 WAIT P105 R024
2025-10-25 00:28:44 HOLD P110 R009
```

---

## Output Format

The program generates `outputs.txt` with the following structure:

```
<Total process nodes>
<Total resource nodes>
<Total directed edges>
<Execution time in seconds>
<Number of deadlock cycles>
<Cycle 1: space-separated node sequence>
<Cycle 2: ...>
...
<Total unique processes in deadlocks>
<Space-separated process IDs (alphabetically sorted)>
<Date 1> <Frequency>
<Date 2> <Frequency>
...
<Top 5 resource IDs with in-degree counts>
<Top 5 process IDs with out-degree counts>
```

---

## Performance Analysis Summary

**Key Findings (from Report.pdf):**

- **Optimal core configuration:** 5-6 cores for large datasets (100k records)
- **Speedup characteristics:** Sub-linear due to Spark overhead (serialization, shuffling, synchronization)
- **Small dataset behavior:** Execution time increases with more cores due to parallelization overhead
- **Scalability threshold:** Diminishing returns beyond 6-7 cores

**Execution Time Observations:**
- 10k records with 1 core: 48.00s
- 100k records with 5 cores: 77.39s (best performance)
- Non-linear behavior caused by task scheduling and I/O overhead

---

## Key Features

✅ **MapReduce cycle detection** with distributed path propagation  
✅ **Cycle normalization** for accurate deduplication  
✅ **Timeline analysis** identifying when deadlocks first formed  
✅ **Graph metrics** (in-degree, out-degree hotspot analysis)  
✅ **Scalable architecture** supporting up to 100k+ records  
✅ **Configurable parallelism** via command-line core specification  

---

## Notes

- **No hardcoded paths:** All file paths passed as command-line arguments
- **Deterministic output:** Lexicographic sorting ensures reproducible results
- **Output redirection:** Results written to `outputs.txt` for automated verification

---