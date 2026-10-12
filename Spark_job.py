#!/usr/bin/env python3
"""
Detect deadlock cycles in HPC resource allocation graph (RAG) log.

Usage:
    spark-submit deadlock_detect.py /path/to/hpc_rag.log no_of_cores

This script implements:
- RAG edge creation from WAIT/HOLD logs
- MapReduce-style iterative path propagation to detect cycles (max length 25)
- Deadlock formation timeline (first-formation date)

"""

import sys
import re
from datetime import datetime
from operator import add
from pyspark import SparkContext
import time

MAX_CYCLE_LEN = 25

LOG_LINE_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})[\t ]+(\d{2}:\d{2}:\d{2})[\t ]+(\w+)[\t ]+(P\d+)[\t ]+(R\d+)\s*$'
)

def parse_line(line):
    """
    Parse a log line into (timestamp(datetime), level, proc, res)
    Works with both tab and space separated logs.
    """
    parts = line.strip().split()
    if len(parts) != 5:
        return None
    ts_str = parts[0] + " " + parts[1]
    level, proc, res = parts[2], parts[3], parts[4]
    try:
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None
    return (ts, level.upper(), proc.strip(), res.strip())


def edge_from_event(event):
    """
    Convert parsed event to directed edge(s).
    WAIT -> Process -> Resource (P -> R)
    HOLD -> Resource -> Process (R -> P)
    Returns ((src, dst), timestamp)
    """
    ts, level, proc, res = event
    if level == "WAIT":
        return ((proc, res), ts)
    elif level == "HOLD":
        return ((res, proc), ts)
    else:
        return None

def normalize_cycle(cycle):
    """
    Normalize cycle representation for deduplication:
    - rotate cycle so lexicographically smallest node is first
    - represent as tuple
    This ensures same cycle found in different rotations will equal.
    """
    # cycle is tuple/list of nodes where first and last are NOT equal (we will have closed cycle when reporting)
    n = len(cycle)
    # create rotations
    rotations = []
    for i in range(n):
        rot = tuple(cycle[i:] + cycle[:i])
        rotations.append(rot)
    # get lexicographically smallest rotation
    norm = min(rotations)
    return norm

def cycle_sort_key(cycle_tuple):
    # Sorting key: (length, tuple) where tuple is lexicographic
    return (len(cycle_tuple), cycle_tuple)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit deadlock_detect.py /path/to/hpc_rag.log num_cores")
        sys.exit(1)

    log_path = sys.argv[1]
    num_cores = sys.argv[2]

    # Limit Spark to the given number of cores
    sc = SparkContext(master=f"local[{num_cores}]", appName="DeadlockDetectAssignment4")



    # Read and parse lines
    lines = sc.textFile(log_path)

    # Parse lines into events
    parsed = lines.map(lambda l: parse_line(l)) \
                  .filter(lambda x: x is not None)

    # Convert events to directed edges with timestamps
    # Each event -> exactly one directed edge per rules
    edges_with_ts = parsed.map(edge_from_event).filter(lambda x: x is not None)

    # We want unique directed edges E. Also we need the first timestamp (earliest) that edge appeared (for Q3)
    # Map to (edge, ts) then reduceByKey to keep earliest ts
    edge_earliest_ts = edges_with_ts.map(lambda ed: (ed[0], ed[1])) \
                                    .reduceByKey(lambda a, b: a if a <= b else b) \
                                    .cache()

    # Unique edge list
    edges = edge_earliest_ts.keys().cache()  # each is (src, dst)

    # Q1.2 counts
    # Extract nodes and classify
    src_nodes = edges.map(lambda e: e[0])
    dst_nodes = edges.map(lambda e: e[1])

    # Process nodes start with 'P', resource nodes start with 'R'
    processes = edges.flatMap(lambda e: [e[0] if e[0].startswith('P') else None, e[1] if e[1].startswith('P') else None]) \
                    .filter(lambda x: x is not None) \
                    .distinct()

    resources = edges.flatMap(lambda e: [e[0] if e[0].startswith('R') else None, e[1] if e[1].startswith('R') else None]) \
                    .filter(lambda x: x is not None) \
                    .distinct()

    num_process_nodes = processes.count()
    num_resource_nodes = resources.count()
    num_directed_edges = edges.count()

    # Build adjacency list RDD: (node, [neighbors])
    adjacency = edges.map(lambda e: (e[0], e[1])) \
                     .groupByKey() \
                     .mapValues(lambda it: list(set(it))) \
                     .cache()

    # For efficient joins later we need adjacency keyed by node
    # Also create a small map of adjacency collected to driver if needed. But we will implement MapReduce approach using joins.
    adjacency_cached = adjacency.cache()

    # We also need an RDD or dict to map edges -> earliest timestamp (datetime)
    edge_ts_rdd = edge_earliest_ts.map(lambda kv: (kv[0], kv[1]))
    edge_ts = dict(edge_ts_rdd.collect())  # small enough for memory (<=50k edges)

    # Prepare initial paths RDD: each node as starting point; represent path as tuple of nodes
    all_nodes = edges.flatMap(lambda e: [e[0], e[1]]).distinct().cache()

    # Start paths: (start_node, path_tuple)
    paths = all_nodes.map(lambda n: (n, (n,)))  # each starting path of length 1

    # We'll iteratively extend paths up to MAX_CYCLE_LEN steps.
    # At each iteration:
    #  - map paths to (last_node, (start, path))
    #  - join with adjacency (node -> neighbors)
    #  - for each neighbor, if neighbor == start => emit cycle
    #  - else if neighbor not in path => produce new path (start, path + (neighbor,))
    #
    # Because we need MapReduce-like operations we'll use RDD joins (no broadcasting adjacency).

    # Convert adjacency to pair RDD keyed by node for join usage (already adjacency_cached)
    # adjacency_cached: (node, [nbrs])

    current_paths = paths  # (start, path_tuple)
    detected_cycles_rdd = sc.emptyRDD()  # will collect cycles as tuples on driver later by collect()
    # We'll store cycles as normalized tuples to deduplicate in the end
    detected_cycles = sc.parallelize([])  # empty RDD of cycles (tuple)

    start_time = time.time()
    for step in range(1, MAX_CYCLE_LEN + 1):
        # For current_paths, key by last node to join with adjacency
        keyed_by_last = current_paths.map(lambda sp: (sp[1][-1], (sp[0], sp[1])))
        # Join with adjacency to get neighbors of last node
        joined = keyed_by_last.join(adjacency_cached)  # (last_node, ((start, path), neighbors_list))
        # Now extend
        def extend_records(record):
            """
            record: (last_node, ((start, path_tuple), neighbors_list))
            yields: ('cycle', norm_cycle_tuple) or ('path', (start, new_path_tuple))
            """
            last_node, ((start, path_tuple), neighbors) = record
            out = []
            path_set = set(path_tuple)
            for nbr in neighbors:
                if nbr == start:
                    # Found a cycle: path_tuple + (start) forms a closed cycle.
                    # Represent cycle without repeating the end since normalization handles rotation
                    cyc = list(path_tuple)
                    # we do not append start to make sequence canonical (cyc is ring)
                    norm = normalize_cycle(cyc)
                    out.append(("cycle", norm))
                else:
                    # Prevent node repetition
                    if nbr in path_set:
                        continue
                    # Extend path but ensure not exceeding MAX_CYCLE_LEN
                    if len(path_tuple) + 1 <= MAX_CYCLE_LEN:
                        new_path = path_tuple + (nbr,)
                        out.append(("path", (start, new_path)))
            return out

        extended = joined.flatMap(lambda rec: extend_records(rec))
        # Separate cycles and new paths
        cycles_part = extended.filter(lambda x: x[0] == "cycle").map(lambda x: x[1])
        paths_part = extended.filter(lambda x: x[0] == "path").map(lambda x: x[1])

        # Accumulate cycles
        detected_cycles = detected_cycles.union(cycles_part).distinct()

        # Prepare for next iteration
        # paths_part is RDD of (start, path_tuple)
        current_paths = paths_part

        # Stop early if no more paths to extend
        if current_paths.isEmpty():
            break

    
    end_time = time.time()
    total_exec_time_seconds = end_time - start_time    

    # Collect cycles to driver, sort, dedupe (already distinct but ensure)
    cycles_collected = detected_cycles.map(lambda t: tuple(t)).distinct().collect()

    # Each cycle found is a tuple like ('P102', 'R006', 'P159', 'R039', 'P143', 'R031')
    # Ensure we sort cycles first by length then lexicographically (tuple comparison)
    cycles_sorted = sorted(cycles_collected, key=cycle_sort_key)

    # Q2.2: Count cycles
    num_cycles = len(cycles_sorted)

    # Q2.3: All unique processes involved in cycles (alphabetically sorted)
    processes_in_cycles = set()
    for cyc in cycles_sorted:
        for node in cyc:
            if node.startswith('P'):
                processes_in_cycles.add(node)
    processes_in_cycles_sorted = sorted(list(processes_in_cycles))

    # Q3.1 Deadlock formation timeline:
    # For each cycle, determine the date when the cycle was first fully formed,
    # i.e., when the last edge of the cycle appeared in the log.
    # For each cycle we need the earliest timestamp of each edge (src->dst) and then
    # the formation timestamp is max( earliest_ts(edge) over edges in cycle ).
    # We stored edge earliest timestamps in edge_ts dict earlier.
    cycle_formation_dates = []
    for cyc in cycles_sorted:
        # edges in cycle are pairs (cyc[i], cyc[i+1]) for i in 0..n-1 with wrap-around (last->first)
        edge_times = []
        n = len(cyc)
        missing_edge_flag = False
        for i in range(n):
            src = cyc[i]
            dst = cyc[(i+1) % n]
            key = (src, dst)
            if key in edge_ts:
                edge_times.append(edge_ts[key])
            else:
                # Edge not found in the current unique edges (shouldn't happen), mark missing.
                missing_edge_flag = True
                break
        if missing_edge_flag or not edge_times:
            # Skip or mark as unknown (but log likely contains them)
            continue
        formation_ts = max(edge_times)
        formation_date = formation_ts.date().isoformat()
        cycle_formation_dates.append((cyc, formation_date))

    # Count frequency per date
    from collections import Counter
    date_counts = Counter([d for (_, d) in cycle_formation_dates])
    # Sort dates chronologically
    date_counts_sorted = sorted(date_counts.items(), key=lambda x: x[0])

    # Q4: Top 5 resources with highest in-degree (resource nodes have many incoming P->R edges)
    # and Top 5 processes with highest out-degree (process nodes waiting for many resources)
    # Compute degrees using edges RDD
    # in-degree per node
    in_degrees = edges.map(lambda e: (e[1], 1)).reduceByKey(add)  # (node, in_deg)
    out_degrees = edges.map(lambda e: (e[0], 1)).reduceByKey(add)  # (node, out_deg)

    # Filter resources for in-degree top5
    resources_in_deg = in_degrees.filter(lambda kv: kv[0].startswith('R'))
    resources_top5 = resources_in_deg.collect()
    resources_top5 = sorted(resources_top5, key=lambda kv: (-kv[1], kv[0]))[:5]

    # Filter processes for out-degree top5
    processes_out_deg = out_degrees.filter(lambda kv: kv[0].startswith('P'))
    processes_top5 = processes_out_deg.collect()
    processes_top5 = sorted(processes_top5, key=lambda kv: (-kv[1], kv[0]))[:5]

    # Output in required format
    # The sample format expects each metric count on its own line, then cycles, then processes list, etc.
    # We'll print in this order:
    # num_process_nodes
    # num_resource_nodes
    # num_directed_edges
    # total_execution_time_for_cycle_detection (we don't precisely measure Spark job time here, show placeholder)
    # num_cycles
    # cycles (one per line)
    # total unique processes involved
    # list of process ids involved (space separated)
    # formation timeline lines (date count)
    # top 5 resources lines "Rxxx count"
    # top 5 processes lines "Pxxx count"


   

    
    output_file_path = "outputs.txt"
    original_stdout = sys.stdout

    with open(output_file_path, 'w') as f:
        sys.stdout = f
       
        
        print(num_process_nodes)
        
        print(num_resource_nodes)
        
        print(num_directed_edges)
        
        # total execution time in seconds for cycle detection (placeholder)
        print("{:.2f}".format(total_exec_time_seconds))
        
        print(num_cycles)
        
        # Cycles (sorted by length then lexicographically)
        for cyc in cycles_sorted:
            print(" ".join(cyc))
        if num_cycles == 0:
            # To match format, if no cycles, just (no cycles lines)
            pass
        
        # Number of unique processes involved
        print(len(processes_in_cycles_sorted))
        
        if processes_in_cycles_sorted:
            print(" ".join(processes_in_cycles_sorted))
        
        # Deadlock formation timeline
        for date_str, cnt in date_counts_sorted:
            print(f"{date_str} {cnt}")
        if not date_counts_sorted:
            # If none, nothing or blank lines allowed. We leave blank section.
            pass

        # Top 5 resources (R-ID count)
        for node, cnt in resources_top5:
            print(f"{node} {cnt}")
        
        # Top 5 processes (P-ID count)
        for node, cnt in processes_top5:
            print(f"{node} {cnt}")

        sys.stdout = original_stdout    

    sc.stop()
