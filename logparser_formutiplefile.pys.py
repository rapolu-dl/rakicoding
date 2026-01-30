import os
from multiprocessing import Pool, cpu_count

def parse_large_file(filepath, buffer_size=1024*1024):
    """
    Worker Function: Runs on a single core.
    Memory Efficient: Uses generators to never load the full file.
    """
    results = [] # In a real system, you might push to a database here
    try:
        # Optimization: Large buffer for faster disk I/O
        with open(filepath, 'r', buffering=buffer_size, encoding='utf-8', errors='replace') as file:
            for line in file:
                # Logic: We only care about errors
                if "ERROR" in line:
                  results.append(f"{filepath} -> {line.strip()}")
        return results
    except Exception as e:
        return [f"FAILED: {filepath} - {e}"]

def main():
    # Imagine this list has 1,000 log files from the cluster
    log_files = [f"/lib/logs/node_{i}.log" for i in range(1,10)]
    
    # STRATEGY: Use ALL available cores (Beast Mode)
    # This bypasses the GIL by creating separate processes
    num_workers = cpu_count() - 1 
    print(f"🔥 Starting Aggregator with {num_workers} processes...")

    with Pool(processes=num_workers) as pool:
        # Optimization: imap_unordered returns results AS SOON as they finish.
        # It prevents waiting for the "slowest" file.
        for file_result in pool.imap_unordered(parse_large_file, log_files):
            for error in file_result:
                print(f"Alert: {error}")

if __name__ == "__main__":
  main()

  #/lib/logs/node_1.log

