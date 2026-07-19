import csv
import requests
import time

TARGET_URL = "http://localhost:8428/api/v1/write" 

def csv_to_timeseries(file_path):
    print(f"Streaming {file_path} over a 15-minute window...")
    
    # 15 minutes = 900 seconds. Batch size calculation for 1M rows spread across 900 seconds[cite: 1]:
    BATCH_SIZE = 1111 
    
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        
        counter = 0
        start_time = time.time()
        
        for row in reader:
            if not row:
                continue
                
            # Mapping columns according to the Alibaba dataset schema[cite: 4]
            machine_id = row[0]
            cpu_util = float(row[2]) if row[2] else 0.0
            mem_util = float(row[3]) if row[3] else 0.0
            
            # Using current local timestamp in milliseconds for live replay tracking[cite: 1]
            current_timestamp_ms = int(time.time() * 1000)
            
            payload = f"alibaba_cpu_util_percent{{machine_id=\"{machine_id}\"}} {cpu_util} {current_timestamp_ms}\n" \
                      f"alibaba_mem_util_percent{{machine_id=\"{machine_id}\"}} {mem_util} {current_timestamp_ms}\n"
            
            try:
                requests.post(TARGET_URL, data=payload)
            except Exception:
                pass # Suppress connection exceptions during the pilot smoke test[cite: 1]
                
            counter += 1
            
            # Rate limiting execution flow every 1111 samples
            if counter >= BATCH_SIZE:
                elapsed_time = time.time() - start_time
                # If the batch was sent faster than 1 second, sleep for the remaining duration
                if elapsed_time < 1.0:
                    time.sleep(1.0 - elapsed_time)
                
                # Reset metrics tracking counters for the next 1-second interval
                counter = 0
                start_time = time.time()

if __name__ == "__main__":
    csv_to_timeseries("data/machine_usage_mini.csv")