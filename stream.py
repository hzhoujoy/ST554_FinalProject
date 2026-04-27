######################################
## ST554_FinalProject: #Streaming Part
## Author: Joy Zhou
## Data: 4/25/2026
######################################

import pandas as pd
import numpy as np
import time
import os

#Using base_dir to ensure that paths are relative to the project root
base_dir = os.path.dirname(os.path.abspath(__file__))

#Use os.path.join() to construct portable file paths
#Path to the input CSV file
file_path = os.path.join(base_dir, "power_streaming_data.csv")
#Directory where output CSV files will be stored
output_dir = os.path.join(base_dir, "csv_files")

#Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)
print(f"Directory ready: {output_dir}")

#Read input data
print("Reading from:", file_path)
data = pd.read_csv(file_path)
print("Rows loaded:", data.shape[0])

# Generate small CSV files to simulate streaming input
print(f"Starting streaming simulation for 20 iterations, sampling 5 rows each.")

for i in range(20):    
    temp = data.sample(n=5)                               #Randomly sample 5 rows from the dataset  
    out_file = os.path.join(output_dir, f"power{i}.csv")  # Build output file path
    temp.to_csv(out_file, index=False)                    # Write sampled data to CSV
    print("Written:", out_file)
    time.sleep(20)                                        # Pause to simulate micro-batch arrival

print("Streaming simulation complete.")