############################
## ST554_FinalProject
## Author: Joy Zhou
## Data: 4/25/2026
##############################

#Streaming Part
import pandas as pd
import time
import os

    
# Define the output directory for streaming files
base_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(base_dir, "csv_files")

os.makedirs(output_dir, exist_ok=True)

# Ensure the output directory exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Read in some data to sample from
try:
    data = pd.read_csv("FinalProject/power_streaming_data.csv")
    print(f"Successfully read {len(data)} rows from the dataset.")
except Exception as e:
    print(f"Error reading dataset: {e}")
    exit()

# Simulate streaming by writing randomly sampled rows to new CSV files
print(f"Starting streaming simulation for 20 iterations, sampling 5 rows each.")

for i in range(0, 20):
    # Randomly sample a few rows
    sampled = data.sample(n=5, random_state=i)                     
    output_filename = os.path.join(output_dir, f'power{i:04d}.csv')

    # Write sampled chunk to a new CSV file, without the index
    sampled.to_csv(output_filename, index=False, header=True)

    print(f"Wrote {len(sampled)} randomly sampled rows to {output_filename}")
    time.sleep(20)

print("Streaming simulation complete.")


