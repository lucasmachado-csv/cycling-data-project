import requests
import os
import xml.etree.ElementTree as ET

# Setup
BUCKET_URL = "https://s3.amazonaws.com/tripdata/"
DATA_DIR = "data_nyc"
KEYWORDS = ["2018", "2019", "2020", "2021", "2022", "2023"]

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 1. Get list of all files
print("Fetching file list...")
response = requests.get(BUCKET_URL)
root = ET.fromstring(response.content)
namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

# 2. Filter and Download
contents = root.findall('s3:Contents', namespace)
all_keys = [c.find('s3:Key', namespace).text for c in contents]

all_necessary_keys = []
for key in all_keys:
    key_lower = key.lower()
    
    # Logic: Must NOT contain "JC" (Jersey City), must contain one of the year keywords
    if "jc" in key_lower:
        continue
        
    if any(k in key_lower for k in KEYWORDS):
        all_necessary_keys.append(key)

print(f"Found {len(all_necessary_keys)} relevant files.")

for i, key in enumerate(all_necessary_keys):
    filename = key.replace('/', '_')
    filepath = os.path.join(DATA_DIR, filename)
    
    # Skip if exists
    if os.path.exists(filepath):
        print(f"[{i+1}/{len(all_necessary_keys)}] Skipping {key} (Already exists)")
        continue

    print(f"[{i+1}/{len(all_necessary_keys)}] Downloading {key}...")
    
    with requests.get(BUCKET_URL + key, stream=True) as r:
        if r.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

print("Done.")
