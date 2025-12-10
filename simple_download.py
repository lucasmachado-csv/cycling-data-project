import requests
import os
import xml.etree.ElementTree as ET

# Setup
BUCKET_URL = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/"
DATA_DIR = "data"
KEYWORDS = ["journey", "data", "extract"]

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

years = ["2018", "2019", "2020", "2021", "2022", "2023"]

all_necessary_keys = []
for key in all_keys:
    key_lower = key.lower()
    if (key_lower.endswith('.csv') and 
        'usage-stats' in key_lower and 
        all(k in key_lower for k in KEYWORDS) and
        any(y in key_lower for y in years)):
        all_necessary_keys.append(key)

print(f"Found {len(all_necessary_keys)} relevant files.")

for i, key in enumerate(all_necessary_keys):
    filename = key.replace('/', '_')
    filepath = os.path.join(DATA_DIR, filename)
    
    print(f"[{i+1}/{len(all_necessary_keys)}] Downloading {key}...")
    
    with requests.get(BUCKET_URL + key, stream=True) as r:
        if r.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

print("Done.")

