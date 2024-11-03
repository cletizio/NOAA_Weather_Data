# Copyright (c) 2024 Chris Letizio

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from collections import defaultdict
import requests
import json
from datetime import datetime
import time
import csv

# NOAA API Details
API_TOKEN = '' # Obtain an API_Token from NOAA website and enter it here: https://www.ncdc.noaa.gov/cdo-web/token
BASE_URL = 'https://www.ncei.noaa.gov/cdo-web/api/v2/data'

# Parameters
location_ids = {'FIPS:24': 'Michigan', 'FIPS:04': 'Arizona'} # Location mapping of FIPS # to name
dataset_id = 'GHCND'
start_date = '2024-01-01'
end_date =  '2024-01-07'
limit = 1000 # Max records per req.

# Set the headers and parameteres for the request
headers = {'token': API_TOKEN}

# Initialize dictionary to aggregate the data
daily_data = defaultdict(lambda: defaultdict(lambda:defaultdict(list)))

for location_id, state_name in location_ids.items():
    # Initialize offset for pagination
    offset = 1
    while True:
        params = {
        'datasetid': dataset_id,
        'locationid': location_id,
        'startdate': start_date,
        'enddate': end_date,
        'limit': limit,
        'units': 'standard', # 'metric' for mm and Celcius
        'datatypeid': 'TMAX, TMIN, PRCP, AWND', # Max and min temp, precip, and avg wind speed
        'offset': offset
        }

        # Create the request
        response = requests.get(BASE_URL, headers=headers, params=params)
        # Check the request
        if response.status_code == 200:
            try:
                data = response.json()
            except json.JSONDecodeError:
                print("Failed to decode JSON. Response text:", response.text)
                break
            
            # Check for more results
            if 'results' not in data or not data['results']:
                break # Exit loop if there are no more results

            # Process the data
            for item in data.get('results', []):
                date = item['date'][:10] # Extract the date only
                datatype = item['datatype']
                value = item['value']

                # Append value to the corresponding date and datatype
                daily_data[date][state_name][datatype].append(value)

            # Increment offset for the next batch
            offset += limit

            # Delay for rate limits on free NOAA API
            time.sleep(0.25) # 0.2 seconds = 5 reqs /sec
        else:
            print(f"Error: {response.status_code}")
            print("Response text:", response.text)
            break


# Save data to CSV
with open('noaa_weather_data.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Date', 'State', 'TMAX_AVG', 'TMIN_AVG', 'PRCP_AVG', 'AWND_AVG'])

    # Calculate averages and print
    for date, states in daily_data.items():
        for state, types in states.items():
            row = [date, state]
            for datatype in ['TMAX', 'TMIN', 'PRCP', 'AWND']:
                values = types[datatype]
                avg_value = sum(values) / len(values) if values else None
                row.append(avg_value)
            writer.writerow(row)

print("Data saved to 'noaa_weather_data.csv'")

