from curl_cffi import requests
import csv
import os
import re
import json
from datetime import datetime

URL = "https://waittime.easternhealth.org.au/"
CSV_PATH = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital.csv"

def format_time(minutes):
    """Converts raw minutes into the 'X hr Y min' format used by the website."""
    try:
        mins = int(minutes)
        if mins < 60:
            return f"{mins} min"
        hrs = mins // 60
        rem_mins = mins % 60
        if rem_mins == 0:
            return f"{hrs} hr"
        return f"{hrs} hr {rem_mins} min"
    except (ValueError, TypeError):
        return "N/A"

def scrape_hospital():
    try:
        # 1. Fetch the page bypassing the firewall
        response = requests.get(URL, impersonate="chrome120", timeout=20)
        if response.status_code != 200:
            print(f"Server rejected connection. Status: {response.status_code}")
            return

        html = response.text
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

        # 2. Extract the exact JSON dictionaries using Regular Expressions
        # We look for "const variableName =" and capture everything inside the { }
        counts_match = re.search(r'const patientCounts\s*=\s*(\{.*?\});', html)
        waits_match = re.search(r'const predictedWaitMinutes\s*=\s*(\{.*?\});', html)

        if not counts_match or not waits_match:
            print("Failed to locate the data variables in the HTML.")
            return

        # 3. Convert the extracted text into Python dictionaries
        patient_counts = json.loads(counts_match.group(1))
        predicted_waits = json.loads(waits_match.group(1))

        mapping = {
            "BoxHill": "Box Hill Hospital",
            "Angliss": "Angliss Hospital",
            "Maroondah": "Maroondah Hospital"
        }

        rows = []
        for key, formal_name in mapping.items():
            # Get counts safely
            c_data = patient_counts.get(key, {})
            waiting = c_data.get("waiting", 0)
            treating = c_data.get("beingTreated", 0)

            # Get wait times safely and format them
            w_data = predicted_waits.get(key, {})
            min_wait = format_time(w_data.get("min", 0))
            max_wait = format_time(w_data.get("max", 0))
            wait_time_str = f"{min_wait} - {max_wait}" if min_wait != "N/A" else "N/A"

            rows.append([timestamp, formal_name, waiting, treating, wait_time_str])

        # 4. Write data to the SSD
        os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
        file_exists = os.path.isfile(CSV_PATH)
        with open(CSV_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['timestamp', 'hospital', 'waiting', 'treating', 'wait_time'])
            writer.writerows(rows)
            
        print(f"[{timestamp}] Success! CSV updated.")
        for row in rows:
            print(f" -> {row[1]}: {row[2]} waiting, {row[3]} treating. Est wait: {row[4]}")

    except Exception as e:
        print(f"Extraction failed: {e}")

if __name__ == "__main__":
    scrape_hospital()
