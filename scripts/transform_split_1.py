# data-class: public-aggregate
import pandas as pd
import re
import holidays
import time
from datetime import timedelta
from status import update_status

# Initialize Victorian Holidays
vic_holidays = holidays.AU(subdiv='VIC')

# Define paths
INPUT_FILE = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital.csv"
OUTPUT_FILE = "/mnt/router_ssd/Data_Hub/Waiting_Live_time/eastern_hospital_cleaned.csv"

def parse_time_to_minutes(time_str):
    if pd.isna(time_str) or time_str == "N/A":
        return 0
    hours, minutes = 0, 0
    hr_match = re.search(r'(\d+)\s*hr', time_str)
    if hr_match: hours = int(hr_match.group(1))
    min_match = re.search(r'(\d+)\s*min', time_str)
    if min_match: minutes = int(min_match.group(1))
    return (hours * 60) + minutes

def get_advanced_features(dt):
    d = dt.date()
    # 1. Holiday Logic
    is_h = 1 if d in vic_holidays else 0
    # 2. Holiday Eve Logic
    tomorrow = d + timedelta(days=1)
    is_eve = 1 if tomorrow in vic_holidays else 0
    # 3. Day Type (0=Weekday, 1=Weekend, 2=Holiday)
    if is_h: day_type = 2
    elif d.weekday() >= 5: day_type = 1
    else: day_type = 0
    # 4. Seasonal Logic
    month = d.month
    if month in [12, 1, 2]: season = 1 # Summer
    elif month in [3, 4, 5]: season = 2 # Autumn
    elif month in [6, 7, 8]: season = 3 # Winter
    else: season = 4 # Spring
    return is_h, is_eve, day_type, season

def clean_hospital_data():
    df = pd.read_csv(INPUT_FILE)
    if df.empty: return

    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp_local'] = df['timestamp'].dt.tz_convert('Australia/Melbourne')

    # Standard Features (derived from local Victoria time)
    df['hour'] = df['timestamp_local'].dt.hour
    df['day_of_week'] = df['timestamp_local'].dt.weekday
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

    # Advanced Features (derived from local Victoria time)
    features = df['timestamp_local'].apply(get_advanced_features)
    df[['is_holiday', 'is_eve', 'day_type', 'season']] = pd.DataFrame(features.tolist(), index=df.index)
    
    # Split wait_time
    split_times = df['wait_time'].fillna('').str.split(' - ', expand=True)
    df['min_wait_mins'] = split_times[0].apply(parse_time_to_minutes)
    df['max_wait_mins'] = split_times[1].apply(parse_time_to_minutes)
    df['load_ratio'] = (df['waiting'] / df['treating'].replace(0, 1)).round(2)

    # Dedup: drop consecutive rows per hospital with unchanged wait and load
    df = df.sort_values(['hospital', 'timestamp']).reset_index(drop=True)
    prev_min  = df.groupby('hospital')['min_wait_mins'].shift(1)
    prev_load = df.groupby('hospital')['load_ratio'].shift(1)
    df = df[~((df['min_wait_mins'] == prev_min) & (df['load_ratio'] == prev_load))]

    # --- RESILIENT SAVE BLOCK ---
    max_retries = 5
    for attempt in range(max_retries):
        try:
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"Success! Cleaned data saved to {OUTPUT_FILE}")
            update_status("transform_split_1", "PASS")
            break
        except OSError as e:
            if e.errno == 16: # Device or resource busy
                print(f"SSD Busy (Attempt {attempt+1}/{max_retries}). Retrying in 5s...")
                time.sleep(5)
                if attempt == max_retries - 1:
                    update_status("transform_split_1", "FAIL")
            else:
                update_status("transform_split_1", "FAIL")
                raise e
    # -----------------------------

if __name__ == "__main__":
    clean_hospital_data()
