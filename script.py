import os
import csv
from glob import glob
from datetime import datetime, timedelta

def is_valid_date(date_val):
    try:
        datetime.strptime(date_val, "%Y-%m-%d %H:%M:%S.%f")
        return True
    except ValueError:
        return False

def is_valid_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def load_tick_data(directory):
    all_data = []
    f_string = os.path.join(directory, "CTG_*.csv")
    count = 0 
    
    for filename in glob(f_string):
        if count == 1000: 
            break
        try:
            with open(filename, 'r') as file:
                reader = csv.reader(file)
                next(reader)  
                for row_number, row in enumerate(reader, start=2):  
                    if len(row) != 3:
                        print(f"Warning: Invalid number of columns in {filename}, row {row_number}")
                        continue
                    
                    timestamp_str, price_str, volume_str = row
                    
                    if not is_valid_date(timestamp_str):
                        print(f"Warning: Invalid date format in {filename}, row {row_number}")
                        continue
                    
                    if not is_valid_number(price_str) or not is_valid_number(volume_str):
                        print(f"Warning: Invalid numeric value in {filename}, row {row_number}")
                        continue
                    
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    price = float(price_str)
                    volume = int(float(volume_str)) 
                    
                    all_data.append((timestamp, price, volume))


            count += 1
        except Exception as e:
            print(f"Error reading file {filename}: {str(e)}")
    
    return sorted(all_data, key=lambda x: x[0])

data_directory = "data"
tick_data = load_tick_data(data_directory)

for record in tick_data[:5]:
    print(record)

from datetime import timedelta

def clean_tick_data(data):
    cleaned_data = []
    last_valid_price = None
    last_valid_timestamp = None

    for timestamp, price, volume in data:
        # Remove negative or zero prices
        if price <= 0:
            continue

        # Remove negative volumes
        if volume < 0:
            continue

        # Remove duplicate timestamps
        if last_valid_timestamp == timestamp:
            continue

        # Remove large price jumps (e.g., more than 10% change)
        if last_valid_price is not None:
            price_change = abs(price - last_valid_price) / last_valid_price
            if price_change > 0.1:
                continue

        cleaned_data.append((timestamp, price, volume))
        last_valid_price = price
        last_valid_timestamp = timestamp

    return cleaned_data

cleaned_tick_data = clean_tick_data(tick_data)



def parse_interval(interval_str):
    units = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    total_seconds = 0
    current_num = ''
    for char in interval_str:
        if char.isdigit():
            current_num += char
        elif char in units:
            if current_num:
                total_seconds += int(current_num) * units[char]
                current_num = ''
    return timedelta(seconds=total_seconds)

def generate_ohlcv_bars(data, interval, start_time, end_time):
    interval_td = parse_interval(interval)
    current_time = start_time
    ohlcv_bars = []

    while current_time < end_time:
        next_time = current_time + interval_td
        interval_data = [d for d in data if current_time <= d[0] < next_time]
        
        if interval_data:
            open_price = interval_data[0][1]
            high_price = max(d[1] for d in interval_data)
            low_price = min(d[1] for d in interval_data)
            close_price = interval_data[-1][1]
            volume = sum(d[2] for d in interval_data)
            
            ohlcv_bars.append((current_time, open_price, high_price, low_price, close_price, volume))
        
        current_time = next_time

    return ohlcv_bars

def save_ohlcv_bars(ohlcv_bars, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        for bar in ohlcv_bars:
            writer.writerow(bar)

interval = "15m"
start_time = datetime(2024, 9, 17)
end_time = datetime(2024, 9, 18)

ohlcv_bars = generate_ohlcv_bars(cleaned_tick_data, interval, start_time, end_time)
save_ohlcv_bars(ohlcv_bars, "CTG_OHLCV_15m.csv")
