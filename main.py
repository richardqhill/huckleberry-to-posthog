import os
import pytz
import time
from datetime import datetime

from dotenv import load_dotenv
import pandas as pd
from posthog import Posthog
from tqdm import tqdm

load_dotenv()

posthog = Posthog(
    project_api_key=os.getenv('POSTHOG_API_KEY'),
    host='https://us.i.posthog.com')

EXPORT_PATH = 'data/huck_export.csv'
BABY_BIRTHDAY = os.getenv('BABY_BIRTHDAY') # YYYY-MM-DD
TIMEZONE = pytz.timezone('America/New_York')

# posthog doesn't seem to have the concept of an unique insert id for historical import idempotency 
# For now, you can just change BABY_USER_ID and add a distinct_id SQL filter into your dashboards
BABY_USER_ID = 9098

def send_to_posthog():
    df = pd.read_csv(EXPORT_PATH)

    df['timestamp'] = df['Start'].apply(lambda x: TIMEZONE.localize(datetime.fromisoformat(x)))
    df['datetime'] = pd.to_datetime(df['Start']).dt.tz_localize(TIMEZONE, ambiguous=True)
    df = df.sort_values(by='datetime')
    birthday = TIMEZONE.localize(datetime.fromisoformat(BABY_BIRTHDAY))
    df['DOL'] = (df['datetime'] - birthday).dt.days

    send_bottle_events(df)
    send_pump_events(df)
    send_diaper_events(df)
    send_sleep_events(df)
    # TODO: nursing events


def posthog_slow_capture(**kwargs):
    posthog.capture(**kwargs)
    # posthog says capture has no rate limit, however, without some delay, they seem to drop events...
    time.sleep(0.15)


def send_bottle_events(df):
    df_bottle = df[(df['Type'] == 'Feed') & (df['Start Location'] == 'Bottle')]
    df_bottle = df_bottle.drop(columns=['End', 'Duration', 'Start Location'])
    df_bottle = df_bottle.rename(columns={'End Condition': 'Amount', 'Start Condition': 'Type'})
    df_bottle['Amount'] = df_bottle['Amount'].str.replace('ml', '', regex=True).astype(int)

    df_bottle['Time Since Last'] = df_bottle['datetime'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 3600
    df_bottle['Time Since Last'] = df_bottle['Time Since Last'].round(3)

    df_bottle['Is Night Bottle'] = df['datetime'].apply(lambda x: True if (x.hour > 21 or x.hour < 7) else False)

    for _, row in tqdm(df_bottle.iterrows(), total=len(df_bottle), desc="bottles"):
        event_name = 'Bottle Feed'
        props = {
                    "timestamp": row["timestamp"],
                    "DOL": row["DOL"],
                    "Amount": row["Amount"],
                    "Type": row["Type"].iloc[1], # Breast Milk or Formula
                    "Time Since Last": row["Time Since Last"],
                    "Is Night Bottle": row["Is Night Bottle"],
                    "Notes": None if pd.isna(row["Notes"]) else row["Notes"]
                }
        # print(f"{props["timestamp"]}, {row["Is Night Bottle"]}")
        posthog_slow_capture(
            distinct_id=BABY_USER_ID,
            event=event_name,
            properties=props,
            timestamp=props['timestamp'])
        
        

def send_pump_events(df):
    df_pump = df[df['Type'] == 'Pump']
    df_pump = df_pump.drop(columns=['Start Location'])
    df_pump = df_pump.rename(columns={'Start Condition': 'Left', 'End Condition': 'Right'})
    df_pump['Left'] = df_pump['Left'].str.replace('ml', '', regex=True).astype(int)
    df_pump['Right'] = df_pump['Right'].str.replace('ml', '', regex=True).astype(int)
    df_pump['Total'] = df_pump['Right'] + df_pump['Left']
    df_pump['Duration Minutes'] = df_pump['Duration'].apply(lambda x: 0 if pd.isna(x) else int(x.split(':')[0]) * 60 + int(x.split(':')[1]))

    for _, row in tqdm(df_pump.iterrows(), total=len(df_pump), desc="pumps"):
        event_name = 'Pump'
        props = {
                    "timestamp": row["timestamp"],
                    "DOL": row["DOL"],
                    "Left": row["Left"],
                    "Right": row["Right"],
                    "Total": row["Total"],
                    "Duration": row["Duration Minutes"],
                    "Notes": None if pd.isna(row["Notes"]) else row["Notes"]
                }
        posthog_slow_capture(
            distinct_id=BABY_USER_ID,
            event=event_name,
            properties=props,
            timestamp=props['timestamp'])

def send_diaper_events(df):
    df_diaper = df[df['Type'] == 'Diaper']
    df_diaper = df_diaper.drop(columns=['Start Location', 'End'])
    df_diaper = df_diaper.rename(columns={'Duration': 'Color', 'End Condition': 'diaper_info'})

    df_diaper['Pee Size'] = df_diaper['diaper_info'].str.extract(r'[Pp]ee[: ]*(\w+)')
    df_diaper['Poo Size'] = df_diaper['diaper_info'].str.extract(r'[Pp]oo[: ]*(\w+)')

    df_diaper['Type'] = df_diaper.apply(lambda row: 'Both' if pd.notna(row['Pee Size']) and pd.notna(row['Poo Size']) 
                        else 'Pee only' if pd.notna(row['Pee Size']) 
                        else 'Poo only', axis=1)
    
    df_diaper['Time Since Last'] = df_diaper['datetime'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 3600
    df_diaper['Time Since Last'] = df_diaper['Time Since Last'].round(3)

    for _, row in tqdm(df_diaper.iterrows(), total=len(df_diaper), desc="diapers"):
        event_name = 'Diaper'
        props = {
                    "timestamp": row["timestamp"],
                    "DOL": row["DOL"],
                    "Color": None if pd.isna(row["Color"]) else row["Color"],
                    "Type": row["Type"],
                    "Pee Size": None if pd.isna(row["Pee Size"]) else row["Pee Size"],
                    "Poo Size": None if pd.isna(row["Poo Size"]) else row["Poo Size"],
                    "Time Since Last": row["Time Since Last"],
                    "Notes": None if pd.isna(row["Notes"]) else row["Notes"]
                }
        posthog_slow_capture(
            distinct_id=BABY_USER_ID,
            event=event_name,
            properties=props,
            timestamp=props['timestamp'])

def send_sleep_events(df):
    NIGHT_START_HOUR = 19
    NIGHT_END_HOUR = 7

    MERGE_SLEEP_CHUNKS = True
    MAX_GAP_MINUTES = 10

    df_sleep = df[df['Type'] == 'Sleep'].copy()
    df_sleep['Duration Minutes'] = df_sleep['Duration'].apply(lambda x: 0 if pd.isna(x) else int(x.split(':')[0]) * 60 + int(x.split(':')[1]))

    df_sleep = df_sleep.sort_values(by='datetime').reset_index(drop=True)
    df_sleep['Start_dt'] = pd.to_datetime(df_sleep['Start'])
    df_sleep['End_dt'] = pd.to_datetime(df_sleep['End'])

    if MERGE_SLEEP_CHUNKS:
        merged_intervals = []
        current_start = df_sleep.loc[0, 'Start_dt']
        current_end = df_sleep.loc[0, 'End_dt']
        current_duration = df_sleep.loc[0, 'Duration Minutes']
        current_DOL = df_sleep.loc[0, 'DOL']
        num_logs = 1

        for i in range(1, len(df_sleep)):
            if (df_sleep.loc[i, 'Start_dt'] - current_end).total_seconds() <= MAX_GAP_MINUTES * 60:
                current_end = max(current_end, df_sleep.loc[i, 'End_dt'])
                current_duration = (current_end - current_start).total_seconds() // 60
                current_DOL = min(current_DOL, df_sleep.loc[i, 'DOL'])
                num_logs += 1
            else:
                merged_intervals.append((current_start, current_end, current_duration, current_DOL, num_logs))
                current_start = df_sleep.loc[i, 'Start_dt']
                current_end = df_sleep.loc[i, 'End_dt']
                current_duration = df_sleep.loc[i, 'Duration Minutes']
                current_DOL = df_sleep.loc[i, 'DOL']
                num_logs = 1

        # Append the last interval
        merged_intervals.append((current_start, current_end, current_duration, current_DOL, num_logs))

        df_sleep = pd.DataFrame(merged_intervals, columns=['Start_dt', 'End_dt', 'Duration Minutes', 'DOL', 'Num_Logs'])

    df_sleep['Time Since Last'] = df_sleep['Start_dt'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 3600
    df_sleep['Time Since Last'] = df_sleep['Time Since Last'].round(3)

    def categorize_sleep(start_time, end_time):
        start_hour = start_time.hour
        end_hour = end_time.hour
        
        # Check if the start or end hour is during night sleep
        if (start_hour >= NIGHT_START_HOUR or start_hour <= NIGHT_END_HOUR) or (end_hour >= NIGHT_START_HOUR or end_hour <= NIGHT_END_HOUR):
            return "Night"
        else:
            return "Day"

    df_sleep['Type'] = df_sleep.apply(lambda row: categorize_sleep(row['Start_dt'], row['End_dt']), axis=1)

    print("df_sleep # rows: ", len(df_sleep))
    for _, row in tqdm(df_sleep.iterrows(), total=len(df_sleep), desc="sleeps"):
        event_name = 'Sleep'
        props = {
                    "timestamp": TIMEZONE.localize(row["Start_dt"]),
                    "DOL": row["DOL"],
                    "Duration": row["Duration Minutes"],
                    "Type": row["Type"],
                    "Num_Logs": row["Num_Logs"],
                    "Time Since Last": row["Time Since Last"]
                }
        
        # print(f"{row["Start_dt"]}, {row["End_dt"]}, {props["Type"]}, {props["Duration"]} ")
        posthog_slow_capture(
            distinct_id=BABY_USER_ID,
            event=event_name,
            properties=props,
            timestamp=props['timestamp'])
    

if __name__ == "__main__":
    send_to_posthog()