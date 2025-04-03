import streamlit as st
import psycopg2
import pandas as pd
from datetime import timedelta, time
import os

# Database connection details
db_config_challan = {
    'host': '34.100.223.97',
    'port': '5432',
    'database': 'master_prod',
    'user': 'postgres',
    'password': 'theimm0rtaL'
}

db_config_trips = {
    'host': '34.100.223.97',
    'port': '5432',
    'database': 'trips',
    'user': 'postgres',
    'password': 'theimm0rtaL'
}

db_config_uber = {
    'host': '34.100.216.3',
    'port': '5432',
    'database': 'uber_full',
    'user': 'kartik',
    'password': 'Project@Li3'
}

# Connect to the PostgreSQL database
def connect_to_db(config):
    conn = psycopg2.connect(**config)
    return conn

# Streamlit UI
st.title("Challan Data Matching App")

# Define city options
city_options = ["All", "BLR", "HYD", "CHN", "PNQ", "KOL", "NCR", "NCR-GGN", "NCR-NOIDA", "MPL"]

# Allow multi-selection with "All" option
selected_cities = st.multiselect("Select City", city_options, default=["All"])

# If "All" is selected, treat it as selecting all cities
if "All" in selected_cities:
    selected_cities = city_options[1:]  # Exclude "All
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

def load_challan_data_from_db(conn_challan, start_date, end_date):
    query = """
    SELECT * FROM traffic_challan
    WHERE violation_date_time BETWEEN %s AND %s;
    """
    challan_df = pd.read_sql(query, conn_challan, params=(start_date, end_date))
    challan_df['violation_date_time'] = pd.to_datetime(challan_df['violation_date_time'])
    return challan_df

def get_trip_for_vehicle(conn_trips, vehicle_reg_no, violation_time):
    morning_start = time(0, 0)
    morning_end = time(8, 0)
    afternoon_start = time(13, 0)

    if morning_start <= violation_time.time() < morning_end:
        start_time_range = violation_time.replace(hour=0, minute=0)
        end_time_range = violation_time.replace(hour=8, minute=0)
    elif violation_time.time() >= afternoon_start:
        start_time_range = violation_time.replace(hour=13, minute=0)
        end_time_range = violation_time + timedelta(hours=11)
    else:
        time_range = timedelta(hours=1)
        start_time_range = violation_time - time_range
        end_time_range = violation_time + time_range

    query = """
    SELECT vehicle_reg_no, actual_start_time, actual_end_time, driver_lithium_id, driver_name, client_office
    FROM etms_trips
    WHERE vehicle_reg_no = %s
    AND actual_start_time BETWEEN %s AND %s;
    """
    with conn_trips.cursor() as cur:
        cur.execute(query, (vehicle_reg_no, start_time_range, end_time_range))
        trips = cur.fetchall()
    if trips:
        trips_df = pd.DataFrame(trips, columns=['vehicle_reg_no', 'actual_start_time', 'actual_end_time', 'driver_lithium_id', 'driver_name', 'client_office'])
        trips_df['actual_start_time'] = pd.to_datetime(trips_df['actual_start_time'])
        closest_trip = trips_df.loc[(trips_df['actual_start_time'] - violation_time).abs().idxmin()]
        return closest_trip['driver_lithium_id'], closest_trip['driver_name'], closest_trip['client_office']
    return None, None, None

def get_trip_from_uber(conn_uber, vehicle_reg_no, violation_time):
    query = """
    SELECT vehicle_number, city, Trip_date, driver_name, driver_uuid, trip_request_time, trip_drop_off_time
    FROM public.seven_trip_report
    WHERE vehicle_number = %s
    AND trip_request_time <= %s
    AND trip_drop_off_time >= %s;
    """
    with conn_uber.cursor() as cur:
        cur.execute(query, (vehicle_reg_no, violation_time, violation_time))
        trips = cur.fetchall()
    if trips:
        trips_df = pd.DataFrame(trips, columns=['vehicle_number', 'city', 'Trip_date', 'driver_name', 'driver_uuid', 'trip_request_time', 'trip_drop_off_time'])
        trips_df['trip_request_time'] = pd.to_datetime(trips_df['trip_request_time'])
        trips_df['time_diff'] = (trips_df['trip_request_time'] - violation_time).abs()
        closest_trip = trips_df.loc[trips_df['time_diff'].idxmin()]
        return closest_trip['driver_uuid'], closest_trip['driver_name'], closest_trip['city']
    return None, None, None

def get_drivers_for_day(conn_trips, vehicle_reg_no, date):
    query = """
    SELECT driver_lithium_id, driver_name
    FROM etms_trips
    WHERE vehicle_reg_no = %s
    AND DATE(actual_start_time) = %s;
    """
    with conn_trips.cursor() as cur:
        cur.execute(query, (vehicle_reg_no, date))
        result = cur.fetchone()
    return result if result else (None, None)

def match_challan_with_trips():
    conn_challan = connect_to_db(db_config_challan)
    conn_trips = connect_to_db(db_config_trips)
    conn_uber = connect_to_db(db_config_uber)

    challan_df = load_challan_data_from_db(conn_challan, start_date, end_date)

    def find_driver(row):
        driver_lithium_id, driver_name, client_office = get_trip_for_vehicle(conn_trips, row['vehicle_number'], row['violation_date_time'])
        if not driver_lithium_id:
            driver_lithium_id, driver_name, city = get_trip_from_uber(conn_uber, row['vehicle_number'], row['violation_date_time'])
            if not driver_lithium_id:
                for day_offset in range(1, 21):
                    previous_date = row['violation_date_time'].date() - timedelta(days=day_offset)
                    driver_lithium_id, driver_name = get_drivers_for_day(conn_trips, row['vehicle_number'], previous_date)
                    if driver_lithium_id:
                        return pd.Series([driver_lithium_id, driver_name, None])
                return pd.Series([None, None, None])
            return pd.Series([driver_lithium_id, driver_name, city])
        return pd.Series([driver_lithium_id, driver_name, client_office])

    challan_df[['driver_lithium_id', 'driver_name', 'client_office']] = challan_df.apply(find_driver, axis=1)

    # Apply city filter
    if selected_cities:
        challan_df = challan_df[challan_df['city'].isin(selected_cities)]

    st.dataframe(challan_df)

    csv = challan_df.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", data=csv, file_name="filtered_challan_data.csv", mime="text/csv")

if st.button("Run Matching"):
    match_challan_with_trips()
