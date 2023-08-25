import os
from datetime import datetime
import boto3
import argparse
import requests
import pandas as pd
from io import StringIO
import awswrangler as wr
import pyarrow as pa
import pyarrow.parquet as pq
import sys
from dotenv import load_dotenv, find_dotenv

# Dictionary mapping city names to station names in station Inventory
city_map={
    "Victoria": "VICTORIA INTL A",
    "Vancouver": "VANCOUVER INTL A",
    "Edmonton": "EDMONTON INTL A",
    "Calgary": "CALGARY INTL A",
    "Regina": "REGINA INTL A",
    "Saskatoon": "SASKATOON INTL A",
    "Winnipeg": "WINNIPEG INTL A",
    "Ottawa": "OTTAWA INTL A",
    "Toronto": "TORONTO INTL A",
    "Quebec": "QUEBEC INTL A",
    "Quebec/Jean Lesage": "QUEBEC/JEAN LESAGE INTL A",
    "Montreal/Pierre Elliott Trudeau": "MONTREAL/PIERRE ELLIOTT TRUDEAU INTL A",
    "Montreal": "MONTREAL INTL A",
    "Montreal Mirabel": "MONTREAL MIRABEL INTL A",
    "Fredericton": "FREDERICTON INTL A",
    "Moncton/Greater Moncton Romeo Leblanc": "MONCTON/GREATER MONCTON ROMEO LEBLANC INTL A",
    "Moncton / Greater Moncton Romeo Leblanc": "MONCTON / GREATER MONCTON ROMEO LEBLANC INTL A",
    "Gander": "GANDER INTL A",
    "St. John's": "ST. JOHN'S INTL A"
}

#selected columns from downloaded data
SELECTED_COLUMNS = [
    "Station Name", "Date/Time", "Year", "Month", "Day",
    "Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)"
]

#Function to download weather data for given station ID
def download_weather_data(year, station_id):
    url = f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&timeframe=2&submit=Download+Data'
    response = requests.get(url)

    if response.status_code == 200:
        csv_content = response.content
        data = pd.read_csv(StringIO(csv_content.decode('utf-8')))
        return data
    else:
        print("Error downloading data.")
        return None

#Function to get stationid from Station inventory
def get_station_id(city_name):
    # Read the CSV data and skip the first three rows
    data = pd.read_csv('Station Inventory EN.csv', skiprows=3)
    # Create a dictionary to map Name to Station ID
    name_to_station_id = dict(zip(data['Name'], data['Station ID']))
    # Function to get Station ID by Name
    def get_station_id_by_name(name):
        return name_to_station_id.get(name)
    # Example usage
    station_name = city_name
    station_id = get_station_id_by_name(station_name)
    if station_id is not None:
        return(station_id)
    else:
        return("No matching")

#Function to upload data in S3 with parquet partition.
def upload_to_s3(data, s3_bucket, s3_prefix):
    
    s3 = pa.fs.S3FileSystem(region=os.environ.get("AWS_REGION"))

    for year_month_day, partitioned_data in data.groupby('Year_Month_Day'):
        year, month, day = year_month_day.split('-')
        partition_key = f'{s3_bucket}//{s3_prefix}/{year}/{month}/{day}/'
        
        table = pa.Table.from_pandas(partitioned_data)
        pq.write_to_dataset(table, root_path=partition_key, filesystem=s3) 
        

#Function to create excel for weather data.
def create_excel_sheets(data, output_file):
    with pd.ExcelWriter(output_file) as writer:
        for year, year_data in data.groupby('Year'):
            year_sheet_name = f'Data_{year}'
            year_data.to_excel(writer, sheet_name=year_sheet_name, index=False)

#Function to scan data from s3
def read_data_froms3(s3_bucket,s3_prefix):
    # Load the data for the given year and city from S3 bucket
    s3_path = f's3://{s3_bucket}/{s3_prefix}'
    dataset = pq.ParquetDataset(s3_path)
    # Read the dataset into a Pandas DataFrame
    data_frame = dataset.read().to_pandas()
    return data_frame

#Function to get max and min temp
def get_max_min_temp(df):
    max_temp = df['Max Temp (°C)'].max()
    min_temp = df['Min Temp (°C)'].min()
    return max_temp, min_temp

#Function to get percentage difference
def get_percentage_diff(df,input_year):
    # convert from string to datetime format
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    # Calculate the average daily temperature for the given year
    avg_temp_current_year = df[df['Year'] == input_year]['Mean Temp (°C)'].mean()
    # Calculate the average daily temperature for the previous two years
    previous_years = [input_year - 1, input_year - 2]
    avg_temp_previous_years = df[df['Year'].isin(previous_years)]['Mean Temp (°C)'].mean()
    # Calculate the percentage difference
    percentage_difference = round(((avg_temp_current_year - avg_temp_previous_years) / avg_temp_previous_years) * 100,2)
    return percentage_difference

#Function to get diff between avg per month
def get_diff_avg_temp_per_month(df,input_year):
    # Convert 'Date/Time' column to datetime
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    # Extract year and month from 'Date/Time' column
    df['Year'] = df['Date/Time'].dt.year
    df['Month'] = df['Date/Time'].dt.month
    # Calculate the average temperature per month for the given year
    avg_temp_per_month = df[df['Year'] == input_year].groupby('Month')['Mean Temp (°C)'].mean()
    temperature_difference = avg_temp_per_month.diff()
    return temperature_difference

def main():
    parser = argparse.ArgumentParser(description="Download weather data for a specific city.")
    parser.add_argument("--city", type=str, help="Name of the city")
    parser.add_argument("--year", type=int, help="Start year")
    args = parser.parse_args()
    fileenv= load_dotenv(find_dotenv(filename='.environment'))
    
    
    if args.city is None:
        print("Please provide a city using the --city argument.")
        return

    if args.year is None:
        print("Please provide a year using the --year argument.")
        return

    
    # Get station Id for given City.
    station_id = get_station_id(city_map.get(args.city))
    if station_id == "No matching":
        print(f"Station id is not matching for City '{args.city}'. Please check for exact City name in given list of Cities.")
        return

    #Fetch data with stationId
    weather_data = pd.DataFrame()
    for year in range(args.year - 2, args.year+1):
        data = pd.DataFrame(download_weather_data(year,station_id))
        if data is not None:
            # Process and save the data as needed
            weather_data = pd.concat([weather_data, data], ignore_index=True)

    # Create an Excel file with separate sheets for each year's data
    output_file = "/wave/weather_data_"+args.city+".xlsx"
    create_excel_sheets(weather_data, output_file)

    # Remove rows with null values in specific columns and remove unwanted columns
    weather_data = weather_data[SELECTED_COLUMNS]
    weather_data.dropna(subset=["Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)"], inplace=True)
    
    # Remove rows with future dates
    future_year= args.year +1
    first_day_of_end_year = pd.to_datetime(f"{future_year}-01-01")
    weather_data["Date/Time"] = pd.to_datetime(weather_data["Date/Time"])
    weather_data = weather_data[weather_data["Date/Time"] < first_day_of_end_year]

    # Convert the "Date/Time" column back to string format
    weather_data["Date/Time"] = weather_data["Date/Time"].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Create a new column for the Year, Month and day for partitioning
    weather_data['Year_Month_Day'] = weather_data['Date/Time'].str.slice(0, 10)

    # Upload data to S3 and partition it
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    print(s3_bucket)
    s3_prefix = "weather_data/" + args.city
    upload_to_s3(weather_data,s3_bucket,s3_prefix)

    #scan required dataset
    scanned_df= read_data_froms3(s3_bucket,s3_prefix)

    #Analyze the weather data to get max and min temprature for input year
    max_temp, min_temp = get_max_min_temp(scanned_df)
    print(f"\n--> Max Temperature for {str(args.year)}: {str(max_temp)} °C \n")
    print(f"--> Min Temperature for {str(args.year)}: {str(min_temp)} °C \n")

    #Analyzed data to Percentage difference between the average daily temperature for the year versus the average of the previous 2 years.
    percent_diff= get_percentage_diff(scanned_df,args.year)
    print(f"--> Percentage Difference between avg temp of {str(args.year)} and avg temp of {str(args.year-1)} and {str(args.year-2)} is {str(percent_diff)} % \n")

    #Difference between the average temperature per month for year
    avg_diff= get_diff_avg_temp_per_month(scanned_df,args.year)
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
        7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    print(f"--> Difference in average temperature between months for {args.year}:")
    for month_num, temp_diff in avg_diff.items():
        if not pd.isnull(temp_diff):
            month_name = month_names[month_num]
            print(f"    Difference in {month_name}: {temp_diff:.2f} °C")
if __name__ == '__main__':
    main()