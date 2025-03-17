import requests
import xarray as xr
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import destinelab as deauth
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv("keys.env")  # take environment variables from .env.

DESP_USERNAME = os.environ["DESP_USERNAME"]
DESP_PASSWORD = os.environ["DESP_PASSWORD"]
# Function to retrieve data for a specific date range
def retrieve_data(start_date, end_date, output_file):
    """
    Retrieve CAMS Global Atmospheric Composition Forecasts from Destination Earth HDA.
    
    Parameters:
      start_date (str): Start date/time (e.g., "2020-06-01")
      end_date (str): End date/time (e.g., "2020-06-10")
      output_file (str): Local filename to save the downloaded data.
    """
    
    # === 1. Authenticate with DEDL/DESP ===
    auth = deauth.AuthHandler(DESP_USERNAME, DESP_PASSWORD)
    access_token = auth.get_token()
    if access_token is not None:
        print("DEDL/DESP Access Token Obtained Successfully")
    else:
        print("Failed to Obtain DEDL/DESP Access Token")

    auth_headers = {"Authorization": f"Bearer {access_token}"}

    # === 2. Build the STAC search payload ===
    # Map your CDS parameters to STAC query filters.
    # Note: For multiple values (e.g., variables, times, leadtime_hour) we use the "in" operator.
    query_filters = {
    key: {"eq": value}
    for key, value in {
        "variable": [
            "analysed_sst", 
            "analysed_sst_uncertainty", 
            "mask", 
            "sea_ice_fraction", 
        ],
        "data_format": "netcdf",
        "area": [
            67.8, -44.8, -21.8, 44.8
        ]
        }.items()
    }
    search_url = "https://hda.data.destination-earth.eu/stac/search"
    payload = {
        "collections": ["EO.MO.DAT.SST_GLO_SST_L4_REP_OBSERVATIONS_010_024"],
        "datetime": f"{start_date}/{end_date}",
        "query": query_filters
    }

    print(f"Sending STAC search request for {start_date} to {end_date} ...")
    response = requests.post(search_url, headers=auth_headers, json=payload)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)  # See what was returned (error page or issue with the request)
        return

    if not response.text:
        print("Empty response received.")
        return

    try:
        search_results = response.json()
    except ValueError as e:
        print(f"Failed to decode JSON: {e}")
        print(f"Response text: {response.text}")
        return

    if "features" not in search_results or len(search_results["features"]) == 0:
        print(f"No matching products found for {start_date}.")
        return


    # For simplicity, pick the first product in the search results
    product = search_results["features"][0]
    # DownloadLink is an asset representing the whole product
    download_url = product["assets"]["downloadLink"]["href"]
    HTTP_SUCCESS_CODE = 200
    HTTP_ACCEPTED_CODE = 202

    direct_download_url=''

    response = requests.get(download_url, headers=auth_headers)
    if (response.status_code == HTTP_SUCCESS_CODE):
        direct_download_url = product['assets']['downloadLink']['href']
    elif (response.status_code != HTTP_ACCEPTED_CODE):
        print(response.text)
    print(download_url)
    response.raise_for_status()

    # === 4. Poll until the data is ready (if necessary) ===
    # Some orders are asynchronous. If no direct download URL is provided,
    # poll using the provided Location header until the data is ready.
    # we poll as long as the data is not ready
    old_status=""
    if direct_download_url=='':
        while url := response.headers.get("Location"):
            if old_status!=response.json()['status']:
                print(f"order status: {response.json()['status']}")
                old_status=response.json()['status']
                # time.sleep(60)
            response = requests.get(url, headers=auth_headers, stream=True)

    if (response.status_code not in (HTTP_SUCCESS_CODE,HTTP_ACCEPTED_CODE)):
        (print(response.text))        
    response.raise_for_status()  

    # === 5. Determine the output filename ===
    filename = output_file

    total_size = int(response.headers.get("content-length", 0))
    print(f"Downloading {filename} ({total_size} bytes) ...")

    # === 6. Download the file with a progress bar ===
    with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
        with open(filename, 'wb') as f:
            for data in response.iter_content(1024):
                progress_bar.update(len(data))
                f.write(data)
    print("Download completed.")

# Function to merge NetCDF files into xarray
def merge_netcdf_files_xarray(output_files, merged_file):
    print(f"Merging files into {merged_file}")
    datasets = [xr.open_dataset(file) for file in output_files]
    merged_dataset = xr.concat(datasets, dim='time', data_vars='all', coords='all')
    merged_dataset.to_netcdf(merged_file)
    print(f"Merge complete: {merged_file}")

# Function to retrieve missing data files
def retrieve_missing_data(date_ranges, output_dir, workers):
    output_files = []
    for i, (start_date, end_date) in enumerate(date_ranges):
        output_file = os.path.join(output_dir, f"{start_date}.nc")
        output_files.append(output_file)
        if not os.path.exists(output_file):
            print(f"File {output_file} is missing. Starting download...")
        else:
            print(f"File {output_file} already exists. Skipping download.")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for start_date, end_date in date_ranges:
            output_file = os.path.join(output_dir, f"{start_date}.nc")
            if not os.path.exists(output_file):
                futures.append(executor.submit(retrieve_data, start_date, end_date, output_file))
        for future in as_completed(futures):
            future.result()

    return output_files

# Function to generate date ranges for each month within the specified year range
def generate_date_ranges(start_year, end_year):
    print("Generating Ranges")
    date_ranges = []
    start_date = datetime(start_year, 1, 1)
    # end_date = datetime(end_year, 12, 31)
    end_date = datetime(end_year, 1, 31)

    while start_date <= end_date:
        # Calculate the end of the month
        if start_date.month == 12:
            end_of_month = start_date.replace(day=31)
        else:
            end_of_month = start_date.replace(day=1, month=start_date.month + 1) - timedelta(days=1)

        # Split the month into three parts
        days_in_month = (end_of_month - start_date).days + 1
        part_size = days_in_month // 3

        # First part
        end_part_1 = start_date + timedelta(days=part_size - 1)
        date_ranges.append((start_date.strftime("%Y-%m-%d"), end_part_1.strftime("%Y-%m-%d")))

        # Second part
        start_part_2 = end_part_1 + timedelta(days=1)
        end_part_2 = start_part_2 + timedelta(days=part_size - 1)
        date_ranges.append((start_part_2.strftime("%Y-%m-%d"), end_part_2.strftime("%Y-%m-%d")))

        # Third part
        start_part_3 = end_part_2 + timedelta(days=1)
        date_ranges.append((start_part_3.strftime("%Y-%m-%d"), end_of_month.strftime("%Y-%m-%d")))

        # Move to the next month
        if start_date.month == 12:
            start_date = start_date.replace(year=start_date.year + 1, month=1, day=1)
        else:
            start_date = start_date.replace(month=start_date.month + 1, day=1)

    print("Ranges generated")
    return date_ranges

def download_dataset(start_year, end_year, filename, workers=1):
    date_ranges = generate_date_ranges(start_year, end_year)
    print(date_ranges)

    output_dir = "data/partial"
    os.makedirs(output_dir, exist_ok=True)

    total_start_time = time.time()
    output_files = retrieve_missing_data(date_ranges, output_dir, workers)
    total_end_time = time.time()
    print(f"All threads completed. Total time taken: {total_end_time - total_start_time:.2f} seconds")

    merge_start_time = time.time()
    merged_file = f"data/{filename}.nc"
    merge_netcdf_files_xarray(output_files, merged_file)
    merge_end_time = time.time()
    print(f"Merging completed. Time taken: {merge_end_time - merge_start_time:.2f} seconds")

    print("All done!")

download_dataset(2021,2021, "SST", 2)