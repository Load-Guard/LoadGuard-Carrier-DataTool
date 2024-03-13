#region ###IMPORTS###
import csv
import sys
import json
import glob
import os
import gc
gc.set_threshold(10000, 2000, 1000)
import time
import logging
from distributed import LocalCluster
import requests
import inquirer
from csv import writer
from ftplib import FTP, error_perm
from rich.console import Console
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import dask.dataframe as dd
from dask.distributed import Client
from dask import config as dask_config
from collections import defaultdict
from csv import writer
import pandas as pd
from dask.diagnostics import ProgressBar
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    SpinnerColumn,
)
#endregion

#region ###ANSI COLOR CODES###
MAGENTA = '\033[95m'
BLACK = '\033[30m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
CYAN = '\033[36m'
WHITE = '\033[37m'
ENDC = '\033[0m'
#endregion

#region Setup logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#endregion

#region ###VARIABLES###

# Increase the CSV field size limit
csv.field_size_limit(2**28)

# List of Carrier files Columns to keep (normalized headers).
columns_to_keep = [
    "ACT_STAT", "CARSHIP", "DOT_NUMBER", "NAME", "NAME_DBA", "DBNUM", "PHY_NATN", "PHY_STR", "PHY_CITY", "PHY_ST",
    "PHY_ZIP", "TEL_NUM", "CELL_NUM", "FAX_NUM", "MAI_NATN", "MAI_STR", "MAI_CITY", "MAI_ST", "MAI_ZIP",
    "ICC_DOCKET_1_PREFIX", "ICC1", "ICC_DOCKET_2_PREFIX", "ICC2", "ICC_DOCKET_3_PREFIX", "ICC3", "class",
    "CRRINTER", "CRRHMINTRA", "CRRINTRA", "ORG", "GENFREIGHT", "HOUSEHOLD", "METALSHEET", "MOTORVEH", "DRIVETOW",
    "LOGPOLE", "BLDGMAT", "MACHLRG", "PRODUCE", "LIQGAS", "INTERMODAL", "OILFIELD", "LIVESTOCK", "GRAINFEED",
    "COALCOKE", "MEAT", "CHEM", "DRYBULK", "COLDFOOD", "BEVERAGES", "PAPERPROD", "UTILITY", "FARMSUPP", "CONSTRUCT",
    "WATERWELL", "CARGOOTHR", "OTHERCARGO", "HM_IND", "OWNTRUCK", "OWNTRACT", "OWNTRAIL", "TRMTRUCK", "TRMTRACT", "TRMTRAIL", "TRPTRUCK", "TRPTRACT", "TRPTRAIL", "TOT_TRUCKS", "TOT_PWR", "TOT_DRS", "CDL_DRS", "REVTYPE", "REVDOCNUM", "REVDATE", "ACC_RATE",
    "REPPREVRAT", "MLG150", "RATING", "RATEDATE", "EMAILADDRESS", "USDOT_REVOKED_FLAG", "USDOT_REVOKED_NUMBER", "COMPANY_REP1", "COMPANY_REP2", "MCS_150_DATE"
]

# List of Inspection files Columns to keep (normalized headers).
insp_columns_to_keep = [
    "INSPECTION_ID", "DOT_NUMBER", "REPORT_STATE", "INSP_DATE", "REGISTRATION_DATE",
    "REGION", "CI_STATUS_CODE", "INSP_LEVEL_ID", "CARGO_TANK", "HAZMAT_PLACARD_REQ",
    "INSP_CONFIDENCE_LEVEL", "OOS_DEFECT_VER", "VIOL_TOTAL", "OOS_TOTAL",
    "DRIVER_VIOL_TOTAL", "DRIVER_OOS_TOTAL", "VEHICLE_VIOL_TOTAL",
    "VEHICLE_OOS_TOTAL", "HAZMAT_VIOL_TOTAL", "HAZMAT_OOS_TOTAL"
]

# Global setting for splitting files
LINES_PER_FILE = 15000

#endregion

# ASCII Art as a Raw String.
ascii_art = RED + r"""
██╗      ██████╗  █████╗ ██████╗  ██████╗ ██╗   ██╗ █████╗ ██████╗ ██████╗   
██║     ██╔═══██╗██╔══██╗██╔══██╗██╔════╝ ██║   ██║██╔══██╗██╔══██╗██╔══██╗  
██║     ██║   ██║███████║██║  ██║██║  ███╗██║   ██║███████║██████╔╝██║  ██║  
██║     ██║   ██║██╔══██║██║  ██║██║   ██║██║   ██║██╔══██║██╔══██╗██║  ██║  
███████╗╚██████╔╝██║  ██║██████╔╝╚██████╔╝╚██████╔╝██║  ██║██║  ██║██████╔╝  
╚══════╝ ╚═════╝ ╚═╝  ╚═╝╚═════╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝    

 ██████╗██╗     ██╗    ████████╗ ██████╗  ██████╗ ██╗     
██╔════╝██║     ██║    ╚══██╔══╝██╔═══██╗██╔═══██╗██║                                                                                                                                           
██║     ██║     ██║       ██║   ██║   ██║██║   ██║██║     
██║     ██║     ██║       ██║   ██║   ██║██║   ██║██║             
╚██████╗███████╗██║       ██║   ╚██████╔╝╚██████╔╝███████╗
 ╚═════╝╚══════╝╚═╝       ╚═╝    ╚═════╝  ╚═════╝ ╚══════╝
"""

# Main Menu Code.
def main_menu():
    print(ascii_art)  # Display ASCII art
    navigation_guide = """
+----------------------------------------------------+
|            Directions & Navigation:                |
|                                                    |
|    Place Carrier Census and Inspection Files       |
|     In The Same Directory As The Cli Tool.         |
|                                                    |
|   ↑ / ↓        - Move Up/Down through options      |
|   → / Space    - Select a highlighted option       |
|   Enter        - Confirm the selected option       |
|                                                    |
+----------------------------------------------------+
"""
    print(navigation_guide)  # Display navigation guide above the menu

    questions = [
        inquirer.Checkbox(
            'operations',
            message="Select operations to perform",
            choices=[
                'Step 1: Process Carrier CSV Files',
                'Step 2: Process Inspection CSV Files',
                'Step 3: Process Insp/VIN CSV Files',
                'Step 4: Enhance Census with Violation Data',
                'Step 5: Create DOTs to VIN File',
                'Step 6: Create VIN to DOTs File',
                'Step 7: Create VIN to DOTs MATCHED Only File',
                'Step 8: Split Processed Census Files',
                'Step 9: Split Processed Inspection Files',
                'Step 10: Split Processed Insp_Unit Files',
                'Step 11: Split DOT-VIN Files',
                'Step 12: Split VIN-DOT Files',
                'Step 13: Split VIN-DOT Matched Files',
                'Step 14: Upload Census Files To FTP',
                'Step 15: Upload Inspection Files To FTP',
                'Step 16: Upload Insp_Pub Files To FTP',
                'Step 17: Upload DOT-VIN Files To FTP',
                'Step 18: Upload VIN-DOT Files To FTP',
                'Step 19: Upload VIN-DOT Matched Files to FTP',
                'Step 21: Initiate Census MySQL Merge',
                'Step 22: Initiate Inspections MySQL Merge',
                'Step 23: Initiate Insp_Unit MySQL Merge',
                'Step 23: Initiate DOT-VIN Files MySQL Merge',
                'Step 25: Initiate VIN-DOT Files MySQL Merge',
                'Step 26: Initiate VIN-DOT Matched MySQL Merge',
                'Step 27: Initiate Auth Hist MySQL Merge',
                'Step 28: Initiate Insp Counts MySQL Merge',
                'Step 29: Analize Insurance Data',
                'Restart the Script',
                'Exit'
            ],
        )
    ]
    return inquirer.prompt(questions)


#region ### SHARED FUNCTIONS ###

# Split Files.
def split_files(input_file_path, output_directory):
    """
    Splits a CSV file into smaller chunks.

    Parameters:
    - input_file_path: The full path to the input CSV file.
    - output_directory: The directory where the split files will be saved.
    """
    os.makedirs(output_directory, exist_ok=True)
    base_file_name = os.path.basename(input_file_path).replace('.csv', '')
    part_number = 1

    with open(input_file_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        outfile = None
        for i, row in enumerate(reader):
            if i % LINES_PER_FILE == 0:
                if outfile is not None:
                    outfile.close()
                part_file_name = f"{base_file_name}_part_{part_number}.csv"
                outfile = open(os.path.join(output_directory, part_file_name), 'w', newline='', encoding='utf-8')
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()
                part_number += 1
            writer.writerow(row)
        if outfile is not None:
            outfile.close()
    logging.info(f"File {base_file_name} split into {part_number-1} parts.")

# Upload files to FTP server.
def upload_files_to_ftp(directory, target_dir, description):
    FTP_HOST = "195.179.237.122"
    FTP_USER = "u895992627"
    FTP_PASS = "Supra1122!"

    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            logging.info(f"Connected to FTP server: {FTP_HOST}")

            # Check if target directory exists and create if not
            try:
                ftp.cwd(target_dir)
                logging.info(f"Changed to FTP directory: {target_dir}")
            except error_perm as e:
                if str(e).startswith('550'):
                    # Directory does not exist, attempt to create it
                    try:
                        ftp.mkd(target_dir)
                        logging.info(f"Created directory: {target_dir}")
                        time.sleep(4)  # Wait for 2 seconds
                        ftp.cwd(target_dir)
                        logging.info(f"Changed to directory: {target_dir}")
                    except error_perm as e:
                        logging.error(f"Failed to create or change to directory: {e}")
                        return
                else:
                    logging.error(f"FTP error: {e}")
                    return

            files = os.listdir(directory)
            for filename in files:
                filepath = os.path.join(directory, filename)
                with open(filepath, 'rb') as file:
                    ftp.storbinary(f'STOR {filename}', file)
                logging.info(f"Uploaded: {filename}")

            logging.info("All files uploaded to FTP successfully.")
    except Exception as e:
        logging.error(f"FTP upload failed: {e}")

# Remove WhiteSpace and Hidden Characters.
def clean_row(row):
    """Remove hidden characters and strip whitespace."""
    return {k: v.replace('\n', '').replace('\r', '').strip() for k, v in row.items()}


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}


# Call Carrier MySQL data merger.
def call_data_merger(url):
    startIndex = 0
    response = requests.get(f"{url}?start={startIndex}", headers=headers)
    totalFiles = None
    completed = False

    console = Console()

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        SpinnerColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Data Merger Progress", total=100)

        while not completed:
            response = requests.get(f"{url}?start={startIndex}")

            try:
                response_json = response.json()
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON response. Status Code: {response.status_code}, Response: {response.text}")
                break

            startIndex = response_json.get('nextStartIndex', startIndex)
            totalFiles = response_json.get('totalFiles', totalFiles)
            completed = response_json.get('completed', False)

            if totalFiles is not None:
                progress.update(task, total=totalFiles, completed=startIndex)

            if response_json.get('errors'):
                logging.error(f"Errors encountered: {response_json['errors']}")

    if completed:
        logging.info("All files processed and merged successfully.")
    else:
        logging.error(f"Data merging process was not completed successfully. Current startIndex: {startIndex}, Total files: {totalFiles}")

#endregion


#region ###CARRIER PROCESSING FUNCTIONS###

# Step1: Process Carrier and Combine Census Files.
def combine_and_process_census_files():
    output_folder = 'Processed Files'
    os.makedirs(output_folder, exist_ok=True)
    combined_file_path = os.path.join(output_folder, 'combined_census.csv')
    dot_numbers = set()
    processed_rows = 0

    logging.info("Starting to combine and process Census files.")
    with open(combined_file_path, 'w', newline='', encoding='utf-8') as combined_file:
        writer = csv.DictWriter(combined_file, fieldnames=columns_to_keep)
        writer.writeheader()

        for file_path in glob.glob('CENSUS_PUB*.csv'):
            file_processed = False
            for encoding in ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']:
                try:
                    with open(file_path, 'r', encoding=encoding) as infile:
                        reader = csv.DictReader(infile, delimiter='~')
                        for row in reader:
                            cleaned_row = clean_row(row)  # Make sure to define this function
                            if cleaned_row.get('ICC_DOCKET_1_PREFIX') == 'MC' and cleaned_row.get('DOT_NUMBER') not in dot_numbers and all(key in cleaned_row for key in columns_to_keep):
                                dot_numbers.add(cleaned_row['DOT_NUMBER'])
                                writer.writerow({k: cleaned_row.get(k, '') for k in columns_to_keep})
                                processed_rows += 1
                    file_processed = True
                    break  # Break if successful for a given encoding
                except UnicodeDecodeError:
                    continue  # Try next encoding if current one fails
            if not file_processed:
                logging.error(f"Failed to process {file_path} with specified encodings.")

    logging.info(f"Combination and processing of Census files completed. Processed rows: {processed_rows}, Unique DOT numbers found: {len(dot_numbers)}")
    
    # Save DOT numbers from processed combined file
    dot_numbers_file = os.path.join(output_folder, 'dot_numbers.txt')
    with open(dot_numbers_file, 'w') as file:
        for dot_number in sorted(dot_numbers):
            file.write(dot_number + '\n')
    logging.info(f"DOT Numbers Saved To {dot_numbers_file}")

# Step2: Split Carrier Processed files into smaller files.
def split_processed_files(input_directory):
    global LINES_PER_FILE
    csv.field_size_limit(2147483647)
    split_folder = 'Split and Ready Files'
    os.makedirs(split_folder, exist_ok=True)
    console = Console()

    for filename in glob.glob(os.path.join(input_directory, '*.csv')):
        base_file_name = os.path.splitext(os.path.basename(filename))[0]
        part_number = 1
        line_count = 0

        with open(filename, 'r', encoding='utf-8') as infile:
            total_rows = sum(1 for _ in infile)  # Count total rows for progress bar
            infile.seek(0)  # Reset file pointer to beginning
            reader = csv.DictReader(infile)
            outfile = None

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                SpinnerColumn(),
            ) as progress:
                task = progress.add_task(f"Splitting {base_file_name}", total=total_rows)

                for row in reader:
                    if line_count % LINES_PER_FILE == 0:
                        if outfile is not None:
                            outfile.close()
                            progress.update(task, advance=LINES_PER_FILE)
                        new_file_name = os.path.join(split_folder, f"{base_file_name}_part_{part_number}.csv")
                        outfile = open(new_file_name, 'w', newline='', encoding='utf-8')
                        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        part_number += 1

                    writer.writerow(row)
                    line_count += 1

                if outfile is not None and not outfile.closed:
                    outfile.close()
                    remaining_rows = line_count % LINES_PER_FILE
                    progress.update(task, advance=remaining_rows if remaining_rows else LINES_PER_FILE)

                progress.update(task, completed=total_rows)  # Ensure task is marked as completed

            logging.info(f"File {base_file_name} split into {part_number - 1} parts.")

# Step3: Count Carrier DOT numbers from processed files.
def count_dot_numbers_in_census_files():
    directory = 'Split and Ready Files'
    dot_numbers = set()  # Use a set to automatically ensure uniqueness
    for filename in glob.glob(os.path.join(directory, '*.csv')):
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'DOT_NUMBER' in row:
                    dot_numbers.add(row['DOT_NUMBER'])
    
    # Save the unique DOT numbers to a file
    with open('census_dot_numbers.txt', 'w') as f:
        for dot_number in sorted(dot_numbers):  # Sorting for easier reference
            f.write(dot_number + '\n')
    
    logging.info(f"Counted and saved {len(dot_numbers)} unique DOT numbers from census files.")

#endregion



#region ###CARRIER INSPECTION PROCESSING FUNCTIONS###

# Process Inspection Files: Extract, Combine, Filter by Column and Date

def process_inspection_files(directory="."):
    dot_numbers_path = os.path.join(directory, 'Processed Files', 'dot_numbers.txt')
    combined_file_path = os.path.join(directory, "Processed Files/Inspections", "combined_filtered_inspections.csv")
    encodings = ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']

    # Load carrier DOT numbers
    with open(dot_numbers_path, 'r') as file:
        carrier_dot_numbers = set(file.read().splitlines())

    all_data_frames = []
    latest_date = None

    # Process each file
    for file_path in glob.glob(os.path.join(directory, 'Insp_Pub*.txt')):
        logging.info(f"Processing file: {file_path}")
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, delimiter='\t', usecols=insp_columns_to_keep, dtype={'DOT_NUMBER': 'str'}, encoding=encoding)
                # Convert INSP_DATE to datetime
                df['INSP_DATE'] = pd.to_datetime(df['INSP_DATE'], format='%Y%m%d', errors='coerce')
                # Keep rows with valid dates and DOT numbers in the list
                df = df[df['DOT_NUMBER'].isin(carrier_dot_numbers) & df['INSP_DATE'].notna()]
                # Update latest inspection date
                max_date_in_file = df['INSP_DATE'].max()
                if latest_date is None or max_date_in_file > latest_date:
                    latest_date = max_date_in_file
                all_data_frames.append(df)
                break  # Successfully read with current encoding
            except UnicodeDecodeError:
                continue  # Try next encoding if current one fails
        else:
            logging.error(f"Failed to process {file_path} with specified encodings.")

    if not all_data_frames:
        logging.error("No inspection data found. Please check your directory and file names.")
        return

    # Combine all data frames
    combined_df = pd.concat(all_data_frames, ignore_index=True)

    # Filter data to the last 24 months
    if latest_date:
        cutoff_date = latest_date - relativedelta(months=24)
        filtered_df = combined_df[combined_df['INSP_DATE'] >= cutoff_date]
    else:
        filtered_df = combined_df

    # Save the filtered data
    filtered_df.to_csv(combined_file_path, index=False, columns=insp_columns_to_keep)
    logging.info(f"Filtered inspection data saved to {combined_file_path}")

    dot_numbers_path = os.path.join(directory, 'Processed Files', 'dot_numbers.txt')
    combined_file_path = os.path.join(directory, "Processed Files/Inspections", "combined_filtered_inspections.csv")
    dot_inspection_map_path = os.path.join(directory, "Processed Files/Inspections", "dot_inspection_map.csv")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(combined_file_path), exist_ok=True)

    # Load carrier DOT numbers
    with open(dot_numbers_path, 'r') as file:
        carrier_dot_numbers = set(file.read().splitlines())
    logging.info(f"Loaded {len(carrier_dot_numbers)} DOT numbers for processing.")

    latest_date = None
    inspection_ids = set()
    dot_inspection_map = defaultdict(set)
    rows_processed = 0
    skipped_rows_due_to_date = 0

    # Open the combined file once for writing headers and then appending rows
    with open(combined_file_path, 'w', newline='', encoding='utf-8') as f_combined:
        writer = csv.DictWriter(f_combined, fieldnames=insp_columns_to_keep)
        writer.writeheader()

        files_found = glob.glob(os.path.join(directory, 'Insp_Pub*.txt'))
        logging.info(f"Found {len(files_found)} inspection files to process.")

        for file_path in files_found:
            logging.info(f"Processing file: {file_path}")
            for encoding in ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']:
                try:
                    with open(file_path, 'r', encoding=encoding) as infile:
                        reader = csv.DictReader(infile, delimiter='\t')
                        for row in reader:
                            dot_number = row.get("DOT_NUMBER")
                            if dot_number in carrier_dot_numbers and "INSP_DATE" in row and row["INSP_DATE"].strip():
                                insp_date = datetime.strptime(row["INSP_DATE"], "%Y%m%d")
                                if not latest_date or insp_date > latest_date:
                                    latest_date = insp_date
                                dot_inspection_map[dot_number].add(row["INSPECTION_ID"])
                                inspection_ids.add(row["INSPECTION_ID"])
                                writer.writerow({k: row.get(k) for k in insp_columns_to_keep if k in row})
                                rows_processed += 1
                    break  # Break after successful processing
                except UnicodeDecodeError:
                    continue  # Try the next encoding if current one fails

    logging.info(f"Processed {rows_processed} rows from {len(files_found)} files. Skipped {skipped_rows_due_to_date} rows due to missing INSP_DATE.")

    temp_file_path = combined_file_path + ".tmp"
    with open(combined_file_path, 'r', encoding='utf-8') as infile, open(temp_file_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=insp_columns_to_keep)
        writer.writeheader()
        for row in reader:
            insp_date = datetime.strptime(row.get("INSP_DATE", ""), "%Y%m%d")
            if insp_date >= cutoff_date:
                writer.writerow(row)
    os.replace(temp_file_path, combined_file_path)
    logging.info(f"Filtered inspections saved to {combined_file_path}, covering the last 24 months.")

    combined_inspection_path = os.path.join(directory, "Processed Files/Inspections", "combined_filtered_inspections.csv")
    combined_insp_units_path = os.path.join(directory, "Processed Files/Inspections", "combined_insp_units.csv")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(combined_insp_units_path), exist_ok=True)

    # Load inspection IDs from the combined inspection file
    inspection_df = pd.read_csv(combined_inspection_path, usecols=['INSPECTION_ID'], dtype={'INSPECTION_ID': str})
    inspection_ids = set(inspection_df['INSPECTION_ID'].unique())
    logging.info(f"Loaded {len(inspection_ids)} unique inspection IDs for processing.")

    # Initialize an empty DataFrame for combining Insp_Unit_Pub files
    combined_units_df = pd.DataFrame()

    files_found = glob.glob(os.path.join(directory, 'Insp_Unit_Pub*.txt'))
    logging.info(f"Found {len(files_found)} Insp_Unit_Pub files to process.")

    # Process each Insp_Unit_Pub file
    for file_path in files_found:
        logging.info(f"Processing file: {file_path}")
        try:
            unit_df = pd.read_csv(file_path, delimiter='\t', dtype={'INSPECTION_ID': str}, low_memory=False)
            # Filter rows where INSPECTION_ID is in the list of IDs from the combined inspection file
            filtered_unit_df = unit_df[unit_df['INSPECTION_ID'].isin(inspection_ids)]

            # Combine filtered data
            combined_units_df = pd.concat([combined_units_df, filtered_unit_df], ignore_index=True)

        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            continue

    # Save the combined and filtered Insp_Unit_Pub data to CSV
    combined_units_df.to_csv(combined_insp_units_path, index=False)
    logging.info(f"Combined Insp_Unit_Pub data saved to {combined_insp_units_path}.")

def process_and_combine_insp_unit_files(directory="."):
    combined_inspection_path = os.path.join(directory, "Processed Files/Inspections", "combined_filtered_inspections.csv")
    combined_insp_units_path = os.path.join(directory, "Processed Files/Inspections", "combined_insp_units.csv")
    encodings = ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']  # List of encodings to try

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(combined_insp_units_path), exist_ok=True)

    # Load inspection IDs from the combined inspection file
    inspection_df = pd.read_csv(combined_inspection_path, usecols=['INSPECTION_ID'], dtype={'INSPECTION_ID': 'str'})
    inspection_ids = set(inspection_df['INSPECTION_ID'].unique())
    logging.info(f"Loaded {len(inspection_ids)} unique inspection IDs for processing.")

    files_found = glob.glob(os.path.join(directory, 'Insp_Unit_Pub*.txt'))
    logging.info(f"Found {len(files_found)} Insp_Unit_Pub files to process.")

    # Initialize writer only once and write header
    with open(combined_insp_units_path, 'w', newline='', encoding='utf-8') as f_out:
        writer = None

        # Process each Insp_Unit_Pub file
        for file_path in files_found:
            logging.info(f"Processing file: {file_path}")
            for encoding in encodings:
                try:
                    unit_df = pd.read_csv(file_path, delimiter='\t', dtype={'INSPECTION_ID': 'str'}, encoding=encoding, low_memory=False)
                    # Filter rows where INSPECTION_ID is in the list of IDs from the combined inspection file
                    filtered_unit_df = unit_df[unit_df['INSPECTION_ID'].isin(inspection_ids)]
                    
                    if writer is None:
                        writer = csv.DictWriter(f_out, fieldnames=filtered_unit_df.columns)
                        writer.writeheader()
                    
                    filtered_unit_df.to_csv(f_out, header=False, index=False)
                    logging.debug(f"Successfully processed {file_path} with encoding: {encoding}")
                    break  # Success, move to the next file
                except UnicodeDecodeError:
                    continue  # Try next encoding
                except Exception as e:
                    logging.error(f"Error processing file {file_path} with encoding {encoding}: {e}")
                    break  # Move to the next file if other errors occur

    logging.info(f"Combined Insp_Unit_Pub data saved to {combined_insp_units_path}.")

#endregion


#region ###MAP DOT TO INSPECTION DATA AND ADD TO CENSUS###

def enhance_census_with_inspection_data():
    census_file_path = "./Processed Files/combined_census.csv"
    inspections_file_path = "./Processed Files/Inspections/combined_filtered_inspections.csv"
    
    # Load the census data with low_memory=False to avoid mixed types warning
    census_df = pd.read_csv(census_file_path, dtype={'DOT_NUMBER': 'str'}, low_memory=False)

    # Load the inspections data with specified data types
    inspections_df = pd.read_csv(inspections_file_path, dtype={
        'DOT_NUMBER': 'str',
        'INSPECTION_ID': 'str',
        'INSP_LEVEL_ID': 'str',
        'VEHICLE_OOS_TOTAL': 'int',
        'DRIVER_OOS_TOTAL': 'int',
        'HAZMAT_OOS_TOTAL': 'int'  # Assuming this field exists and is numeric
    })

    # Prepare a data structure to hold aggregated inspection data
    inspection_data_map = defaultdict(lambda: {
        'insp_ids': set(),  # Use a set to avoid duplicate IDs
        'veh_insp_count': 0,
        'drv_insp_count': 0,
        'veh_insp_oos': 0,
        'drv_insp_oos': 0,
        'hzmt_insp_count': 0,
        'hzmt_insp_oos': 0
    })

    # Iterate through each row of inspections dataframe to count inspections and OOS
    for _, row in inspections_df.iterrows():
        dot_number = row['DOT_NUMBER']
        in_data = inspection_data_map[dot_number]
        in_data['insp_ids'].add(row['INSPECTION_ID'])

        inspection_level = row['INSP_LEVEL_ID']
        
        # Increment if values are greater than 0
        is_oos_veh = 1 if row['VEHICLE_OOS_TOTAL'] > 0 else 0
        is_oos_drv = 1 if row['DRIVER_OOS_TOTAL'] > 0 else 0
        is_oos_hzmt = 1 if row.get('HAZMAT_OOS_TOTAL', 0) > 0 else 0

        if inspection_level in ['1', '2', '5', '6']:
            in_data['veh_insp_count'] += 1
            in_data['veh_insp_oos'] += is_oos_veh

        if inspection_level in ['1', '2', '3', '6']:
            in_data['drv_insp_count'] += 1
            in_data['drv_insp_oos'] += is_oos_drv

        if inspection_level in ['1', '2', '3', '4', '5', '6'] and row.get('HAZMAT_PLACARD_REQ', 'N') == 'Y':
            in_data['hzmt_insp_count'] += 1
            in_data['hzmt_insp_oos'] += is_oos_hzmt

    # Convert the inspection data map to a DataFrame for merging and handle INSPECTION_IDS correctly
    inspection_data_df = pd.DataFrame.from_dict(inspection_data_map, orient='index', columns=['insp_ids', 'veh_insp_count', 'drv_insp_count', 'veh_insp_oos', 'drv_insp_oos', 'hzmt_insp_count', 'hzmt_insp_oos']).reset_index()
    inspection_data_df['DOT_NUMBER'] = inspection_data_df['index']
    inspection_data_df.drop('index', axis=1, inplace=True)
    inspection_data_df['INSPECTION_IDS'] = inspection_data_df['insp_ids'].apply(lambda x: ', '.join(x))  # Convert set of IDs to a comma-separated string
    inspection_data_df.drop('insp_ids', axis=1, inplace=True)  # Remove the original set column

    # Merge the aggregated inspection data with the census data
    enhanced_census_df = pd.merge(census_df, inspection_data_df, on='DOT_NUMBER', how='left')

    # Fill NaN values for newly created columns after merge
    fill_values = {'INSPECTION_IDS': '', 'veh_insp_count': 0, 'drv_insp_count': 0, 'veh_insp_oos': 0, 'drv_insp_oos': 0, 'hzmt_insp_count': 0, 'hzmt_insp_oos': 0}
    enhanced_census_df.fillna(fill_values, inplace=True)

    # Apply type conversion to ensure integer types
    int_columns = ['veh_insp_count', 'drv_insp_count', 'veh_insp_oos', 'drv_insp_oos', 'hzmt_insp_count', 'hzmt_insp_oos']
    enhanced_census_df[int_columns] = enhanced_census_df[int_columns].astype(int)
    
    # Correctly format numeric columns to avoid decimals in original data
    for col in enhanced_census_df.select_dtypes(include=['float64']).columns:
        if col not in int_columns:  # Skip the already converted columns
            enhanced_census_df[col] = enhanced_census_df[col].apply(lambda x: int(x) if x == x else "")

    # Save the enhanced census data
    enhanced_file_path = census_file_path.replace(".csv", "_enhanced.csv")
    enhanced_census_df.to_csv(enhanced_file_path, index=False)

    print(f"Enhanced census data saved to: {enhanced_file_path}")

def vin_to_dot_all(directory):
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    # Define file paths
    inspections_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_filtered_inspections.csv')
    insp_units_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_insp_units.csv')

    # Load data
    dtype_dict_inspections = {
        'INSPECTION_ID': 'int64',
        'DOT_NUMBER': 'object'
    }
    dtype_dict_units = {
        'INSPECTION_ID': 'int64',
        'INSP_UNIT_VEHICLE_ID_NUMBER': 'object'
    }

    inspections_ddf = dd.read_csv(inspections_file_path, usecols=dtype_dict_inspections.keys(), dtype=dtype_dict_inspections)
    insp_units_ddf = dd.read_csv(insp_units_file_path, usecols=dtype_dict_units.keys(), dtype=dtype_dict_units)

    # Merge inspections and units dataframes
    merged_ddf = dd.merge(insp_units_ddf, inspections_ddf, on='INSPECTION_ID')

    # Filter for 17-character VINs
    merged_ddf = merged_ddf[merged_ddf['INSP_UNIT_VEHICLE_ID_NUMBER'].str.len() == 17]

    # Group by VIN and aggregate DOT numbers
    def agg_dots(x):
        # Ensure unique DOT numbers are joined as a string
        return ', '.join(x.unique())

    vin_to_dot_agg = merged_ddf.groupby('INSP_UNIT_VEHICLE_ID_NUMBER')['DOT_NUMBER'].apply(agg_dots, meta=('DOT_NUMBER', 'object')).compute().reset_index()

    # Save the output to a CSV file
    output_file_path = os.path.join(directory, 'Processed Files/Data Maps', 'vin_to_dot_all.csv')
    vin_to_dot_agg.to_csv(output_file_path, index=False)

    print(f"VIN to DOT mapping saved to: {output_file_path}")

def dot_to_vin_all(directory):
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    # Define file paths
    inspections_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_filtered_inspections.csv')
    insp_units_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_insp_units.csv')

    # Load data
    dtype_dict_inspections = {
        'INSPECTION_ID': 'int64',
        'DOT_NUMBER': 'object'
    }
    dtype_dict_units = {
        'INSPECTION_ID': 'int64',
        'INSP_UNIT_VEHICLE_ID_NUMBER': 'object'
    }

    inspections_ddf = dd.read_csv(inspections_file_path, usecols=dtype_dict_inspections.keys(), dtype=dtype_dict_inspections)
    insp_units_ddf = dd.read_csv(insp_units_file_path, usecols=dtype_dict_units.keys(), dtype=dtype_dict_units)

    # Merge inspections and units dataframes
    merged_ddf = dd.merge(inspections_ddf, insp_units_ddf, on='INSPECTION_ID')

    # Group by DOT_NUMBER and aggregate VINs, handling NA values
    def agg_vins(x):
        # Filter out NA values before joining
        valid_vins = x.dropna().unique()
        return ', '.join(valid_vins)

    dot_to_vin_agg = merged_ddf.groupby('DOT_NUMBER')['INSP_UNIT_VEHICLE_ID_NUMBER'].apply(agg_vins, meta=('INSP_UNIT_VEHICLE_ID_NUMBER', 'object')).compute().reset_index()

    # Save the output to a CSV file
    output_file_path = os.path.join(directory, 'Processed Files/Data Maps', 'dot_to_vin_all.csv')
    dot_to_vin_agg.to_csv(output_file_path, index=False)

    print(f"DOT to VIN mapping saved to: {output_file_path}")

def vin_to_dot_matchonly(directory):
    cluster = LocalCluster()
    client = Client(cluster)
    print(f"Dask Dashboard available at: {client.dashboard_link}")

    # Define file paths
    inspections_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_filtered_inspections.csv')
    insp_units_file_path = os.path.join(directory, 'Processed Files/Inspections', 'combined_insp_units.csv')

    # Load data
    dtype_dict_inspections = {
        'INSPECTION_ID': 'int64',
        'DOT_NUMBER': 'object'
    }
    dtype_dict_units = {
        'INSPECTION_ID': 'int64',
        'INSP_UNIT_VEHICLE_ID_NUMBER': 'object'
    }

    inspections_ddf = dd.read_csv(inspections_file_path, usecols=dtype_dict_inspections.keys(), dtype=dtype_dict_inspections)
    insp_units_ddf = dd.read_csv(insp_units_file_path, usecols=dtype_dict_units.keys(), dtype=dtype_dict_units)

    # Merge inspections and units dataframes
    merged_ddf = dd.merge(insp_units_ddf, inspections_ddf, on='INSPECTION_ID')

    # Filter for 17-character VINs
    merged_ddf = merged_ddf[merged_ddf['INSP_UNIT_VEHICLE_ID_NUMBER'].str.len() == 17]

    # Group by VIN and aggregate DOT numbers
    def agg_dots(x):
        # Ensure unique DOT numbers are joined as a string
        return ', '.join(x.unique())

    vin_to_dot_agg = merged_ddf.groupby('INSP_UNIT_VEHICLE_ID_NUMBER')['DOT_NUMBER'].apply(agg_dots, meta=('DOT_NUMBER', 'object')).compute().reset_index()

    # Filter for VINs matched to more than one DOT
    vin_to_dot_agg['DOT_COUNT'] = vin_to_dot_agg['DOT_NUMBER'].apply(lambda x: len(x.split(', ')))
    filtered_vin_to_dot_agg = vin_to_dot_agg[vin_to_dot_agg['DOT_COUNT'] > 1].drop(columns=['DOT_COUNT'])

    # Save the output to a CSV file
    output_file_path = os.path.join(directory, 'Processed Files/Data Maps', 'filtered_vin_to_dot_mapping.csv')
    filtered_vin_to_dot_agg.to_csv(output_file_path, index=False)

    print(f"Filtered VIN to DOT mapping saved to: {output_file_path}")

#endregion

#region ###CARRIER INSURANCE PROCESSING FUNCTIONS###

def process_and_analyze_insurance_data_backup(directory):
    def read_and_normalize(file_path, lowercase_columns=False):
        try:
            df = pd.read_csv(file_path, delimiter='~', encoding='ISO-8859-1', low_memory=False)
            if lowercase_columns:
                df.columns = df.columns.str.upper()
            return df
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return pd.DataFrame()

    # Load data
    active_df = read_and_normalize(os.path.join(directory, 'Raw Data/Insurance Data/CENSUS_INS_ACTIVE_HDR_FOIA_20240109.txt'))
    pend_df = read_and_normalize(os.path.join(directory, 'Raw Data/Insurance Data/CENSUS_INS_PEND_HDR_FOIA_20240109.txt'))
    revoc_df = read_and_normalize(os.path.join(directory, 'Raw Data/Insurance Data/CENSUS_INS_REVOCATION_NOAUTH_HDR_FOIA_20240109.txt'), True)
    hist_dfs = [read_and_normalize(f) for f in glob.glob(os.path.join(directory, 'Raw Data/Insurance Data/CENSUS_INS_HIST_HDR_FOIA_*.txt'))]
    hist_df = pd.concat(hist_dfs, ignore_index=True) if hist_dfs else pd.DataFrame()

    # Combine all dataframes and handle duplicates
    combined_df = pd.concat([active_df, pend_df, hist_df], ignore_index=True).drop_duplicates(subset=['DOT_NUMBER'], keep='last')
    
    # Correctly calculate 'COVERAGE_SUFFICIENT', 'FREQUENCY_OF_APPLICATIONS', and 'HISTORICAL_LAPSES'
    combined_df['COVERAGE_SUFFICIENT'] = combined_df['MAX_COV_AMOUNTX1K'].astype(float).apply(lambda x: 'Yes' if x >= 750 else 'No')
    combined_df['FREQUENCY_OF_APPLICATIONS'] = combined_df['POLICY_NO'].notnull().astype(int)
    combined_df['HISTORICAL_LAPSES'] = 0  # Placeholder, adjust based on your data
    
    # Merge with revocation reasons and update ACTIVE status based on presence in active_df
    active_dot_numbers = active_df['DOT_NUMBER'].unique()
    combined_df['ACTIVE'] = combined_df['DOT_NUMBER'].apply(lambda x: 'Yes' if x in active_dot_numbers else 'No')
    combined_df = combined_df.merge(revoc_df[['DOT_NUMBER', 'REASON']], on='DOT_NUMBER', how='left')

    # Adjust authority classification logic
    def classify_authority(row):
        if row['ACTIVE'] == 'Yes' and pd.isna(row['REASON']):
            return 'High'
        elif row['ACTIVE'] == 'Yes' and row['REASON'] == 'INVOLUNTARY REVOCATION':
            return 'Medium'
        return 'Low'

    combined_df['AUTHORITY_CLASSIFICATION'] = combined_df.apply(classify_authority, axis=1)

    # Select the most recent record for each DOT_NUMBER
    combined_df['EFFECTIVE_DATE'] = pd.to_datetime(combined_df['EFFECTIVE_DATE'], errors='coerce')
    latest_df = combined_df.sort_values('EFFECTIVE_DATE', ascending=False).drop_duplicates('DOT_NUMBER')

    # Prepare the final DataFrame
    output_columns = ['DOT_NUMBER', 'LEGAL_NAME', 'ACTIVE', 'COVERAGE_SUFFICIENT', 'FREQUENCY_OF_APPLICATIONS', 'HISTORICAL_LAPSES', 'REASON', 'AUTHORITY_CLASSIFICATION']
    final_df = latest_df[output_columns]

    # Save to CSV
    final_path = os.path.join(directory, 'final_insurance_analysis.csv')
    final_df.to_csv(final_path, index=False)
    print("Data analysis completed and saved to:", final_path)





    dtypes = {
        'MAIL_COLONIA': 'object',
        'RFC_NUMBER': 'object',
        'ZIP_CODE': 'object',
        'MAIL_FAX': 'object',
        'MAIL_TELNO': 'object',
        'INS_FORM_CODE': 'object',
        'MAX_COV_AMOUNTX1K': 'object',
        'BUS_ZIP_CODE': 'object',
        'UNDERL_LIM_AMOUNT': 'float64',
        'MAIL_ZIP_CODE': 'object',
        'TELE_NUM': 'float64',
        'DBA_NAME': 'object',
        'BUS_FAX': 'object'
        
    }

# endregion

# Main Function.
def main():

# region ###VARIABLES###
    answers = main_menu()  # Assumes this returns user choices as before
    directory = '.'  # or explicitly set to your script's running directory
# endregion

# region ### MENU FUNCTIONS ###
    
    # Step 1: Process Carrier CSV Files
    if 'Step 1: Process Carrier CSV Files' in answers['operations']:
        combine_and_process_census_files()

    # Step 2: Process Inspection Archives
    if 'Step 2: Process Inspection CSV Files' in answers['operations']:
        process_inspection_files(".")

    # Step 3: Process Insp_Units Files
    if 'Step 3: Process Insp/VIN CSV Files' in answers['operations']:    
        process_and_combine_insp_unit_files(directory=".")

    # Step 4: Enhance Census with Violation Data
    if 'Step 4: Enhance Census with Violation Data' in answers['operations']:
        enhance_census_with_inspection_data()

    # Step 5: Create DOTs to VIN File
    if 'Step 5: Create DOTs to VIN File' in answers['operations']:
        dot_to_vin_all(directory)

    # Step 6: Create VIN to DOTs File
    if 'Step 6: Create VIN to DOTs File' in answers['operations']:
        vin_to_dot_all(directory)

    # Step 7: Create VIN to DOTs MATCHED ONLY File
    if 'Step 7: Create VIN to DOTs MATCHED Only File' in answers['operations']:
        vin_to_dot_matchonly(directory)

    # Step 8: Split processed Census Files.
    if 'Step 8: Split Processed Census Files' in answers['operations']:
            input_file_path = 'Processed Files/combined_census_enhanced.csv'
            output_directory = 'Processed Files/Split Data/Census'
            split_files(input_file_path, output_directory)

    # Step 9: Split processed Inspection Files.
    if 'Step 9: Split Processed Inspection Files' in answers['operations']:
            input_file_path = 'Processed Files/Inspections/combined_filtered_inspections.csv'
            output_directory = 'Processed Files/Split Data/Inspections'
            split_files(input_file_path, output_directory)

    # Step 10: Split processed Insp_Unit Files.
    if 'Step 10: Split Processed Insp_Unit Files' in answers['operations']:
            input_file_path = 'Processed Files/Inspections/combined_insp_units.csv'
            output_directory = 'Processed Files/Split Data/Insp_Unit'
            split_files(input_file_path, output_directory)

    # Step 11: Split DOT-VIN Files.
    if 'Step 11: Split DOT-VIN Files' in answers['operations']:
            input_file_path = 'Processed Files/Data Maps/dot_to_vin_all.csv'
            output_directory = 'Processed Files/Split Data/DOT to VIN'
            split_files(input_file_path, output_directory)

    # Step 12: Split VIN-DOT Files.
    if 'Step 12: Split VIN-DOT Files' in answers['operations']:
            input_file_path = 'Processed Files/Data Maps/vin_to_dot_all.csv'
            output_directory = 'Processed Files/Split Data/VIN to DOT'
            split_files(input_file_path, output_directory)
 
    # Step 13: Split VIN-DOT Matched Files.
    if 'Step 13: Split VIN-DOT Matched Files' in answers['operations']:
            input_file_path = 'Processed Files/Data Maps/filtered_vin_to_dot_mapping.csv'
            output_directory = 'Processed Files/Split Data/Matched VINs'
            split_files(input_file_path, output_directory)

    # Step 14: Upload Census Files to FTP
    if  'Step 14: Upload Census Files To FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/Census'
        target_upload_dir = 'ld/dataset/Carriers'
        description = "Uploading Census Files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 15: Upload Inspection Files to FTP
    if 'Step 15: Upload Inspection Files To FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/Inspections'
        target_upload_dir = 'ld/dataset/Inspections'
        description = "Uploading files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 16: Upload Insp_Pub Files to FTP
    if 'Step 16: Upload Insp_Pub Files To FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/Insp_Unit'
        target_upload_dir = 'ld/dataset/insp_unit_vin'
        description = "Uploading files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 17: Upload DOT-VIN Files to FTP
    if 'Step 17: Upload DOT-VIN Files To FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/DOT to VIN'
        target_upload_dir = 'ld/dataset/dot_vin'
        description = "Uploading files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 18: Upload VIN-DOT Files to FTP
    if 'Step 18: Upload VIN-DOT Files To FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/VIN to DOT'
        target_upload_dir = 'ld/dataset/vin_dot'
        description = "Uploading files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 19: Upload VIN-DOT Matched Files to FTP
    if 'Step 19: Upload VIN-DOT Matched Files to FTP' in answers['operations']:
        input_dir = 'Processed Files/Split Data/Matched VINs'
        target_upload_dir = 'ld/dataset/vin_matched'
        description = "Uploading files"
        upload_files_to_ftp(input_dir, target_upload_dir, description)

    # Step 20: Initiate Census MySQL Merge
    if 'Step 21: Initiate Census MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/census_merge.php")

    # Step 21: Initiate Inspections MySQL Merge
    if 'Step 21: Initiate Inspections MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/insp_merge.php")

    # Step 22: Initiate Insp_Unit MySQL Merge
    if 'Step 22: Initiate Insp_Unit MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/insp_unit_merge.php")

    # Step 23: Initiate DOT-VIN Files MySQL Merge
    if 'Step 23: Initiate DOT-VIN Files MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/dot_vin_merge.php")

    # Step 24: Initiate VIN-DOT Files MySQL Merge
    if 'Step 24: Initiate VIN-DOT Files MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/vin_dot_merge.php")

    # Step 25: Initiate VIN-DOT Matched MySQL Merge
    if 'Step 26: Initiate VIN-DOT Matched MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/vin_matched_merge.php")

    # Step 26: Initiate Auth Hist MySQL Merge
    if 'Step 27: Initiate Auth Hist MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/auth_hist_all.php")

    # Step 27: Initiate Insp Counts MySQL Merge
    if 'Step 28: Initiate Insp Counts MySQL Merge' in answers['operations']:
        call_data_merger("https://loadguard.ai/ld/mergefuncs/insp_counts.php")

    # Step 28: Analyze Insurance Data
    if 'Step 29: Analize Insurance Data' in answers['operations']:
        process_and_analyze_insurance_data_backup(directory)

    # Restart Operation
    if 'Restart the Script' in answers['operations']:
        print("Restarting script...")
        os.execl(sys.executable, sys.executable, *sys.argv)
        
    # Exit Operation
    if 'Exit' in answers['operations']:
        logging.info("Exiting the program.")
        sys.exit()

# endregion
if __name__ == '__main__':
    main()
