import csv
import json
import glob
import os
import sys
import zipfile
import logging
from concurrent.futures import ProcessPoolExecutor
from ftplib import FTP
import requests
import inquirer
from tqdm import tqdm
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    SpinnerColumn,
    TotalFileSizeColumn,
)
from rich.console import Console
from collections import defaultdict



# Define ANSI escape codes for magenta and end color.
MAGENTA = '\033[95m'
BLACK = '\033[30m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
CYAN = '\033[36m'
WHITE = '\033[37m'
ENDC = '\033[0m'

# Setup logging.
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

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
                'Process And Split Carrier CSV Files',
                'Upload Filtered And Split Carrier Files To FTP',
                'Initiate Carrier Data Merge With Database',
                'Process And Split Inspection Archives',
                'Upload Filtered And Split Inspection Files To FTP',
                'Initiate Inspections Data Merge with Database',
                'Exit'
            ],
        )
    ]
    return inquirer.prompt(questions)

# List of columns to keep (normalized headers).
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

# Remove WhiteSpace and Hidden Characters.
def clean_row(row):
    """Remove hidden characters and strip whitespace."""
    return {k: v.replace('\n', '').replace('\r', '').strip() for k, v in row.items()}

# Process Carrier Census Files.
def process_csv(file_path):
    logging.info(f"Starting processing of file: {file_path}")
    base_file_name = os.path.splitext(os.path.basename(file_path))[0]
    output_folder = 'Processed Files'
    os.makedirs(output_folder, exist_ok=True)
    dot_numbers = set()
    skipped_rows_count = 0
    console = Console()

    file_processed = False
    for encoding in ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']:
        try:
            with open(file_path, 'r', encoding=encoding) as infile:
                reader = csv.DictReader(infile, delimiter='~')
                total_rows = sum(1 for _ in reader)  # Count total rows for progress bar
                infile.seek(0)  # Reset file pointer to beginning
                reader = csv.DictReader(infile, delimiter='~')

                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    SpinnerColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Processing...", total=total_rows)

                    new_file_name = os.path.join(output_folder, f"{base_file_name}_processed.csv")
                    outfile = open(new_file_name, 'w', newline='', encoding='utf-8')
                    writer = csv.DictWriter(outfile, fieldnames=columns_to_keep)
                    writer.writeheader()

                    for row in reader:
                        cleaned_row = clean_row(row)

                        if not any(cleaned_row.values()) or not cleaned_row.get('ICC1'):
                            skipped_rows_count += 1
                            progress.update(task, advance=1)
                            continue

                        # Collect DOT numbers after cleaning and validation
                        if 'DOT_NUMBER' in cleaned_row:
                            dot_numbers.add(cleaned_row['DOT_NUMBER'])

                        writer.writerow({k: cleaned_row.get(k, '') for k in columns_to_keep})
                        progress.update(task, advance=1)

                    outfile.close()
                logging.info(f"Completed processing {file_path}. Processed rows: {total_rows}, Skipped rows: {skipped_rows_count}, Unique DOT numbers found: {len(dot_numbers)}")
                file_processed = True
                progress.update(task, completed=total_rows)
                if skipped_rows_count > 0:
                    logging.info(f"Skipped {skipped_rows_count} rows due to empty or missing ICC1 in file {file_path}")
                logging.info(f"File {base_file_name} processed and saved.")
                break
        except UnicodeDecodeError as e:
            if not file_processed:
                last_error = f"UnicodeDecodeError processing {file_path} with {encoding}: {e}"
            continue

    if not file_processed:
        logging.error(last_error)
        logging.error(f"Failed to process file {file_path} with any of the tried encodings.")
        return set()

    return dot_numbers

def split_processed_files(input_directory, lines_per_file):
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
                    if line_count % lines_per_file == 0:
                        if outfile is not None:
                            outfile.close()
                            progress.update(task, advance=lines_per_file)
                        new_file_name = os.path.join(split_folder, f"{base_file_name}_part_{part_number}.csv")
                        outfile = open(new_file_name, 'w', newline='', encoding='utf-8')
                        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        part_number += 1

                    writer.writerow(row)
                    line_count += 1

                if outfile is not None and not outfile.closed:
                    outfile.close()
                    remaining_rows = line_count % lines_per_file
                    progress.update(task, advance=remaining_rows if remaining_rows else lines_per_file)

                progress.update(task, completed=total_rows)  # Ensure task is marked as completed

            logging.info(f"File {base_file_name} split into {part_number - 1} parts.")

def map_dot_to_inspection_id(dot_numbers, archive_directory):
    inspection_map = defaultdict(list)
    console = Console()
    total_archives = len(glob.glob(os.path.join(archive_directory, '*.zip')))

    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                  SpinnerColumn(),
                  console=console) as progress:
        task = progress.add_task("Mapping DOT to Inspection IDs", total=total_archives)

        for archive in glob.glob(os.path.join(archive_directory, '*.zip')):
            with zipfile.ZipFile(archive, 'r') as zip_ref:
                for zip_info in zip_ref.infolist():
                    if zip_info.filename.startswith("Insp_Pub"):
                        zip_ref.extract(zip_info, archive_directory)
                        file_path = os.path.join(archive_directory, zip_info.filename)

                        for encoding in ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']:
                            try:
                                with open(file_path, 'r', encoding=encoding) as infile:
                                    reader = csv.DictReader(infile, delimiter='\t')
                                    for row in reader:
                                        dot_number = row.get('DOT_NUMBER')
                                        if dot_number in dot_numbers:
                                            inspection_map[dot_number].append(row.get('INSPECTION_ID'))
                                break  # Break the loop if file processed successfully
                            except UnicodeDecodeError:
                                continue  # Try next encoding

                progress.update(task, advance=1)  # Update progress after each archive

    return inspection_map

def extract_dot_numbers_from_processed(processed_directory):
    dot_numbers = set()
    for filename in glob.glob(os.path.join(processed_directory, '*.csv')):
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'DOT_NUMBER' in row and row['DOT_NUMBER']:
                    dot_numbers.add(row['DOT_NUMBER'])
    return dot_numbers

def add_inspection_id_to_census(census_directory, inspection_map):
    console = Console()
    total_files = len(glob.glob(os.path.join(census_directory, '*.csv')))

    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                  SpinnerColumn(),
                  console=console) as progress:
        task = progress.add_task("Adding Inspection IDs to Census", total=total_files)

        for filename in glob.glob(os.path.join(census_directory, '*.csv')):
            new_rows = []
            with open(filename, 'r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                fieldnames = reader.fieldnames + ['INSPECTION_ID']
                for row in reader:
                    dot_number = row.get('DOT_NUMBER')
                    inspection_ids = ','.join(inspection_map.get(dot_number, []))
                    row['INSPECTION_ID'] = inspection_ids
                    new_rows.append(row)
            
            with open(filename, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for new_row in new_rows:
                    writer.writerow(new_row)
            
            progress.update(task, advance=1)  # Update progress after each file

    return True  # Indicates successful completion

# Extract Dot Numbers From Carrier Files.
def extract_dot_numbers(output_folder):
    # Combine dot numbers from all files
    combined_dot_numbers = set()
    for filename in glob.glob(os.path.join(output_folder, '*.csv')):
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'DOT_NUMBER' in row and row['DOT_NUMBER']:
                    combined_dot_numbers.add(row['DOT_NUMBER'])

    dot_numbers_file = os.path.join(output_folder, 'dot_numbers.txt')
    with open(dot_numbers_file, 'w') as file:
        for dot in combined_dot_numbers:
            file.write(dot + '\n')
    logging.info(f"DOT numbers saved to {dot_numbers_file}")

    return combined_dot_numbers

# Function To Upload Files To FTP.
def upload_files_to_ftp(directory, target_dir, description):
    FTP_HOST = "195.179.237.122"
    FTP_USER = "u895992627"
    FTP_PASS = "Supra1122!"
    FTP_TARGET_DIR = target_dir

    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            logging.info(f"Connected to FTP server: {FTP_HOST}")

            ftp.cwd(FTP_TARGET_DIR)
            logging.info(f"Changed to FTP directory: {FTP_TARGET_DIR}")

            files = os.listdir(directory)
            console = Console()

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                SpinnerColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(description, total=len(files))

                for filename in files:
                    filepath = os.path.join(directory, filename)
                    with open(filepath, 'rb') as file:
                        ftp.storbinary(f'STOR {filename}', file)
                    progress.update(task, advance=1)

            # Log message after progress bar completion
            logging.info("All files uploaded to FTP successfully.")
    except Exception as e:
        logging.error(f"FTP upload failed: {e}")

# Function To Call Inspections MySQL Data Merge.
def call_inspections_data_merger():
    url = "https://loadguard.ai/ld/process_inspections.php"
    startIndex = 0
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
        task = progress.add_task("Inspections Data Merger Progress", total=1)  # Initial total is 1 to avoid ZeroDivisionError

        while not completed:
            response = requests.get(f"{url}?start={startIndex}").json()
            startIndex = response.get('nextStartIndex', startIndex)
            totalFiles = response.get('totalFiles', totalFiles)
            completed = response.get('completed', False)

            if totalFiles is not None:
                progress.update(task, total=totalFiles, completed=startIndex)

            if response.get('errors'):
                print(f"Errors encountered: {response['errors']}")

        print("All inspections files processed and merged.")

# Function To Call Carrier MySQL Data Merge.
def call_data_merger():
    url = "https://loadguard.ai/ld/process_chunk.php"
    startIndex = 0
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
        task = progress.add_task("Data Merger Progress", total=0)

        while not completed:
            response = requests.get(f"{url}?start={startIndex}")
            
            # Check if the response is successful and is in JSON format
            try:
                response_json = response.json()
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON response. Status Code: {response.status_code}, Response: {response.text}")
                break

            # Update the progress and other variables
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

# Function to Extract CDOT Numbers From Filtered Carrier Files and Save To File.
def extract_dot_numbers(output_folder):
    dot_numbers = set()
    for filename in glob.glob(os.path.join(output_folder, '*.csv')):
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                if 'DOT_NUMBER' in row:
                    dot_numbers.add(row['DOT_NUMBER'])
    with open(os.path.join(output_folder, 'dot_numbers.txt'), 'w') as file:
        for dot in dot_numbers:
            file.write(dot + '\n')
    return dot_numbers

# Function To Process Extracted Inspection File.
def process_insp_file(file_path, dot_numbers, lines_per_file):
    output_folder = 'Split and Ready Files/Inspections'
    os.makedirs(output_folder, exist_ok=True)
    base_file_name = os.path.splitext(os.path.basename(file_path))[0]
    part_number = 1
    data_row_count = 0
    outfile = None
    insp_dot_numbers = set()
    saved_parts = 0  # Track the number of parts saved

    # Define the columns to keep
    columns_to_keep = [
        "INSPECTION_ID", "DOT_NUMBER", "REPORT_STATE", "INSP_DATE", "REGISTRATION_DATE", 
        "REGION", "CI_STATUS_CODE", "INSP_LEVEL_ID", "CARGO_TANK", "HAZMAT_PLACARD_REQ", 
        "INSP_CONFIDENCE_LEVEL", "OOS_DEFECT_VER", "VIOL_TOTAL", "OOS_TOTAL", 
        "DRIVER_VIOL_TOTAL", "DRIVER_OOS_TOTAL", "VEHICLE_VIOL_TOTAL", 
        "VEHICLE_OOS_TOTAL", "HAZMAT_VIOL_TOTAL", "HAZMAT_OOS_TOTAL"
    ]

    for encoding in ['utf-8', 'ISO-8859-1', 'latin1', 'cp1252']:
        try:
            with open(file_path, 'r', encoding=encoding) as infile:
                total_rows = sum(1 for _ in infile)  # Count total rows for progress bar
                infile.seek(0)  # Reset file pointer to beginning
                reader = csv.DictReader(infile, delimiter='\t')

                # Rich Progress Bar setup
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    SpinnerColumn(),
                    transient=True,  # Close the progress bar on completion
                ) as progress:
                    task = progress.add_task(f"Splitting {base_file_name}", total=total_rows)
                    
                    for row in reader:
                        if row['DOT_NUMBER'] in dot_numbers:
                            insp_dot_numbers.add(row['DOT_NUMBER'])

                            # Filter the row to keep only the specified columns
                            filtered_row = {k: row[k] for k in columns_to_keep if k in row}

                            if data_row_count % lines_per_file == 0:
                                if outfile is not None:
                                    outfile.close()
                                    saved_parts += 1
                                new_file_name = os.path.join(output_folder, f"{base_file_name}_part_{part_number}.csv")
                                outfile = open(new_file_name, 'w', newline='', encoding='utf-8')
                                writer = csv.DictWriter(outfile, fieldnames=columns_to_keep, delimiter=',')
                                writer.writeheader()
                                part_number += 1

                            writer.writerow(filtered_row)
                            data_row_count += 1

                        progress.update(task, advance=1)

                    if outfile is not None and not outfile.closed:
                        outfile.close()
                        saved_parts += 1

                logging.info(f"File {base_file_name} processed and split into {saved_parts} parts.")
                return insp_dot_numbers
        except UnicodeDecodeError as e:
            last_error = f"UnicodeDecodeError processing {file_path} with {encoding}: {e}"
            continue

    if last_error:
        logging.error(last_error)
    return insp_dot_numbers

# Compare To Make Sure Inspection Dot Numbers Exist In Filter Carrier Files.
def compare_dot_numbers(dot_numbers_file, insp_dot_numbers_file):
    carrier_dot_numbers = read_dot_numbers(dot_numbers_file)
    insp_dot_numbers = read_dot_numbers(insp_dot_numbers_file)

    if insp_dot_numbers.issubset(carrier_dot_numbers):
        logging.info("All DOT numbers in inspection files are present in carrier files.")
    else:
        missing_dots = insp_dot_numbers - carrier_dot_numbers
        logging.info(f"Mismatch in DOT numbers. The following DOT numbers from inspection files are not present in carrier files: {missing_dots}")

# Function To Read Dot Numbers File.
def read_dot_numbers(dot_numbers_file):
    with open(dot_numbers_file, 'r') as file:
        return set(file.read().splitlines())

# Function To Detect And Process Inspection Archive and Extract Inspection Files.
def process_inspection_archive(archive, directory, lines_per_file):
    dot_numbers_file = os.path.join('Split and Ready Files', 'dot_numbers.txt')
    all_dot_numbers = read_dot_numbers(dot_numbers_file)
    extracted_dot_numbers = set()

    with zipfile.ZipFile(os.path.join(directory, archive), 'r') as zip_ref:
        for zip_info in zip_ref.infolist():
            if zip_info.filename.startswith("Insp_Pub"):
                zip_ref.extract(zip_info, directory)
                logging.info(f"Extracted Inspections Data From Archive: {zip_info.filename}")
                extracted_file_path = os.path.join(directory, zip_info.filename)
                extracted_dot_numbers.update(process_insp_file(extracted_file_path, all_dot_numbers, lines_per_file))

    return extracted_dot_numbers

# Main Function.
def main():
    answers = main_menu()
    processed_files_dir = 'Processed Files'

    if 'Process And Split Carrier CSV Files' in answers['operations']:
        csv_files = glob.glob('*.csv')
        if csv_files:
            logging.info(f"CSV Files Found: {csv_files}")
            for file_path in csv_files:
                process_csv(file_path)

            all_dot_numbers = extract_dot_numbers_from_processed(processed_files_dir)

            dot_numbers_file = os.path.join(processed_files_dir, 'dot_numbers.txt')
            with open(dot_numbers_file, 'w') as file:
                for dot in all_dot_numbers:
                    file.write(dot + '\n')
            logging.info(f"DOT Numbers Saved To {dot_numbers_file}")

            # Process inspection archives and map DOT numbers to inspection IDs
            inspection_map = map_dot_to_inspection_id(all_dot_numbers, '.')

            # Add INSPECTION_ID to the processed carrier files
            add_inspection_id_to_census(processed_files_dir, inspection_map)

            # Split the updated files
            split_processed_files(processed_files_dir, 15000)

        else:
            logging.info("No CSV Files Found.")

    if 'Upload Filtered And Split Carrier Files To FTP' in answers['operations']:
        carrier_files_dir = 'Split and Ready Files'
        if not os.path.exists(carrier_files_dir) or not os.listdir(carrier_files_dir):
            logging.info("No Carrier CSV Files Found. Have You Processed The Files Yet?")
        else:
            upload_files_to_ftp(carrier_files_dir, '/public_html/ld/dataset/', 'Uploading Files to FTP')

    if 'Initiate Carrier Data Merge With Database' in answers['operations']:
        call_data_merger()

    if 'Process And Split Inspection Archives' in answers['operations']:
        inspection_archives = [file for file in os.listdir('.') if file.startswith('Insp_') and file.endswith('.zip')]
        if inspection_archives:
            logging.info(f"Found {len(inspection_archives)} Inspection Archives.")
            all_extracted_dot_numbers = set()  # Accumulate DOT numbers from all archives
            for archive in inspection_archives:
                extracted_dot_numbers = process_inspection_archive(archive, '.', 15000)
                all_extracted_dot_numbers.update(extracted_dot_numbers)

            # Save all extracted DOT numbers after processing all archives
            insp_dot_numbers_file = os.path.join('Split and Ready Files', 'insp_dot_numbers.txt')
            with open(insp_dot_numbers_file, 'w') as file:
                for dot in sorted(all_extracted_dot_numbers):
                    file.write(dot + '\n')
            logging.info(f"Unique DOT Numbers From All Inspection Files Saved: {insp_dot_numbers_file}")
            
            compare_dot_numbers(dot_numbers_file, insp_dot_numbers_file)
        else:
            logging.info("No Inspection Archives Found.")

    if 'Upload Filtered And Split Inspection Files To FTP' in answers['operations']:
        inspection_files_dir = 'Split and Ready Files/Inspections'
        if not os.path.exists(inspection_files_dir) or not os.listdir(inspection_files_dir):
            logging.info("No Inspection CSV files found. Have You Processed The Inspection Files Yet?")
        else:
            upload_files_to_ftp(inspection_files_dir, '/public_html/ld/dataset/Inspections', 'Uploading Inspection Files To FTP')

    if 'Initiate Inspections Data Merge with Database' in answers['operations']:
        call_inspections_data_merger()

    if 'Exit' in answers['operations']:
        logging.info("Exiting the program.")

if __name__ == '__main__':
    main()