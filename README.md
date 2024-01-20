
<p align="center">
  <img src="https://i.imgur.com/cI1fVZe.png">
</p>

Place Census and Inspection Archive or Extracted Inspection files in the same directory as the script.

﻿LoadGuard-Carrier-DataTool
This script is a command-line tool that performs various operations on CSV files related to carrier census and inspection data. Let's go through the main function `main()`:

1. `answers = main_menu()`: This line calls the `main_menu()` function, which displays an ASCII art and a navigation guide. It then prompts the user to select one or more operations to perform from a list of options using the `inquirer` library. The selected options are stored in the `answers` variable.

2. `processed_files_dir = 'Processed Files'`: This line defines the directory where processed files will be stored.

3. `'Process And Split Carrier CSV Files' in answers['operations']`: This condition checks if the user selected the option to process and split carrier CSV files.

4. `csv_files = glob.glob('*.csv')`: This line uses the `glob` module to find all CSV files in the current directory. The `*.csv` pattern matches any file with a `.csv` extension.

5. `for file_path in csv_files: process_csv(file_path)`: This loop iterates over each CSV file found in the previous step and calls the `process_csv()` function to process the file. The function reads the file, cleans the rows, removes empty or invalid rows, and writes the processed rows to a new CSV file in the "Processed Files" directory.

6. `all_dot_numbers = extract_dot_numbers_from_processed(processed_files_dir)`: This line calls the `extract_dot_numbers_from_processed()` function to extract DOT numbers from the processed carrier files. It reads each processed CSV file, extracts the DOT numbers, and returns a set of unique DOT numbers.

7. `dot_numbers_file = os.path.join(processed_files_dir, 'dot_numbers.txt')`: This line defines the path to a text file (`dot_numbers.txt`) where the extracted DOT numbers will be saved.

8. `with open(dot_numbers_file, 'w') as file: for dot in all_dot_numbers: file.write(dot + '\n')`: This block of code opens the `dot_numbers_file` in write mode and writes each DOT number from the `all_dot_numbers` set to a new line in the file.

9. `inspection_map = map_dot_to_inspection_id(all_dot_numbers, '.')`: This line calls the `map_dot_to_inspection_id()` function to map DOT numbers to inspection IDs. It takes the set of all DOT numbers and the current directory as arguments. The function searches for inspection files in ZIP archives, extracts them, reads the files, and creates a dictionary mapping each DOT number to a list of inspection IDs.

10. `add_inspection_id_to_census(processed_files_dir, inspection_map)`: This line calls the `add_inspection_id_to_census()` function to add inspection IDs to the processed carrier files. It takes the processed files directory and the inspection map as arguments. The function reads each processed CSV file, looks up the DOT number in the inspection map, and adds the corresponding inspection IDs to a new column in the file.

11. `split_processed_files(processed_files_dir, 15000)`: This line calls the `split_processed_files()` function to split the updated processed files into smaller parts. It takes the processed files directory and a line limit (15000) as arguments. The function reads each processed CSV file, splits it into multiple smaller files with a specified number of lines per file, and saves them in a "Split and Ready Files" directory.

12. `'Upload Filtered And Split Carrier Files To FTP' in answers['operations']`: This condition checks if the user selected the option to upload filtered and split carrier files to an FTP server.

13. `carrier_files_dir = 'Split and Ready Files'`: This line defines the directory where the split carrier files are located.

14. `upload_files_to_ftp(carrier_files_dir, '/public_html/ld/dataset/', 'Uploading Files to FTP')`: This line calls the `upload_files_to_ftp()` function to upload the carrier files to an FTP server. It takes the carrier files directory, the target directory on the FTP server, and a description as arguments. The function connects to the FTP server, navigates to the target directory, and uploads each file from the carrier files directory to the server.

15. `'Initiate Carrier Data Merge With Database' in answers['operations']`: This condition checks if the user selected the option to initiate the carrier data merge with a database.

16. `call_data_merger()`: This line calls the `call_data_merger()` function, which initiates the data merging process with a database. The function sends HTTP requests to a specified URL, retrieves data in chunks, and updates the progress using a progress bar.

17. `'Process And Split Inspection Archives' in answers['operations']`: This condition checks if the user selected the option to process and split inspection archives.

18. `inspection_archives = [file for file in os.listdir('.') if file.startswith('Insp_') and file.endswith('.zip')]`: This line uses a list comprehension to find all files in the current directory that start with "Insp_" and end with ".zip". These files are considered inspection archives.

19. `for archive in inspection_archives: extracted_dot_numbers = process_inspection_archive(archive, '.', 15000)`: This loop iterates over each inspection archive found in the previous step and calls the `process_inspection_archive()` function to process the archive. The function extracts the inspection data from the archive, processes it, splits it into smaller parts, and returns a set of extracted DOT numbers.

20. `insp_dot_numbers_file = os.path.join('Split and Ready Files', 'insp_dot_numbers.txt')`: This line defines the path to a text file (`insp_dot_numbers.txt`) where the extracted DOT numbers from inspection files will be saved.

21. `with open(insp_dot_numbers_file, 'w') as file: for dot in sorted(all_extracted_dot_numbers): file.write(dot + '\n')`: This block of code opens the `insp_dot_numbers_file` in write mode and writes each DOT number from the `all_extracted_dot_numbers` set to a new line in the file. The DOT numbers are sorted before writing.

22. `compare_dot_numbers(dot_numbers_file, insp_dot_numbers_file)`: This line calls the `compare_dot_numbers()` function to compare DOT numbers between the carrier files and the inspection files. It takes the paths to the DOT numbers files as arguments. The function reads the DOT numbers from both files, compares them, and logs any mismatched DOT numbers.

23. `'Upload Filtered And Split Inspection Files To FTP' in answers['operations']`: This condition checks if the user selected the option to upload filtered and split inspection files to an FTP server.

24. `inspection_files_dir = 'Split and Ready Files/Inspections'`: This line defines the directory where the split inspection files are located.

25. `upload_files_to_ftp(inspection_files_dir, '/public_html/ld/dataset/Inspections', 'Uploading Inspection Files To FTP')`: This line calls the `upload_files_to_ftp()` function to upload the inspection files to an FTP server. It takes the inspection files directory, the target directory on the FTP server, and a description as arguments. The function connects to the FTP server, navigates to the target directory, and uploads each file from the inspection files directory to the server.

26. `'Initiate Inspections Data Merge with Database' in answers['operations']`: This condition checks if the user selected the option to initiate the inspections data merge with a database.

27. `call_inspections_data_merger()`: This line calls the `call_inspections_data_merger()` function, which initiates the inspections data merging process with a database. The function sends HTTP requests to a specified URL, retrieves data in chunks, and updates the progress using a progress bar.

28. `'Exit' in answers['operations']`: This condition checks if the user selected the option to exit the program.

29. `logging.info("Exiting the program.")`: This line logs a message indicating that the program is exiting.

Counting Each Inspection Type: For each DOT number, the script counts the number of inspections for each type (vehicle, driver, hazmat). This count is incremented by 1 for each relevant inspection found in the inspection data.

Counting Out-Of-Service (OOS) Inspections: The script also counts the number of inspections where there is at least one OOS violation for each type. This means that if an inspection has any OOS violations (greater than 0), it is counted as one OOS inspection for that type. The total number of OOS inspections for each type is not the sum of all OOS violations but the count of inspections where OOS violations occurred.

For example, if there are 10 inspections and 5 of them have any number of OOS violations (regardless of the actual number of violations in each of those inspections), then the OOS count for that type is 5, not the sum of the number of violations in those 5 inspections.

This logic ensures that each inspection contributes only once to the OOS count for its type, regardless of the number of OOS violations it contains. The same applies to the total inspection count for each type. This method provides a clearer picture of how many inspections resulted in OOS findings, rather than the total number of violations, which could be skewed by a few inspections with multiple violations.

Overall, this script provides a menu-driven interface to perform various operations on carrier census and inspection data, including processing CSV files, splitting files, uploading files to an FTP server, merging data with a database, and comparing DOT numbers between carrier and inspection files.
