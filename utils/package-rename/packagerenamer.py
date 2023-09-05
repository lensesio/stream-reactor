import os
import re

# Define the source and target package names
target_package = "io.lenses"

# Define the function to replace package declarations and import references
def replace_package_and_import(file_path, source_package):
    with open(file_path, 'r') as file:
        content = file.read()

    # Replace package declaration
    content = content.replace(f'package {source_package}', f'package {target_package}')

    # Replace import references
    content = re.sub(rf'\bimport {source_package}\.', f'import {target_package}.', content)

    with open(file_path, 'w') as file:
        file.write(content)

# Define the function to process files in a directory
def process_directory(directory_path, source_package):
    for root, _, files in os.walk(directory_path):
        for file_name in files:
            if file_name.endswith(".java") | file_name.endswith(".scala"):  # You can change this to match other file types
                file_path = os.path.join(root, file_name)
                replace_package_and_import(file_path, source_package)
                print(f'Replaced package and imports in {file_path}')

# Specify the directory path you want to process
directory_to_process = '/stream-reactor'

# Call the function to process the directory
process_directory(directory_to_process, "com.datamountaineer" )
process_directory(directory_to_process, "com.landoop" )
