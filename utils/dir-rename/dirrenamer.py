import os

# Define the source and target directory paths
target_directory = "io/lenses"

# Define the function to rename nested directories with verbose logging
def rename_nested_directories(directory_path, source_directory):
    for root, dirs, _ in os.walk(directory_path, topdown=True):
        for dir_name in dirs:
            source_dir_path = os.path.join(root, dir_name)
            if source_directory in source_dir_path and os.path.exists(source_dir_path):
                target_dir_path = source_dir_path.replace(source_directory, target_directory)

                # Check if the target directory exists and is not the same as the source
                if not os.path.exists(target_dir_path) and source_dir_path != target_dir_path:
                    print(f'Renaming directory: {source_dir_path} -> {target_dir_path}')
                    # Create the parent directory if it doesn't exist
                    os.makedirs(os.path.dirname(target_dir_path), exist_ok=True)
                    os.rename(source_dir_path, target_dir_path)
                    print(f'Renamed directory: {source_dir_path} -> {target_dir_path}')
                elif source_dir_path != target_dir_path:
                    print(f'Skipped renaming, target directory already exists: {target_dir_path}')
                else:
                    print(f'Skipped renaming, source and target directories are the same: {source_dir_path}')

# Specify the directory path you want to process
directory_to_process = '/stream-reactor'

# Call the function to rename nested directories with verbose logging
rename_nested_directories(directory_to_process, "com/datamountaineer")
rename_nested_directories(directory_to_process, "com/landoop")
