import os

def count_files_in_directory(directory):
    file_counts = {}
    
    # Traverse the directory tree
    for root, _, files in os.walk(directory):
        relative_path = os.path.relpath(root, directory)
        file_counts[relative_path] = len(files)
    
    return file_counts

def print_file_counts(file_counts):
    for directory, count in file_counts.items():
        if count==2:
            pass
        else:
            print(f"{directory}: {count} file(s)")

# Replace 'your_directory' with the path to your target directory
directory = 'data'
file_counts = count_files_in_directory(directory)
print_file_counts(file_counts)