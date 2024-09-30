import os

# Folder path
folder_path = "C:\\path\\to\\your\\folder"

# Output text file path
output_file_path = "C:\\path\\to\\your\\output.txt"

# Get all files in the folder
files = os.listdir(folder_path)

# Write filenames without extensions to the text file
with open(output_file_path, 'w') as f:
    for file in files:
        # Only write files, not directories
        if os.path.isfile(os.path.join(folder_path, file)):
            file_name_without_extension = os.path.splitext(file)[0]
            f.write(file_name_without_extension + '\n')
            print(file_name_without_extension)
