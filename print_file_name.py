import os

# Folder path
folder_path = "C:\\Users\\ksubraman1\\Desktop\\uc"

# Output text file path
output_file_path = "C:\\Users\\ksubraman1\\Desktop\\uc\\file_list.txt"

# Get all files in the folder
files = os.listdir(folder_path)

# Write filenames to the text file
with open(output_file_path, 'w') as f:
    for file in files:
        # Only write files, not directories
        if os.path.isfile(os.path.join(folder_path, file)):
            file = os.path.splitext(file)[0]
            f.write(file + '\n')
            print(file)

