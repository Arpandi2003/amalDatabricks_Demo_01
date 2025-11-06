import os

# Define paths
FOLDER_PATH = "AMALDAB/resources/SharedObjects"
FILE_NAME = "empty_file.yml"
FULL_PATH = os.path.join(FOLDER_PATH, FILE_NAME)

# Create directory (and parent directories) if they don't exist
os.makedirs(FOLDER_PATH, exist_ok=True)

# Create an empty file
with open(FULL_PATH, 'w') as f:
    pass  # Creates an empty file

print(f"✅ Empty file created at: {FULL_PATH}")
