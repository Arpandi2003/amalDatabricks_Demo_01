import os

# Create directory if it doesn't exist
os.makedirs('AMALDAB/resources/SharedObjects', exist_ok=True)

# Create empty file
with open('AMALDAB/resources/SharedObjects/empty_file.yml', 'w') as f:
    pass  # Just create the file, don't write anything

print("Empty file created!")
