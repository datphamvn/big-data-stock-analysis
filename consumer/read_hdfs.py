import pyhdfs
import uuid

# Setup the HDFS client
hdfs = pyhdfs.HdfsClient(hosts="namenode:9870", user_name="hdfs")

# Define the directory you want to read from
directory = '/data'

# List all files in the directory
files = hdfs.listdir(directory)
print("Files in '{}':".format(directory), files)

# Function to read and print the content of each file
def read_and_print_file(file_path):
    try:
        # Read the file content
        file_content = hdfs.open(file_path).read()
        # Print the file content
        print("Content of '{}':".format(file_path))
        print(file_content.decode('utf-8'))  # Assuming the file content is string
    except Exception as e:
        print("Failed to read '{}': {}".format(file_path, e))

# Iterate over files and print their contents
for file in files:
    file_path = "{}/{}".format(directory, file)
    read_and_print_file(file_path)
