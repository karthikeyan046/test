
workspace_file_path = "dbfs:/FileStore/filename_list.txt"  


filename_df = spark.read.text(workspace_file_path)


filename_list = filename_df.rdd.map(lambda row: row[0]).collect()


table_name = "your_databricks_table"  # Replace with your Databricks table name
df = spark.sql(f"SELECT filename FROM {table_name}")  # Assuming the column is named 'filename'


table_filenames = df.select("filename").rdd.flatMap(lambda x: x).collect()

Check which filenames are in both lists
present_in_both = set(filename_list) & set(table_filenames)
missing_in_table = set(filename_list) - set(table_filenames)


print("Filenames present in both the text file and Databricks table:")
for filename in present_in_both:
    print(filename)

print("\nFilenames from text file not found in the Databricks table:")
for filename in missing_in_table:
    print(filename)
