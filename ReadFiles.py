import pandas as pd

def process_file(input_path, output_path, file_format):
    # Read the file based on the file format
    if file_format == "csv":
        df = pd.read_csv(input_path)
    elif file_format == "json":
        df = pd.read_json(input_path)
    elif file_format == "excel":
        df = pd.read_excel(input_path)
    else:
        raise ValueError("Unsupported file format. Supported formats: 'csv', 'json', 'excel'.")

    print("Original DataFrame:")
    print(df.head())

    df.dropna(inplace=True)  # Drop rows with missing values
    if "column1" in df.columns:
        df = df[df["column1"] != ""]  # Filter rows where 'column1' is not empty

    print("\nTransformed DataFrame:")
    print(df.head())

    # Write to a Parquet file
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"\nData successfully written to {output_path}")



if __name__ == "__main__":
    input_path = "C:/Users/ksubraman1/Py_Workspace/APIMail/input_file.json" 
    output_path = "output_file.parquet"  
    file_format = "json"  

    try:
        process_file(input_path, output_path, file_format)
    except Exception as e:
        print(f"Error: {e}")
