import json
import pyodbc
from google.cloud import bigquery

def get_teradata_schema(connection_string, tables):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    for table in tables:
        cursor.execute(f"""
            SELECT ColumnName, ColumnType, Nullable, ColumnLength, DecimalTotalDigits, DecimalFractionalDigits
            FROM DBC.ColumnsV 
            WHERE TableName = '{table}'
        """)
        columns = cursor.fetchall()
        schema[table] = []
        for column in columns:
            column_name = column[0]
            column_type_raw = column[1].strip()  # Trim any whitespace
            column_type = teradata_type_mapping.get(column_type_raw.upper(), column_type_raw.upper())
            is_nullable = column[2] == "Y"
            column_length = column[3] if column[3] else None
            numeric_precision = column[4] if column[4] else None
            numeric_scale = column[5] if column[5] else None
            column_schema = {
                "name": column_name,  # Column name
                "type": column_type,  # Data type
                "nullable": is_nullable,  # Nullable
                "length": column_length,  # Length for strings
                "precision": numeric_precision,  # Precision for numeric types
                "scale": numeric_scale  # Scale for numeric types
            }
            schema[table].append(column_schema)
    
    return schema

