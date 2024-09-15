from pyspark.sql.functions import col, coalesce

def homogenize_columns(df, similar_columns):
    selected_cols = set()
    
    for target_col, similar_cols in similar_columns.items():
        cols_to_coalesce = [col(similar_col) for similar_col in similar_cols if similar_col in df.columns]
        
        if cols_to_coalesce:
            df = df.withColumn(target_col, coalesce(*cols_to_coalesce))
            selected_cols.add(target_col)
    
    return df.select(*selected_cols)