import awswrangler as wr

def save_parquet(df, path_s3):
    wr.s3.to_parquet(df, path_s3)
    return print(f"Parquet save {path_s3}")