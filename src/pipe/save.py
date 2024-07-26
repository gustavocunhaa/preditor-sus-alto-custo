import awswrangler as wr

def save_parquet(df, path_s3, boto3_session=None):
    wr.s3.to_parquet(df=df, path=path_s3, boto3_session=boto3_session)
    return print(f"Parquet save {path_s3}")

