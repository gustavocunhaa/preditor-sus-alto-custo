import os
import argparse
from pathlib import Path

import boto3
from pysus.online_data.SIH import download

from config import s3_bucket
from save import save_parquet

def extracao_dados_sih(estado: str, ano: str, mes: int):
  
  session = boto3.Session(aws_access_key_id=os.getenv("AWS_ID"), 
                          aws_secret_access_key=os.getenv("AWS_KEY_ID"), 
                          region_name='us-east-1')  

  dados = download(states=estado,
                   years=ano, 
                   months=mes, 
                   groups='RD', 
                   data_dir=Path(f"{os.getcwd()}/data")
                   )
  
  print(f"Dados de {estado}, de {mes}/{ano}, baixados")
  save_path = f"{s3_bucket}/raw/estado={estado}/ano={ano}/mes={mes}/sih_raw_data.parquet"
  df = dados.to_dataframe()
  
  return save_parquet(df, save_path, session)


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="DataSUS SIH Collect")
    parser.add_argument("--ano", required=True)
    parser.add_argument("--mes", required=True)
    args = parser.parse_args()
    
    siglas_estados_brasil = [
      'ac', 'ap', 'am', 'pa', 'ro', 'rr', 'to',  # Norte
      'al', 'ba', 'ce', 'ma', 'pb', 'pe', 'pi', 'rn', 'se',  # Nordeste
      'df', 'go', 'mt', 'ms',  # Centro-Oeste
      'es', 'mg', 'rj', 'sp',  # Sudeste
      'pr', 'rs', 'sc'  # Sul
      ]
    
    for estado in siglas_estados_brasil:
      extracao_dados_sih(estado, args.ano, args.mes)