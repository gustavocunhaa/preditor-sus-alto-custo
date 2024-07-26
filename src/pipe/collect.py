import os
from pathlib import Path
from pysus.online_data.SIH import download

from config import s3_bucket
from save import save_parquet

ROOT_PATH = os.getcwd()

def extracao_dados_sih(estado: str, ano: str, mes: int):
  
  dados = download(states=estado,
                   years=ano, 
                   months=mes, 
                   groups='RD', 
                   data_dir=Path(f"{ROOT_PATH}/data")
                   )
  
  print(f"Dados de {estado}, de {mes}/{ano}, baixados")
  save_path = f"{s3_bucket}/raw/estado={estado}/ano={ano}/mes={mes}/sih_raw_data.parquet"
  df = dados.to_dataframe()
  
  return save_parquet(df, save_path) 
