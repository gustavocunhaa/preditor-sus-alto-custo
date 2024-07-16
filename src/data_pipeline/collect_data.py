import os
from pathlib import Path
from pysus.online_data.SIH import download

ROOT_PATH = os.getcwd()

def extracao_dados(estado: str, ano: int, mes: int):
  
  dados = download(states=estado, 
                   years=ano, 
                   months=mes, 
                   groups='RD', 
                   data_dir=Path(f"{ROOT_PATH}/data/{estado}")
                   )
  print(f"Dados de {estado}, de {mes}/{ano}, baixados")
  df = dados.to_dataframe()
  df['UF'] = estado
  
  return df

