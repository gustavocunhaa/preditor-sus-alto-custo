import os
from pathlib import Path
from pysus.online_data.SIH import download

ROOT_PATH = os.getcwd()

def extracao_dados_sih(estado: str, ano: int, mes: int):
  
  download(states=estado,
           years=ano, 
           months=mes, 
           groups='RD', 
           data_dir=Path(f"{ROOT_PATH}/data/{estado}")
           )
  
  return print(f"Dados de {estado}, de {mes}/{ano}, baixados")
  
