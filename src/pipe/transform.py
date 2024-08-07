import os
import argparse
from pyspark.sql import SparkSession, functions

import config
from save import save_parquet

ROOT_PATH = os.getcwd()

def transform_dataframe_alto_custo(spark, estado:str, ano:str, mes:int):
    if mes < 10:
        mes = f"0{mes}"

    df = spark.read.parquet(f"{ROOT_PATH}/data/RD{estado.upper()}{ano[-2:]}{mes}.parquet")

    df = df.where("MORTE == 0")
    df = df.withColumn("DIAG_PRINC",functions.expr("substring(DIAG_PRINC, 1, length(DIAG_PRINC) - 1)"))

    df = df.select(config.filter_columns)\
            .replace(config.dicionario_sexo, subset=["SEXO"])\
            .replace(config.dicionario_raca, subset=["RACA_COR"])\
            .replace(config.dicionario_instru, subset=["INSTRU"])
    
    dicionario_diag_class = config.dicionario_diag_class

    df = df.withColumn("DIAG_CLASS",
                        functions.when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["CARDIOVASCULAR"]), "CARDIOVASCULAR")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["RESPIRATORIO"]), "RESPIRATORIO")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["DIABTES"]), "DIABTES")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["OBESIDADE"]), "OBESIDADE")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["NEPLASIA"]), "NEPLASIA")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["FIGADO"]), "FIGADO")\
                        .when(
                            df["DIAG_PRINC"].isin(dicionario_diag_class["RENAL"]), "RENAL")
                        )
    
    df = df.na.drop(subset=["DIAG_CLASS"])
    save_path = f"{config.s3_bucket}/transform/estado={estado}/ano={ano}/mes={mes}/sih_transform_data.parquet" 

    return save_parquet(df.toPandas(), save_path)

if __name__ == '__main__':
    
    spark = SparkSession\
        .builder\
        .appName("datasus_transformation")\
        .getOrCreate()
    
    parser = argparse.ArgumentParser(description="Spark DataSUS Transformation")
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
        transform_dataframe_alto_custo(spark, estado, args.ano, args.mes)