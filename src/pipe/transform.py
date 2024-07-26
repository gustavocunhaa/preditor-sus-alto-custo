from pyspark.sql import functions
import config


def transform_dataframe_alto_custo(df):
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
    
    return df