from sklearn.ensemble import AdaBoostClassifier

transform_s3_path = "s3://datalake-data-sus/transform/"
features = {
    "SEXO": "str",
    "RACA_COR": "str",
    "INSTRU": "str",
    "IDADE": "int",
    "DIAS_PERM": "float",
    "DIAG_CLASS": "str"
}


SEED = 1337
test_size = 0.3
model = AdaBoostClassifier(random_state=SEED, algorithm='SAMME')
threshold = 0.5