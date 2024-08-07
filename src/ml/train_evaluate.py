import awswrangler as wr
import numpy as np
import pandas as pd

from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from imblearn.under_sampling import RandomUnderSampler
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score, f1_score

import config


def load_data(s3_transform_path):
    df = wr.s3.read_parquet(s3_transform_path)
    df['US_TOT'] = df['US_TOT'].astype('float')
    p95 = np.percentile(df['US_TOT'], 95)
    df.loc[(df['US_TOT']>=p95), 'ALTO_CUSTO'] = 1
    df.loc[(df['US_TOT']<p95), 'ALTO_CUSTO'] = 0
    x = df[list(config.features.keys())]
    y = df['ALTO_CUSTO']
    return x, y

def norm_data(data):
    scaler = MinMaxScaler()
    data_norm = scaler.fit_transform(data)
    colunas = data.columns
    df = pd.DataFrame(data_norm, columns=colunas)
    return df

def make_ohe(data):
    ohe = OneHotEncoder(dtype=int)
    data_ohe = ohe.fit_transform(data).toarray()
    df = pd.DataFrame(data_ohe, columns=ohe.get_feature_names_out(data.columns))
    return df

def transform_features(x):
    dict_features = config.features
    df_transform_num = pd.DataFrame()
    df_transform_str = pd.DataFrame()
    for col in x.columns:
        dtype = dict_features[col]
        x[col] = x[col].astype(dtype)
        if dtype in ('float', 'int'):
            num = norm_data(x[[col]])
            df_transform_num = pd.concat([df_transform_num, num], axis=1)
        elif dtype in ('str'):
            dummy = make_ohe(x[[col]])
            df_transform_str = pd.concat([df_transform_str, dummy], axis=1)    
    df = pd.concat([df_transform_num, df_transform_str], axis=1)
    return df

def split_data(x, y, test_size):
    print(f">>> Spliting data on {test_size*100}% for test")
    try:
        X_train, X_test, y_train, y_test = train_test_split(x, y, 
                                                            random_state=config.SEED, 
                                                            test_size=test_size)
        print(f"SUCESS: Slipt Data")
    except ValueError as e:
        print(f"ERROR: Slipt Data - {e}")
    return X_train, X_test, y_train, y_test 

def balance_target(X_train, y_train):
    rus = RandomUnderSampler(random_state = config.SEED)
    x_rus, y_rus = rus.fit_resample(X_train, y_train)
    return x_rus, y_rus

def train_model(model, X_train, y_train):
    modelo_treinado = model.fit(X_train, y_train)
    print(f"Modelo {modelo_treinado} treinado")
    return modelo_treinado

def score(model_fit, threshold, X_test):
    prob_predict = model_fit.predict_proba(X_test)[:,1]
    predcit = np.where(prob_predict >= threshold, 1, 0)
    return prob_predict, predcit

def evaluate(prob_predict, predictions, y_test):
    roc_auc = roc_auc_score(y_test, prob_predict)
    acc_value = accuracy_score(y_test, predictions)
    precision_value = precision_score(y_test, predictions, average='weighted')
    recall_value = recall_score(y_test, predictions, average='weighted')
    f1_value = f1_score(y_test, predictions, average='weighted')
    results = {
        'accuracy':  round(acc_value, 4),
        'precision': round(precision_value, 4),
        'recall':    round(recall_value, 4),
        'f1':        round(f1_value, 4),
        'roc_auc':   round(roc_auc, 4)       
    }
    return results

if __name__ == '__main__':
    x, y = load_data(config.transform_s3_path)
    x = transform_features(x)
    
    X_train, X_test, y_train, y_test = split_data(x, y, config.test_size)
    x_rus, y_rus = balance_target(X_train, y_train)
    
    model_fit = train_model(config.model, x_rus, y_rus)
    prob_predict, predcit = score(model_fit, config.threshold, X_test)
    results = evaluate(prob_predict, predcit, y_test)
    print(results)

