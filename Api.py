import pandas as pd
import requests
import json
API_KEY = "budd78Ge.SaAgUftJb373ylfP2dP254A3y7IwGDib"
import numpy as np

def Requisita_dados_peeringDB():


    ''# def Requisita_NET():
    URL = (f"https://www.peeringdb.com/api/net")
    #FAC NÃO
    headers = {"Authorization": "Api-Key " + API_KEY}
    response1 = requests.get(URL, headers=headers)
    dic1 = response1.json()["data"]
    df_net = pd.json_normalize(data=dic1)



    URL = (f"https://www.peeringdb.com/api/org")
    #FAC NÃO
    headers = {"Authorization": "Api-Key " + API_KEY}
    response2 = requests.get(URL, headers=headers)
    dic2 = response2.json()["data"]
    df_org = pd.json_normalize(data=dic2)


    URL = (f"https://www.peeringdb.com/api/poc")
    #FAC NÃO
    headers = {"Authorization": "Api-Key " + API_KEY}
    response3 = requests.get(URL, headers=headers)
    dic3 = response3.json()["data"]
    df_poc = pd.json_normalize(data=dic3)

    #PROCV DA tabelas
    Data_tratado_semPOC = pd.merge(df_net, df_org, how='inner', suffixes=('', '_y'), left_on='org_id', right_on='id')
    Data_tratado_semPOCNew = Data_tratado_semPOC.drop(Data_tratado_semPOC.filter(regex='_y$').columns, axis=1)

    Data_Geral = pd.merge(Data_tratado_semPOCNew, df_poc, how='inner', left_on='id', right_on='net_id')
    # Data_Geral.to_excel("Peering.xlsx")


    # Data_Geral.to_excel("Banco_de_Dados.xlsx")
    print("Dados Peering DB - OK ")
    return Data_Geral


