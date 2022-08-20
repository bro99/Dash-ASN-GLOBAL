import pandas as pd
import Api as ap
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import numpy as np
import data_contrato as ad

# #
def Junta_Data_Base():

#Variaveis globais que recebem valores de importação de Dataframe.
    var_contrato = ad.Gera_contrato()
    df_contrato = pd.DataFrame(var_contrato)
    valorAPi = ap.Requisita_dados_peeringDB()
    df_Api = pd.DataFrame(valorAPi)




    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    #Fim das declarações

    #Inicio do serviço de autenticação
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Fim do serviço de Autenticação.
    #
    # Pega os valores da planilha.
    #Começando tratamento do DataFrame
    request = service.spreadsheets().values().get(spreadsheetId='1y3hQA8R6g-inEmiGMvN9LDgaGR2SDU-in3LYYUkRQOU', range='Geral').execute()
    values = request.get("values")
    df_tony = pd.DataFrame(values)
    df_tony.columns = df_tony.loc[0]
    df_tony = df_tony.drop(0)
    df_tony.reset_index(drop=True, inplace=True)
    #Fim do tratamento!



    #Tratamento de mudanças das colunas - Renomear - Apagar
    df_Api = df_Api.drop(
        columns=["name_long",
                 "id_x",
                 "org_id",
                 "aka",
                 "website",
                 "looking_glass",
                 "route_server",
                 "irr_as_set",
                 "info_prefixes4",
                 "info_prefixes6",
                 "info_unicast",
                 "info_multicast",
                 "info_ipv6",
                 "info_never_via_route_servers",
                 "ix_count",
                 "fac_count",
                 "notes",
                 "netixlan_updated",
                 "netfac_updated",
                 "poc_updated",
                 "policy_url",
                 "policy_general",
                 "policy_locations",
                 "policy_ratio",
                 "policy_contracts",
                 "allow_ixp_update",
                 "status_dashboard",
                 "created_x",
                 "updated_x",
                 "status_x",
                 "id_y",
                 "net_id",
                 "visible",
                 "url",
                 "created_y",
                 "updated_y",
                 "status_y"
                 ])
    df_Api.rename(
        columns = {
            'zipcode':'CEP',
            'city': 'Cidade',
            'state':'Estado',
            'country':'País',
            'role':'Função',
            'name_y': 'Nome do Funcionario',
            'phone': 'Telefone Funcionario/Setor',
            'email':'Email Funcionario'
        },
        inplace = True)

    df_tony = df_tony.drop(
        columns = [
            "Criado",
            "Alterado",
            "Contato ID",
            "Timestamp",
            "Devolvido",
            "Abertura",
            "Data Situação",
            "Situação Especial",
            "Última Atualização"
        ])


    df_tony.rename(
        columns = {
            'inetnum':'Prefixo'
        },
        inplace = True
    )




    #Mudo o tipo do valor da cmpo para int
    df_Api['asn']=df_Api['asn'].astype(int)
    df_tony['asn'] = df_tony['asn'].astype(int)

    #Faço o procv dos DataFrames
    result = pd.merge(df_Api, df_tony, how= "outer",on = ["asn", "Estado", "País", "CEP"])




    #Substituo os valores NaN por espaço
    result['Estado'] = result["Estado"].replace('', np.nan)



    #Pego os valores da coluna cidade e trago para a coluna Estado Vazia
    cidade = result['Cidade']
    result['Estado'].fillna(cidade, inplace = True)
    result = result.replace(np.nan, ' ')



    #Coloco tudo em maiusculo
    #Renomeio as siglas dos estados
    result['Estado'] = result['Estado'].str.upper()
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SP' if 'PAULO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RJ' if 'JANEIRO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AC' if 'ACRE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AL' if 'ALAGOAS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AP' if 'AMAPÁ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AP' if 'AMAPA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AM' if 'AMAZONAS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-BA' if 'BAHIA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-CE' if 'CEARÁ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-CE' if 'CEARA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-ES' if 'ESPÍRITO ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-ES' if 'ESPIRITO ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-GO' if 'GOIÁS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-GO' if 'GOIAS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-GO' if 'GOIAIS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MA' if 'MARANHÃO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MA' if 'MARANHAO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MT' if 'MATO GROSSO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MT' if 'MATO-GROSSO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MS' if 'MATO GROSSO DO SUL' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MG' if 'MINAS GERAIS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PA' if 'PARÁ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PA' if 'PARA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PB' if 'PARAÍBA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PB' if 'PARAIBA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PR' if 'PARANÁ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PR' if 'PARANA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PE' if 'PERNAMBUCO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PE' if 'PERNABUCO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PE' if 'PERNANBUCO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PI' if 'PIAUÍ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PI' if 'PIAUI' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RN' if 'RIO GRANDE DO NORTE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RS' if 'RIO GRANDE DO SUL' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RS' if 'RIO GRANDE DOSUL' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RO' if 'RONDÔNIA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RO' if 'RONDONIA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RO' if 'RORAIMA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SC' if 'SANTA CATARINA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SE' if 'SERGIPE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-TO' if 'TOCANTINS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-DF' if 'DISTRITO FEDERAL' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MG' if 'MINAS GERIAS' in x else x)

    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AC' if 'AC' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AL' if 'AL' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AP' if 'AP' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-AM' if 'AM' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-BA' if 'BA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-CE' if 'CE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-ES' if 'ES' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MA' if 'MA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MT' if 'MT' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MS' if 'MS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-MG' if 'MG' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PA' if 'PA' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PR' if 'PR' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PE' if 'PE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PI' if 'PI' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RJ' if 'RJ' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RN' if 'RN' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RS' if 'RS' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RO' if 'RO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-RR' if 'RR' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SC' if 'SC' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SP' if 'SP' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-SE' if 'SE' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-TO' if 'TO' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-DF' if 'DF' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-PB' if 'PB' in x else x)
    result['Estado'] = result['Estado'].apply(lambda x: 'BR-GO' if 'GO' in x else x)
    result.to_excel("Result.xlsx")
    df_final = pd.merge(result, df_contrato, how= "inner",on="asn" )
    df_final = df_final.replace(np.nan, ' ')


    df_final.to_excel("Banco.xlsx")



    return df_final
