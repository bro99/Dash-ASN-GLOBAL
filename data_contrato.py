import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
from datetime import datetime

def Gera_contrato():
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

    #Fim do serviço de Autenticação.

    #Pega os valores da planilha.
    request = service.spreadsheets().values().get\
        (
        spreadsheetId='1Q-p3HCS-A0MSm1CiqGEzxpX9pgAmrfUniW7EErinmXU',
        range='LACNIC Upstreams'
        ).execute()
    values = request.get("values")

    df_contrato = pd.DataFrame(values)
    df_contrato.columns = df_contrato.loc[0]
    df_contrato = df_contrato.drop(0)
    df_contrato.reset_index(drop=True, inplace=True)

    # df_contrato['Tempo restante'] = df_contrato['Time to Talk'].apply(
    #     lambda values: values.split(',')
    # )


    df_contrato.rename(
        columns={
            'ASN Lead': 'asn',
            'Time to Renew': 'Tempo restante'
        },
        inplace=True)
    df_contrato['asn'] = df_contrato['asn'].astype(int)
    df_contrato['Tempo restante'] = df_contrato['Tempo restante'].astype(str)

    return df_contrato

