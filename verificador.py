import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
import requests
import json
import merge as mg

#Declaração Variaveis Global
valorAPi = mg.Junta_Data_Base()
dfnew = pd.DataFrame(valorAPi)

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
# request = service.spreadsheets().values().get(spreadsheetId='1fwkF8M08HyiklnihKck7VgRSJvbJ7bFTY1yyyjhgeZQ', range='Clientes Globais!A1:B2').execute()
# values = request.get('values')
# Data_anterior = pd.DataFrame(values)

#Limpa a planilha
request = service.spreadsheets().values().clear(
    spreadsheetId='1FXcLE0fmEByhelNhk7RFFuHpGP61i1z44ySZio2aWFk',
    range='Geral'
)
response = request.execute()



print("Enviando Dados...")
#Insere os dados atualizados
response_date = service.spreadsheets().values().append(
        spreadsheetId='1FXcLE0fmEByhelNhk7RFFuHpGP61i1z44ySZio2aWFk',
        valueInputOption='RAW',
        range='Geral!A1:B1',
        body=dict(
            majorDimension='ROWS',
            values=dfnew.T.reset_index().T.values.tolist())
    ).execute()

