import pandas as pd
import merge as mg
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


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








#Declaração Variaveis Global
valorAPi = mg.Junta_Data_Base()
dfnew = pd.DataFrame(valorAPi)

anomalias_df = pd.read_csv("anomalias.csv")
new_anomaly = anomalias_df["anomaly"].str.split(" ", n = 1, expand = True)
anomalias_df["Protocolo1"]= new_anomaly[0]
anomalias_df = pd.merge(anomalias_df, dfnew, how= "inner",on="asn" )


def format_bits(bytes: int) -> str:
    bytes = abs(bytes) / 8
    
    if abs(bytes) < 1000:
        return str(bytes) + "B"
    elif abs(bytes) < 1e6:
        return str(round(bytes / 1e3, 2)) + "kB"
    elif abs(bytes) < 1e9:
        return str(round(bytes / 1e6, 2)) + "MB"
    else:
        return str(round(bytes / 1e9, 2)) + "GB"

anomalias_df0 = anomalias_df.groupby(['Estado','Protocolo1']).size().groupby(['bits']).sum()
#['bits'].agg('sum')
anomalias_df2 = pd.DataFrame(anomalias_df0)


anomalias_df2.to_excel("Banco_Anomalias.xlsx")
# anomalias_df2 = anomalias_df.groupby(['Protocolo1'])
# anomalias_df2['bits'] = anomalias_df2['bits'].apply(format_bits)
# anomalias_df2 = anomalias_df2.reset_index()


# def convert(seconds): 
#     seconds = seconds % (24 * 3600) 
#     hour = seconds // 3600
#     seconds %= 3600
#     minutes = seconds // 60
#     seconds %= 60
      
#     return "%d:%02d:%02d" % (hour, minutes, seconds) 
      
      

# anomalias_df2['duration'] = anomalias_df2['duration'].apply(convert)

# anomalias_df2 = anomalias_df2.replace('', 'null')
# #Limpa a planilha
# request = service.spreadsheets().values().clear(
#     spreadsheetId='1MzSWGGGTgkRW2H4joHw0mhpTsKSWIbGMa-WynDKBxvo',
#     range='Inicio'
# )
# response = request.execute()



# print("Enviando Dados...")
# #Insere os dados atualizados
# response_date = service.spreadsheets().values().append(
#         spreadsheetId='1MzSWGGGTgkRW2H4joHw0mhpTsKSWIbGMa-WynDKBxvo',
#         valueInputOption='RAW',
#         range='Inicio!A1:B1',
#         body=dict(
#             majorDimension='ROWS',
#             values=anomalias_df2.T.reset_index().T.values.tolist())
#     ).execute()