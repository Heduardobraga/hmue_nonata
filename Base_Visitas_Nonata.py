import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import time
from supabase import create_client, Client
import os

print("Iniciando o script...")

# ==========================
# CONFIGURAÇÕES
# ==========================
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
NOME_TABELA = "hmue_visitas"

PATH_CREDENTIALS = os.getenv('PATH_CREDENTIALS', 'credentials.json')
SPREADSHEET_NAME = 'HMUE.Mapa de Visita'

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================
# CONEXÃO COM GOOGLE SHEETS
# ==========================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(PATH_CREDENTIALS, scope)
client = gspread.authorize(creds)

spreadsheet = client.open(SPREADSHEET_NAME)
worksheets = spreadsheet.worksheets()

df_final = pd.DataFrame()

# ==========================
# LEITURA DAS ABAS
# ==========================
for i, ws in enumerate(worksheets, start=1):
    aba_nome = ws.title.strip()
    print(f"Lendo aba {i}/{len(worksheets)}: {aba_nome} ...")

    try:
        data_ref = datetime.strptime(aba_nome, "%d/%m/%Y").date()
    except ValueError:
        print(f"  -> Ignorando aba '{aba_nome}': nome não é uma data válida.")
        continue

    data = ws.get_all_values()
    if len(data) < 6:
        print(f"  -> Aba '{aba_nome}' não tem dados suficientes.")
        continue

    headers = data[5]
    values = data[6:]
    temp_df = pd.DataFrame(values, columns=headers)

    temp_df = temp_df.loc[:, temp_df.columns != '']
    temp_df = temp_df.loc[:, ~temp_df.columns.duplicated()]
    temp_df = temp_df.apply(lambda col: col.map(lambda x: x.replace('\n', ' ').replace('\r', ' ') if isinstance(x, str) else x))

    for col_to_remove in ['Plano Terapêutico', 'Aviso/OPME', 'Isolam']:
        if col_to_remove in temp_df.columns:
            temp_df = temp_df.drop(columns=[col_to_remove])

    temp_df['Nome_Aba'] = str(data_ref)

    if 'Leito' in temp_df.columns:
        temp_df = temp_df[~temp_df['Leito'].astype(str).str.strip().isin(["", "Leito"])]

    df_final = pd.concat([df_final, temp_df], ignore_index=True)
    time.sleep(1)

# ==========================
# TRATAMENTO DE COLUNAS COM LISTAS
# ==========================
def tratar_coluna_lista(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].str.split(',')
        df = df.explode(coluna)
        df[coluna] = df[coluna].str.strip()
    return df

df_final = tratar_coluna_lista(df_final, 'Especialidades')
df_final = tratar_coluna_lista(df_final, 'Diagnóstico')

# ==========================
# MAPA DE COLUNAS
# ==========================
mapa_colunas_para_supa = {
    'Leito': 'leito',
    'Situação Leito': 'situacao_leito',
    'Nome': 'nome',
    'Data de Nascimento': 'data_de_nascimento',
    'RH': 'rh',
    'Data_Internação': 'data_internacao',
    'Sexo': 'sexo',
    'Especialidades': 'especialidades',
    'Diagnóstico': 'diagnostico',
    'Perfil de transferência': 'perfil_de_transferencia',
    'Plano de Alta': 'plano_de_alta',
    'Nome_Aba': 'nome_aba',
    'Pendências': 'pendencias',
}

df_final.rename(columns=mapa_colunas_para_supa, inplace=True)

# ==========================
# FORMATAR DATAS
# ==========================
colunas_data = ['data_de_nascimento', 'data_internacao', 'plano_de_alta', 'nome_aba']
for col in colunas_data:
    if col in df_final.columns:
        df_final[col] = pd.to_datetime(df_final[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')

# ==========================
# CRIAR CHAVE
# ==========================
df_final['chave'] = df_final['leito'].astype(str).str.strip() + ' - ' + df_final['nome_aba'].astype(str).str.strip()

# ==========================
# FILTRO FINAL DE COLUNAS
# ==========================
colunas_supa = list(mapa_colunas_para_supa.values()) + ['chave']
colunas_supa = [col for col in colunas_supa if col in df_final.columns]
df_final = df_final[colunas_supa]
df_final = df_final.where(pd.notnull(df_final), None)

# ==========================
# FILTRAR APENAS ÚLTIMOS 3 DIAS
# ==========================
print("\n✅ Filtrando apenas os últimos 3 dias...\n")

df_final['nome_aba'] = pd.to_datetime(df_final['nome_aba'], errors='coerce')

hoje = pd.to_datetime(datetime.today().date())
tres_dias_atras = hoje - timedelta(days=2)

df_final = df_final[df_final['nome_aba'] >= tres_dias_atras]

# Converte de volta para string antes de enviar (evita o erro de serialização)
df_final['nome_aba'] = df_final['nome_aba'].dt.strftime('%Y-%m-%d')

print(f"✅ Total de registros a enviar (últimos 3 dias): {len(df_final)}")
print("Datas filtradas:", df_final['nome_aba'].unique())

# ==========================
# REMOVER DUPLICADOS POR CHAVE
# ==========================
print("\nVerificando duplicados antes de enviar...")

duplicados = df_final[df_final.duplicated(subset='chave', keep='last')]

if not duplicados.empty:
    print(f"⚠️ Encontradas {len(duplicados)} duplicatas na chave. Removendo duplicados, mantendo o último registro por chave.")
    df_final = df_final.drop_duplicates(subset='chave', keep='last')
else:
    print("✅ Nenhum duplicado encontrado.")

# ==========================
# ENVIO EM LOTES
# ==========================
batch_size = 500
dados_json = df_final.to_dict(orient='records')

print(f"\nIniciando envio ao Supabase... Total de registros: {len(dados_json)}\n")

for start in range(0, len(dados_json), batch_size):
    end = start + batch_size
    batch = dados_json[start:end]

    print(f"Enviando lote {start}-{end}...")

    try:
        response = supabase.table(NOME_TABELA).upsert(batch).execute()
        print(f"Lote {start}-{end} enviado com sucesso.")
    except Exception as e:
        print(f"❌ Exceção ao enviar lote {start}-{end}: {e}")

print("\n✅ Script finalizado.")
