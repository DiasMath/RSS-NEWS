import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import traceback

# ============================
# COLE AQUI suas credenciais
# ============================
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "rss-news-tracker",
    "private_key_id": "80666e1d54c8d39def8d97740a4d9e62e607c80f",
    "private_key": """-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDCJQzbx+Uow616
OrekOemeLbR3/3Tt7uHZcF8RPhIsd5BU62Yn6CY9kxhDdN2keCaiA6r4yJ5sw31C
EKf7rKxv44h86WN3j3MlwF4xJ1fT7anAlZlOTurGnJWgdmvbRnpPVxqDD8nBLXUn
lrzqahFn9nbyz7zYaCNEAHp8T+DpgJ401R9qbWqOinLntHFNnruThGQOHwVhOXdG
vAresLTo0hKjwTeEYG1N4dM6b5ti0K8/AicwfVHd7im8+sUcOG4OJulzVSifZlHy
dDkaMAbg006SQK2vzgDt4mDjgZTSdg7DxYBJh/vqW/SdniSudwAL8rLk4l8usHZs
iptl4WVRAgMBAAECggEAFQdDUcjggt5S+3LBslmNetMTgY3bYewmbVFVEr6I5GUc
7n3Cxl7ISZIVpjTRzv8unAzOp3y/YCN6fT3lnAZzdPkNrw3udqREghnIegV7Z1qO
MOxjOw6kMlgCgutgulLza4WROxHqNfDRCDU1CCTt9rdE4f7rQvtgmnx8x9s1DsVt
w/o9+jRp71Ee+ahSHq6/rSD/rhBReB3hpv0H6Wxy1ANYXXoZKdLP8FCCTY2JEamp
CWitQPKNaK8oyRaFrrkHBf8DKov9g/T4CS9uM4EGisKLYqNSO1dnyiSNMN9zqsOF
WqRJ/UmT1MHcK8ybxnlMfarX2a18Pwu13jS8Tex+4wKBgQDpy0n8xr7qorLyC9O1
weTuCT2HbuiLiH++PDj9p7CZ8n8F6PVeadY+O5LLGdBkbAKqGc/+Bl8ZErqaafRh
Jzam/gJA638qm1yOCBZJrpZJQjdAhKdywNccxG5gtfrli780dj4+YKOuxtBufyR5
ER/oxPqAztXF3f8u8dvGUfG2mwKBgQDUla99Sb33UhlhtScMHNDaVEhiJX872uBt
goIU3K7qkNWNMia5Y2PNZ99RxI51ZtRFHslxd+ECKpaOd8f0ONqMlaH06XSUzRPF
9zBav79jyr+uS/a2clcoxZkGvoQfaMm1vwQ23xR6fsepwzSRsx7CoucL7gGQ6UM7
sHf17SgcgwKBgQDWR9fJUdO8NGD0zfg997e+oEN/tRx4fyQuFP5bJm4Lu6HGpez2
muSdZ3a0pjVFRWXvx/bFqctRrMPRMVmmDg3eYqNoIzALuhfLqgfbgqQGAyWnAa6D
09GVcUiFZWXPLBWUnOeRvntnfpudGvUv7Y0kiB3dHzX5w+3WdiBsFcEIDwKBgQCV
OLkllgTNvYhpiJJvMy77gpwIoM+OAVb2J90Nrdbuelocsa5zBaxBu/8LU5C4IkUw
e6rlhkOglKp4OOZXrSzj8AjudI1MAiQ2GwyLNvuundwtCc+VQ++ghAulq0ftEE4+
0GWx6qdiUOnwZUDaYURfVaAfRKM+yC5UkMu0ChPU5wKBgQCOxoqHWD2jledpc+ag
D001Fp0sprVKPuCJnsaOCXzqbO+zJMxP0BWDpvlZSgyvKEPsQ0Jpx2gASZk4xC8q
J5oLWVcP7KpKZULhEUT7Wa4/yQCntbUspuXIqqClHWHo1x2wDVgm9NnPPdLqFZbg
n877BbUulbUg/A5ftSyL+xGj8A==
-----END PRIVATE KEY-----\n""",
    "client_email": "leitor-de-rss@rss-news-tracker.iam.gserviceaccount.com",
    "client_id": "110422821286528792366",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/leitor-de-rss%40rss-news-tracker.iam.gserviceaccount.com"
}

SCOPES               = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID       = '1BgYqXwCw2oMFO5Xm_6_GQPE1uUunx0ir2Q3wV-ovJvs'
RSS_RANGE            = 'RSS News!A2:A100'
SOURCE_MAPPING_RANGE = 'DeParaFontes!A2:B'
TOPIC_MAPPING_RANGE  = 'DeParaTopicos!A2:B'
RESULTS_RANGE_START  = 'Resultados!A1'

creds   = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds).spreadsheets()

def load_source_mapping():
    resp   = service.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=SOURCE_MAPPING_RANGE).execute()
    values = resp.get('values', [])
    return {row[0].lower(): row[1] for row in values if len(row) >= 2}

def load_topic_mapping():
    resp   = service.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=TOPIC_MAPPING_RANGE).execute()
    values = resp.get('values', [])
    mapping = []
    for row in values:
        if len(row) >= 2:
            terms = [t.strip().lower() for t in row[0].split(',') if t.strip()]
            mapping.append((terms, row[1]))
    return mapping

def categorize_source(text, source_map):
    low = text.lower()
    for de, para in source_map.items():
        if de in low:
            return para
    return "No match"

def categorize_topic(text, topic_map):
    low = text.lower()
    for terms, para in topic_map:
        if any(de in low for de in terms):
            return para
    return "No match"

def parse_rss_feed(xml_string, source_map, topic_map):
    root = ET.fromstring(xml_string)
    ch   = root.find('channel')
    if ch is None:
        return []
    items = []
    for itm in ch.findall('item'):
        title    = itm.findtext('title', default="")
        link     = itm.findtext('link', default="")
        raw_date = itm.findtext('pubDate', default="")
        if raw_date:
            try:
                dt_utc = parsedate_to_datetime(raw_date)
                dt_sp  = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
                formatted_date = dt_sp.strftime('%Y-%m-%d %H:%M:%S')
            except:
                formatted_date = raw_date
        else:
            formatted_date = ""
        raw = ET.tostring(itm, encoding='unicode')
        items.append({
            'Título':             title,
            'Data de publicação': formatted_date,
            'Fonte':              categorize_source(raw, source_map),
            'Categoria':          categorize_topic(raw, topic_map),
            'URL':                link,
            'Prioridade':         "Não definida",
            'Status':             "Não definido"
        })
    return items

def read_rss_urls():
    resp = service.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RSS_RANGE).execute()
    return [row[0] for row in resp.get('values', []) if row]

def write_results(df: pd.DataFrame):
    values = [df.columns.tolist()] + df.values.tolist()
    body   = {'values': values}
    service.values().clear(spreadsheetId=SPREADSHEET_ID,
                           range=RESULTS_RANGE_START).execute()
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RESULTS_RANGE_START,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

def main():
    src_map = load_source_mapping()
    top_map = load_topic_mapping()
    urls    = read_rss_urls()
    all_items = []
    for url in urls:
        resp = requests.get(url, timeout=10); resp.raise_for_status()
        all_items.extend(parse_rss_feed(resp.text, src_map, top_map))
    if all_items:
        df = pd.DataFrame(all_items)
        write_results(df)

def handler(request):
    try:
        main()
        return { 'statusCode': 200, 'body': '✅ Concluído com sucesso.' }
    except Exception:
        tb = traceback.format_exc()
        return { 'statusCode': 500, 'body': f'❌ Erro interno:\n\n{tb}' }
