import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd

# ============================
# COLE AQUI suas credenciais
# ============================
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "rss-news-tracker",
    "private_key_id": "SEU_PRIVATE_KEY_ID",
    "private_key": """-----BEGIN PRIVATE KEY-----
SEU PRIVATE KEY AQUI (incluindo quebras de linha)
-----END PRIVATE KEY-----\n""",
    "client_email": "SEU_CLIENT_EMAIL",
    "client_id": "SEU_CLIENT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/SEU_CLIENT_EMAIL"
}

SCOPES                 = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID         = '1BgYqXwCw2oMFO5Xm_6_GQPE1uUunx0ir2Q3wV-ovJvs'
RSS_RANGE              = 'RSS News!A2:A100'
SOURCE_MAPPING_RANGE   = 'DeParaFontes!A2:B'
TOPIC_MAPPING_RANGE    = 'DeParaTopicos!A2:B'
RESULTS_RANGE_START    = 'Resultados!A1'

# inicializa Sheets API
creds   = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds).spreadsheets()

# ============================
# Load mappings
# ============================
def load_source_mapping():
    resp   = service.values().get(spreadsheetId=SPREADSHEET_ID, range=SOURCE_MAPPING_RANGE).execute()
    values = resp.get('values', [])
    return {row[0].lower(): row[1] for row in values if len(row) >= 2}

def load_topic_mapping():
    resp   = service.values().get(spreadsheetId=SPREADSHEET_ID, range=TOPIC_MAPPING_RANGE).execute()
    values = resp.get('values', [])
    mapping = []
    for row in values:
        if len(row) >= 2:
            terms = [t.strip().lower() for t in row[0].split(',')]
            mapping.append((terms, row[1]))
    return mapping

# ============================
# Categorization
# ============================
def categorize_source(text, src_map):
    low = text.lower()
    for de, para in src_map.items():
        if de in low:
            return para
    return "No match"

def categorize_topic(text, top_map):
    low = text.lower()
    for terms, para in top_map:
        if any(de in low for de in terms):
            return para
    return "No match"

# ============================
# RSS Parsing
# ============================
def parse_rss_feed(xml_string, src_map, top_map):
    root = ET.fromstring(xml_string)
    ch   = root.find('channel')
    if ch is None:
        return []

    items = []
    for itm in ch.findall('item'):
        title    = itm.findtext('title', default="")
        link     = itm.findtext('link', default="")
        raw_date = itm.findtext('pubDate', default="")
        # formata data
        if raw_date:
            try:
                dt = parsedate_to_datetime(raw_date)
                formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                formatted_date = raw_date
        else:
            formatted_date = ""
        raw = ET.tostring(itm, encoding='unicode')
        items.append({
            'Título':             title,
            'Data de publicação': formatted_date,
            'Fonte':              categorize_source(raw, src_map),
            'Categoria':          categorize_topic(raw, top_map),
            'URL':                link,
            'Prioridade':         "Não definida",
            'Status':             "Não definido"
        })
    return items

# ============================
# Sheets I/O
# ============================
def read_rss_urls():
    r = service.values().get(spreadsheetId=SPREADSHEET_ID, range=RSS_RANGE).execute()
    return [row[0] for row in r.get('values', []) if row]

def write_results(df: pd.DataFrame):
    vals = [df.columns.tolist()] + df.values.tolist()
    body = {'values': vals}
    service.values().clear(spreadsheetId=SPREADSHEET_ID, range=RESULTS_RANGE_START).execute()
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RESULTS_RANGE_START,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

# ============================
# Fluxo principal
# ============================
def main():
    src_map   = load_source_mapping()
    top_map   = load_topic_mapping()
    urls      = read_rss_urls()
    all_items = []
    for url in urls:
        resp = requests.get(url, timeout=10); resp.raise_for_status()
        all_items.extend(parse_rss_feed(resp.text, src_map, top_map))
    if all_items:
        df = pd.DataFrame(all_items)
        write_results(df)

# ============================
# Handler Vercel
# ============================
def handler(request):
    try:
        main()
        return { 'statusCode': 200, 'body': 'Concluído com sucesso.' }
    except Exception as e:
        return { 'statusCode': 500, 'body': f'Erro: {e}' }
