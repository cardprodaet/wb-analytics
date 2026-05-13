import requests
import gspread
import json
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = '1a05NKURoAiCvKhM7t0jLmcAe0pc-kGQA329DiggH5_s'
CREDENTIALS_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
ss = client.open_by_key(SPREADSHEET_ID)
api_key = ss.worksheet('Настройки').acell('B2').value.strip()
headers = {'Authorization': api_key}

print('=' * 60)
print('ШАГ 1: /adv/v1/promotion/count')
print('=' * 60)
r = requests.get('https://advert-api.wildberries.ru/adv/v1/promotion/count', headers=headers, timeout=30)
print(f'Статус: {r.status_code}')
data = r.json()
print(f'Поля верхнего уровня: {list(data.keys())}')
adverts = data.get('adverts', [])
print(f'Групп adverts: {len(adverts)}')

test_ids = []
if adverts:
    first_group = adverts[0]
    print(f'Поля первой группы: {list(first_group.keys())}')
    adv_list = first_group.get('advert_list', [])
    if adv_list:
        print(f'Поля одной кампании из /count: {list(adv_list[0].keys())}')
        print('ПЕРВАЯ КАМПАНИЯ ИЗ /count:')
        print(json.dumps(adv_list[0], ensure_ascii=False, indent=2))
    for item in adverts:
        for adv in item.get('advert_list', []):
            test_ids.append(adv.get('advertId'))
            if len(test_ids) >= 3:
                break
        if len(test_ids) >= 3:
            break
print(f'Тестовые ID: {test_ids}')

today = datetime.now()
date_from = (today - timedelta(days=7)).strftime('%Y-%m-%d')
date_to = (today - timedelta(days=1)).strftime('%Y-%m-%d')

print('')
print('=' * 60)
print(f'ШАГ 2: /adv/v3/fullstats за {date_from} — {date_to}')
print('=' * 60)
url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, test_ids))}&beginDate={date_from}&endDate={date_to}'
r = requests.get(url, headers=headers, timeout=30)
print(f'Статус: {r.status_code}')
if r.status_code == 200:
    stats = r.json()
    print(f'Объектов: {len(stats)}')
    if stats:
        first = stats[0]
        print(f'Поля одной кампании из fullstats: {list(first.keys())}')
        print(f'advertId: {first.get("advertId")}')
        print(f'name: {repr(first.get("name"))}')
        print(f'advertName: {repr(first.get("advertName"))}')
        print('ПЕРВАЯ КАМПАНИЯ ЦЕЛИКОМ (без days):')
        print(json.dumps({k: v for k, v in first.items() if k != 'days'}, ensure_ascii=False, indent=2))
else:
    print(f'Ошибка: {r.text[:500]}')

print('')
print('=' * 60)
print('ШАГ 3: POST /adv/v1/promotion/adverts')
print('=' * 60)
url2 = 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts'
hp = {**headers, 'Content-Type': 'application/json'}

print('--- Вариант А: body = массив ---')
r = requests.post(url2, headers=hp, json=test_ids, timeout=30)
print(f'Статус: {r.status_code} | Ответ: {r.text[:400]}')

print('--- Вариант Б: body = {"id": [...]} ---')
r = requests.post(url2, headers=hp, json={'id': test_ids}, timeout=30)
print(f'Статус: {r.status_code} | Ответ: {r.text[:400]}')

print('')
print('=== DEBUG ЗАВЕРШЁН ===')
