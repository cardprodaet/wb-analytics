import requests
import gspread
import time
import logging
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')
log = logging.getLogger(__name__)

SPREADSHEET_ID = '1a05NKURoAiCvKhM7t0jLmcAe0pc-kGQA329DiggH5_s'
CREDENTIALS_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def get_client():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def get_spreadsheet():
    return get_client().open_by_key(SPREADSHEET_ID)

def get_api_key(ss):
    return ss.worksheet('Настройки').acell('B2').value.strip()

def update_timestamp(ss, name, status):
    try:
        sheet = ss.worksheet('Настройки')
        now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        sheet.update(values=[['Последнее обновление:', name, now, status]], range_name='D2')
        log.info(f'Индикатор: {name} — {status}')
    except Exception as e:
        log.error(f'Ошибка индикатора: {e}')

def wb_request(method, url, api_key, max_retries=10, **kwargs):
    headers = {'Authorization': api_key}
    if 'json' in kwargs:
        headers['Content-Type'] = 'application/json'
    for attempt in range(max_retries):
        try:
            resp = getattr(requests, method)(url, headers=headers, timeout=30, **kwargs)
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f'Лимит 429 — ждем {wait} сек...')
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp
            log.error(f'Ошибка {resp.status_code}: {resp.text[:200]}')
            time.sleep(30)
        except Exception as e:
            log.error(f'Запрос упал: {e}')
            time.sleep(30)
    return None

def write_sheet(ss, name, rows):
    try:
        sheet = ss.worksheet(name)
        sheet.clear()
        time.sleep(3)
        for i in range(0, len(rows), 500):
            chunk = rows[i:i+500]
            if i == 0:
                sheet.update(values=chunk, range_name='A1')
            else:
                sheet.append_rows(chunk)
            time.sleep(2)
        log.info(f'{name}: записано {len(rows)-1} строк')
    except Exception as e:
        log.error(f'Ошибка записи в {name}: {e}')

def load_funnel_period(api_key, date_from, date_to, ss, sheet_name):
    log.info(f'Загружаем {sheet_name}...')
    update_timestamp(ss, sheet_name, '🔄 Загружается...')
    url = 'https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products'
    all_products = []
    offset = 0
    limit = 1000
    page = 1
    while True:
        log.info(f'Страница {page}')
        body = {
            'selectedPeriod': {'start': date_from, 'end': date_to},
            'nmIds': [], 'brandNames': [], 'subjectIds': [], 'tagIds': [],
            'skipDeletedNm': False, 'limit': limit, 'offset': offset
        }
        resp = wb_request('post', url, api_key, json=body)
        if not resp:
            break
        products = resp.json().get('data', {}).get('products', [])
        if not products:
            break
        all_products.extend(products)
        log.info(f'Всего: {len(all_products)}')
        if len(products) < limit:
            break
        offset += limit
        page += 1
        time.sleep(60)
    if not all_products:
        update_timestamp(ss, sheet_name, '❌ Нет данных')
        return
    headers_row = [
        'Артикул продавца', 'Артикул WB', 'Название', 'Предмет', 'Бренд',
        'Переходы в карточку', 'Переходы (пред.период)',
        'В корзину, шт', 'В корзину (пред.)',
        'Конв. в корзину, %', 'Конв. в корзину (пред.%)',
        'В отложенные, шт', 'В отложенные (пред.)',
        'Заказали, шт', 'Заказали (пред.)',
        'Конв. в заказ, %', 'Конв. в заказ (пред.%)',
        'Выкупили, шт', 'Выкупили (пред.)',
        '% выкупа', '% выкупа (пред.)',
        'Отменили, шт', 'Отменили (пред.)',
        'Заказали на сумму', 'Заказали сумма (пред.)',
        'Выкупили на сумму', 'Выкупили сумма (пред.)',
        'Средняя цена', 'Средняя цена (пред.)',
        'Остатки WB', 'Рейтинг товара', 'Рейтинг отзывов',
        'Время доставки ч', 'Время доставки пред ч',
    ]
    rows = [headers_row]
    for item in all_products:
        prod = item.get('product', {})
        s = item.get('statistic', {}).get('selected', {})
        p = item.get('statistic', {}).get('past', {})
        sc = s.get('conversions', {})
        pc = p.get('conversions', {})
        st = s.get('timeToReady', {})
        pt = p.get('timeToReady', {})
        stk = prod.get('stocks', {})
        rows.append([
            prod.get('vendorCode', ''), prod.get('nmId', ''),
            prod.get('title', ''), prod.get('subjectName', ''), prod.get('brandName', ''),
            s.get('openCount', 0), p.get('openCount', 0),
            s.get('cartCount', 0), p.get('cartCount', 0),
            sc.get('addToCartPercent', 0), pc.get('addToCartPercent', 0),
            s.get('addToWishlist', 0), p.get('addToWishlist', 0),
            s.get('orderCount', 0), p.get('orderCount', 0),
            sc.get('cartToOrderPercent', 0), pc.get('cartToOrderPercent', 0),
            s.get('buyoutCount', 0), p.get('buyoutCount', 0),
            sc.get('buyoutPercent', 0), pc.get('buyoutPercent', 0),
            s.get('cancelCount', 0), p.get('cancelCount', 0),
            s.get('orderSum', 0), p.get('orderSum', 0),
            s.get('buyoutSum', 0), p.get('buyoutSum', 0),
            s.get('avgPrice', 0), p.get('avgPrice', 0),
            stk.get('wb', 0), prod.get('productRating', 0),
            prod.get('feedbackRating', 0),
            st.get('days', 0) * 24 + st.get('hours', 0),
            pt.get('days', 0) * 24 + pt.get('hours', 0),
        ])
    write_sheet(ss, sheet_name, rows)
    update_timestamp(ss, sheet_name, f'✅ Готово — {len(all_products)} товаров')

if __name__ == '__main__':
    log.info(f'Запуск main3: {datetime.now()}')
    ss = get_spreadsheet()
    api_key = get_api_key(ss)

    today = datetime.now()
    yesterday   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    week_from   = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    days14_from = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    days14_to   = (today - timedelta(days=8)).strftime('%Y-%m-%d')
    month_from  = today.replace(day=1).strftime('%Y-%m-%d')

    load_funnel_period(api_key, yesterday, yesterday, ss, 'Воронка День')
    time.sleep(10)
    load_funnel_period(api_key, week_from, yesterday, ss, 'Воронка Неделя')
    time.sleep(10)
    load_funnel_period(api_key, days14_from, days14_to, ss, 'Воронка 14 Дней')
    time.sleep(10)
    load_funnel_period(api_key, month_from, yesterday, ss, 'Воронка Месяц')

    update_timestamp(ss, 'Все данные', f'✅ Всё завершено — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    log.info(f'main3 завершён: {datetime.now()}')
