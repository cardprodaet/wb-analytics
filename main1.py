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

def update_dates(ss):
    log.info('Обновляем даты...')
    sheet = ss.worksheet('Настройки')
    today = datetime.now()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=6)
    sheet.update_acell('B3', date_from.strftime('%Y-%m-%d'))
    sheet.update_acell('B4', date_to.strftime('%Y-%m-%d'))
    log.info(f'Даты: {date_from.strftime("%Y-%m-%d")} - {date_to.strftime("%Y-%m-%d")}')
    return date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')

def wb_request(method, url, api_key, max_retries=10, **kwargs):
    headers = {'Authorization': api_key}
    if 'json' in kwargs:
        headers['Content-Type'] = 'application/json'
    for attempt in range(max_retries):
        try:
            resp = getattr(requests, method)(url, headers=headers, timeout=30, **kwargs)
            if resp.status_code == 429:
                log.warning(f'Лимит 429 — ждем 30 сек...')
                time.sleep(30)
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

def load_funnel(api_key, date_from, date_to, ss):
    log.info('Загружаем воронку...')
    update_timestamp(ss, 'Воронка', '🔄 Загружается...')
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
        update_timestamp(ss, 'Воронка', '❌ Нет данных')
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
    write_sheet(ss, 'Воронка', rows)
    update_timestamp(ss, 'Воронка', f'✅ Готово — {len(all_products)} товаров')

def load_stocks(api_key, date_from, ss):
    log.info('Загружаем остатки...')
    update_timestamp(ss, 'Остатки', '🔄 Загружается...')
    url = f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}T00:00:00'
    resp = wb_request('get', url, api_key)
    if not resp:
        update_timestamp(ss, 'Остатки', '❌ Ошибка')
        return
    data = resp.json()
    if not data:
        return
    headers_row = list(data[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in data]
    write_sheet(ss, 'Остатки', rows)
    update_timestamp(ss, 'Остатки', f'✅ Готово — {len(data)} позиций')

def load_sales(api_key, date_from, ss):
    log.info('Загружаем продажи...')
    update_timestamp(ss, 'Продажи', '🔄 Загружается...')
    all_sales = []
    current = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.now() - timedelta(days=1)
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        log.info(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_request('get', url, api_key)
        if resp:
            data = resp.json()
            if data:
                all_sales.extend(data)
        time.sleep(65)
        current += timedelta(days=1)
    if not all_sales:
        return
    headers_row = list(all_sales[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in all_sales]
    write_sheet(ss, 'Продажи', rows)
    update_timestamp(ss, 'Продажи', f'✅ Готово — {len(all_sales)} строк')

def load_orders(api_key, date_from, ss):
    log.info('Загружаем заказы...')
    update_timestamp(ss, 'Заказы', '🔄 Загружается...')
    all_orders = []
    current = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.now() - timedelta(days=1)
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        log.info(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_request('get', url, api_key)
        if resp:
            data = resp.json()
            if data:
                all_orders.extend(data)
        time.sleep(65)
        current += timedelta(days=1)
    if not all_orders:
        return
    headers_row = list(all_orders[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in all_orders]
    write_sheet(ss, 'Заказы', rows)
    update_timestamp(ss, 'Заказы', f'✅ Готово — {len(all_orders)} строк')

if __name__ == '__main__':
    log.info(f'Запуск main1: {datetime.now()}')
    ss = get_spreadsheet()
    api_key = get_api_key(ss)
    update_timestamp(ss, 'Все данные', '🔄 Обновление началось...')
    date_from, date_to = update_dates(ss)
    time.sleep(3)
    load_funnel(api_key, date_from, date_to, ss)
    time.sleep(10)
    load_stocks(api_key, date_from, ss)
    time.sleep(10)
    load_sales(api_key, date_from, ss)
    time.sleep(10)
    load_orders(api_key, date_from, ss)
    update_timestamp(ss, 'Основные данные', f'✅ Готово — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    log.info(f'main1 завершён: {datetime.now()}')
