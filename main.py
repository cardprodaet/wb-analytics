import requests
import gspread
import time
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = '1a05NKURoAiCvKhM7t0jLmcAe0pc-kGQA329DiggH5_s'
CREDENTIALS_FILE = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_client():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def get_config():
    client = get_client()
    ss = client.open_by_key(SPREADSHEET_ID)
    sheet = ss.worksheet('Настройки')
    api_key = sheet.acell('B2').value.strip()
    date_from = sheet.acell('B3').value
    date_to = sheet.acell('B4').value
    return api_key, date_from, date_to

def update_dates():
    print('Обновляем даты...')
    client = get_client()
    ss = client.open_by_key(SPREADSHEET_ID)
    sheet = ss.worksheet('Настройки')
    today = datetime.now()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=6)
    sheet.update_acell('B3', date_from.strftime('%Y-%m-%d'))
    sheet.update_acell('B4', date_to.strftime('%Y-%m-%d'))
    print(f'Даты обновлены: {date_from.strftime("%Y-%m-%d")} - {date_to.strftime("%Y-%m-%d")}')
    return date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')

def wb_request(method, url, api_key, **kwargs):
    headers = {'Authorization': api_key}
    if 'json' in kwargs:
        headers['Content-Type'] = 'application/json'
    while True:
        resp = getattr(requests, method)(url, headers=headers, **kwargs)
        if resp.status_code == 429:
            print('Лимит - ждем 60 сек...')
            time.sleep(60)
            continue
        return resp

def write_to_sheet(ss, sheet_name, rows):
    sheet = ss.worksheet(sheet_name)
    sheet.clear()
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        if i == 0:
            sheet.update(chunk, 'A1')
        else:
            sheet.append_rows(chunk)
        time.sleep(2)

def load_funnel(api_key, date_from, date_to, ss):
    print('Загружаем воронку...')
    url = 'https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products'
    all_products = []
    offset = 0
    limit = 1000
    page = 1
    while True:
        print(f'Страница {page}')
        body = {
            'selectedPeriod': {'start': date_from, 'end': date_to},
            'nmIds': [], 'brandNames': [], 'subjectIds': [], 'tagIds': [],
            'skipDeletedNm': False, 'limit': limit, 'offset': offset
        }
        resp = wb_request('post', url, api_key, json=body)
        if resp.status_code != 200:
            print(f'Ошибка: {resp.text}')
            break
        products = resp.json().get('data', {}).get('products', [])
        if not products:
            break
        all_products.extend(products)
        print(f'Всего: {len(all_products)}')
        if len(products) < limit:
            break
        offset += limit
        page += 1
        time.sleep(20)
    if not all_products:
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
    write_to_sheet(ss, 'Воронка', rows)
    print(f'Воронка загружена! Товаров: {len(all_products)}')

def load_stocks(api_key, date_from, ss):
    print('Загружаем остатки...')
    url = f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}T00:00:00'
    resp = wb_request('get', url, api_key)
    if resp.status_code != 200:
        print(f'Ошибка: {resp.text}')
        return
    data = resp.json()
    if not data:
        return
    headers_row = list(data[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in data]
    write_to_sheet(ss, 'Остатки', rows)
    print(f'Остатки загружены! Позиций: {len(data)}')

def load_sales(api_key, date_from, ss):
    print('Загружаем продажи...')
    all_sales = []
    current_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.now() - timedelta(days=1)
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_request('get', url, api_key)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                all_sales.extend(data)
                print(f'Загружено: {len(data)} строк')
        time.sleep(65)
        current_date += timedelta(days=1)
    if not all_sales:
        return
    headers_row = list(all_sales[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in all_sales]
    write_to_sheet(ss, 'Продажи', rows)
    print(f'Продажи загружены! Строк: {len(all_sales)}')

def load_orders(api_key, date_from, ss):
    print('Загружаем заказы...')
    all_orders = []
    current_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.now() - timedelta(days=1)
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_request('get', url, api_key)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                all_orders.extend(data)
                print(f'Загружено: {len(data)} строк')
        time.sleep(65)
        current_date += timedelta(days=1)
    if not all_orders:
        return
    headers_row = list(all_orders[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in all_orders]
    write_to_sheet(ss, 'Заказы', rows)
    print(f'Заказы загружены! Строк: {len(all_orders)}')

def load_ads(api_key, ss):
    print('Загружаем рекламу...')
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    resp = wb_request('get', url, api_key)
    if resp.status_code != 200:
        print(f'Ошибка: {resp.text}')
        return
    data = resp.json()
    all_ids = []
    for item in data.get('adverts', []):
        for advert in item.get('advert_list', []):
            all_ids.append(advert.get('advertId'))
    print(f'Всего кампаний: {len(all_ids)}')
    chunk_size = 50
    all_stats = []
    for i in range(0, len(all_ids), chunk_size):
        chunk = all_ids[i:i+chunk_size]
        stats_url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, chunk))}'
        resp = wb_request('get', stats_url, api_key)
        if resp.status_code == 200:
            all_stats.extend(resp.json())
        time.sleep(22)
    if not all_stats:
        return
    rows = [['ID кампании', 'Название', 'Показы', 'Клики', 'CTR', 'CPC', 'Расход', 'Заказы', 'Сумма заказов', 'ДРР']]
    for camp in all_stats:
        views = sum(day.get('views', 0) for day in camp.get('days', []))
        clicks = sum(day.get('clicks', 0) for day in camp.get('days', []))
        spend = sum(day.get('sum', 0) for day in camp.get('days', []))
        orders = sum(day.get('orders', 0) for day in camp.get('days', []))
        order_sum = sum(day.get('sum_price', 0) for day in camp.get('days', []))
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        drr = round(spend / order_sum * 100, 1) if order_sum > 0 else 0
        rows.append([
            camp.get('advertId', ''), camp.get('advertName', ''),
            views, clicks, ctr, cpc, spend, orders, order_sum, drr
        ])
    write_to_sheet(ss, 'Реклама', rows)
    print(f'Реклама загружена! Кампаний: {len(all_stats)}')

if __name__ == '__main__':
    print(f'Запуск обновления: {datetime.now()}')
    api_key, _, _ = get_config()
    date_from, date_to = update_dates()
    client = get_client()
    ss = client.open_by_key(SPREADSHEET_ID)
    time.sleep(3)
    load_funnel(api_key, date_from, date_to, ss)
    time.sleep(10)
    load_stocks(api_key, date_from, ss)
    time.sleep(10)
    load_sales(api_key, date_from, ss)
    time.sleep(10)
    load_orders(api_key, date_from, ss)
    time.sleep(10)
    load_ads(api_key, ss)
    print(f'Обновление завершено: {datetime.now()}')
