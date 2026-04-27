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
    return api_key

def update_timestamp(ss, name, status):
    try:
        sheet = ss.worksheet('Настройки')
        now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        sheet.update(values=[['Последнее обновление:', name, now, status]], range_name='D2')
        print(f'Индикатор: {name} — {status}')
    except Exception as e:
        print(f'Ошибка индикатора: {e}')

def update_dates(ss):
    print('Обновляем даты...')
    sheet = ss.worksheet('Настройки')
    today = datetime.now()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=6)
    sheet.update_acell('B3', date_from.strftime('%Y-%m-%d'))
    sheet.update_acell('B4', date_to.strftime('%Y-%m-%d'))
    print(f'Даты: {date_from.strftime("%Y-%m-%d")} - {date_to.strftime("%Y-%m-%d")}')
    return date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')

def wb_get(url, api_key):
    while True:
        resp = requests.get(url, headers={'Authorization': api_key})
        if resp.status_code == 429:
            print('Лимит - ждем 60 сек...')
            time.sleep(60)
            continue
        return resp

def wb_post(url, api_key, body):
    while True:
        resp = requests.post(url, json=body, headers={'Authorization': api_key, 'Content-Type': 'application/json'})
        if resp.status_code == 429:
            print('Лимит - ждем 60 сек...')
            time.sleep(60)
            continue
        return resp

def write_sheet(ss, name, rows):
    sheet = ss.worksheet(name)
    sheet.clear()
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        if i == 0:
            sheet.update(chunk, 'A1')
        else:
            sheet.append_rows(chunk)
        time.sleep(2)

def load_funnel(api_key, date_from, date_to, ss, sheet_name='Воронка'):
    print(f'Загружаем воронку -> {sheet_name}...')
    update_timestamp(ss, sheet_name, '🔄 Загружается...')
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
        resp = wb_post(url, api_key, body)
        if resp.status_code != 200:
            print(f'Ошибка: {resp.text}')
            update_timestamp(ss, sheet_name, '❌ Ошибка')
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
    write_sheet(ss, sheet_name, rows)
    update_timestamp(ss, sheet_name, f'✅ Готово — {len(all_products)} товаров')
    print(f'{sheet_name} загружена! Товаров: {len(all_products)}')

def load_stocks(api_key, date_from, ss):
    print('Загружаем остатки...')
    update_timestamp(ss, 'Остатки', '🔄 Загружается...')
    url = f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}T00:00:00'
    resp = wb_get(url, api_key)
    if resp.status_code != 200:
        update_timestamp(ss, 'Остатки', '❌ Ошибка')
        return
    data = resp.json()
    if not data:
        return
    headers_row = list(data[0].keys())
    rows = [headers_row] + [[str(item.get(h, '')) for h in headers_row] for item in data]
    write_sheet(ss, 'Остатки', rows)
    update_timestamp(ss, 'Остатки', f'✅ Готово — {len(data)} позиций')
    print(f'Остатки загружены! Позиций: {len(data)}')

def load_sales(api_key, date_from, ss):
    print('Загружаем продажи...')
    update_timestamp(ss, 'Продажи', '🔄 Загружается...')
    all_sales = []
    current = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.now() - timedelta(days=1)
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        print(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_get(url, api_key)
        if resp.status_code == 200:
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
    print(f'Продажи загружены! Строк: {len(all_sales)}')

def load_orders(api_key, date_from, ss):
    print('Загружаем заказы...')
    update_timestamp(ss, 'Заказы', '🔄 Загружается...')
    all_orders = []
    current = datetime.strptime(date_from, '%Y-%m-%d')
    end = datetime.now() - timedelta(days=1)
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        print(f'День: {date_str}')
        url = f'https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={date_str}T00:00:00&flag=1'
        resp = wb_get(url, api_key)
        if resp.status_code == 200:
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
    print(f'Заказы загружены! Строк: {len(all_orders)}')

def load_ads(api_key, date_from, date_to, ss):
    print('Загружаем рекламу...')
    update_timestamp(ss, 'Реклама', '🔄 Загружается...')
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    resp = wb_get(url, api_key)
    if resp.status_code != 200:
        update_timestamp(ss, 'Реклама', '❌ Ошибка')
        return
    all_ids = []
    for item in resp.json().get('adverts', []):
        # Берём только активные (9) и на паузе (11) кампании
        if item.get('status') in [9, 11]:
            for advert in item.get('advert_list', []):
                all_ids.append(advert.get('advertId'))
    print(f'Кампаний: {len(all_ids)}')
    all_stats = []
    for i in range(0, len(all_ids), 50):
        chunk = all_ids[i:i+50]
        stats_url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, chunk))}&beginDate={date_from}&endDate={date_to}'
        resp = wb_get(stats_url, api_key)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                all_stats.extend(data)
        time.sleep(22)
    if not all_stats:
        return
    rows = [['ID', 'Название', 'Показы', 'Клики', 'CTR', 'CPC', 'Расход', 'Заказы', 'Сумма заказов', 'ДРР']]
    for camp in all_stats:
        views = sum(day.get('views', 0) for day in camp.get('days', []))
        clicks = sum(day.get('clicks', 0) for day in camp.get('days', []))
        spend = sum(day.get('sum', 0) for day in camp.get('days', []))
        orders = sum(day.get('orders', 0) for day in camp.get('days', []))
        order_sum = sum(day.get('sum_price', 0) for day in camp.get('days', []))
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        drr = round(spend / order_sum * 100, 1) if order_sum > 0 else 0
        rows.append([camp.get('advertId', ''), camp.get('advertName', ''),
            views, clicks, ctr, cpc, spend, orders, order_sum, drr])
    write_sheet(ss, 'Реклама', rows)
    update_timestamp(ss, 'Реклама', f'✅ Готово — {len(all_stats)} кампаний')
    print(f'Реклама загружена! Кампаний: {len(all_stats)}')

def load_rk_period(api_key, date_from, date_to, ss, sheet_name):
    print(f'Загружаем {sheet_name}...')
    update_timestamp(ss, sheet_name, '🔄 Загружается...')
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    resp = wb_get(url, api_key)
    if resp.status_code != 200:
        update_timestamp(ss, sheet_name, '❌ Ошибка')
        return
    all_ids = []
    for item in resp.json().get('adverts', []):
        for advert in item.get('advert_list', []):
            all_ids.append(advert.get('advertId'))
    nm_stats = {}
    for i in range(0, len(all_ids), 50):
        chunk = all_ids[i:i+50]
        stats_url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, chunk))}&beginDate={date_from}&endDate={date_to}'
        resp = wb_get(stats_url, api_key)
        if resp.status_code == 200:
            data = resp.json()
            if not data:
                time.sleep(22)
                continue
            for camp in data:
                for day in camp.get('days', []):
                    for app in day.get('apps', []):
                        for nm in app.get('nms', []):
                            key = str(nm.get('nmId'))
                            if key not in nm_stats:
                                nm_stats[key] = {'nmId': nm.get('nmId'), 'name': nm.get('name', ''),
                                    'views': 0, 'clicks': 0, 'sum': 0, 'orders': 0, 'orderSum': 0}
                            nm_stats[key]['views'] += nm.get('views', 0)
                            nm_stats[key]['clicks'] += nm.get('clicks', 0)
                            nm_stats[key]['sum'] += nm.get('sum', 0)
                            nm_stats[key]['orders'] += nm.get('orders', 0)
                            nm_stats[key]['orderSum'] += nm.get('sum_price', 0)
        time.sleep(22)
    if not nm_stats:
        return
    rows = [['Артикул WB', 'Название', 'Показы', 'Клики', 'CTR %', 'CPC ₽', 'Расход ₽', 'Заказы', 'Сумма заказов ₽', 'ДРР %']]
    for nm in nm_stats.values():
        ctr = round(nm['clicks'] / nm['views'] * 100, 2) if nm['views'] > 0 else 0
        cpc = round(nm['sum'] / nm['clicks'], 2) if nm['clicks'] > 0 else 0
        drr = round(nm['sum'] / nm['orderSum'] * 100, 1) if nm['orderSum'] > 0 else 0
        rows.append([nm['nmId'], nm['name'], nm['views'], nm['clicks'], ctr, cpc, nm['sum'], nm['orders'], nm['orderSum'], drr])
    rows.sort(key=lambda x: x[6] if isinstance(x[6], (int, float)) else 0, reverse=True)
    write_sheet(ss, sheet_name, rows)
    update_timestamp(ss, sheet_name, f'✅ Готово — {len(nm_stats)} артикулов')
    print(f'{sheet_name} загружена! Артикулов: {len(nm_stats)}')

if __name__ == '__main__':
    print(f'Запуск: {datetime.now()}')
    client = get_client()
    ss = client.open_by_key(SPREADSHEET_ID)
    api_key = get_config()

    # Обновляем даты
    update_timestamp(ss, 'Все данные', '🔄 Обновление началось...')
    date_from, date_to = update_dates(ss)
    time.sleep(3)

    # Основные данные
    load_funnel(api_key, date_from, date_to, ss)
    time.sleep(10)
    load_stocks(api_key, date_from, ss)
    time.sleep(10)
    load_sales(api_key, date_from, ss)
    time.sleep(10)
    load_orders(api_key, date_from, ss)
    time.sleep(10)
    load_ads(api_key, date_from, date_to, ss)
    time.sleep(10)

    # РК по периодам
    today = datetime.now()
    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    week_from = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    days14_from = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    month_from = today.replace(day=1).strftime('%Y-%m-%d')

    load_rk_period(api_key, yesterday, yesterday, ss, 'РК День')
    time.sleep(10)
    load_rk_period(api_key, week_from, yesterday, ss, 'РК Неделя')
    time.sleep(10)
    load_rk_period(api_key, days14_from, yesterday, ss, 'РК 14 Дней')
    time.sleep(10)
    load_rk_period(api_key, month_from, yesterday, ss, 'РК Месяц')
    time.sleep(10)

    # Воронка по периодам
    load_funnel(api_key, yesterday, yesterday, ss, 'Воронка День')
    time.sleep(10)
    load_funnel(api_key, week_from, yesterday, ss, 'Воронка Неделя')
    time.sleep(10)
    load_funnel(api_key, days14_from, yesterday, ss, 'Воронка 14 Дней')
    time.sleep(10)
    load_funnel(api_key, month_from, yesterday, ss, 'Воронка Месяц')

    # Финальный индикатор
    update_timestamp(ss, 'Все данные', f'✅ Обновление завершено — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    print(f'Готово: {datetime.now()}')
