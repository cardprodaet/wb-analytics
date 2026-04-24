import requests
import gspread
import time
import schedule
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
    api_key = sheet.acell('B2').value
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

def load_funnel():
    print('Загружаем воронку...')
    api_key, date_from, date_to = get_config()
    url = 'https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products'
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    all_products = []
    offset = 0
    limit = 1000
    page = 1
    while True:
        print(f'Страница {page} offset={offset}')
        body = {
            'selectedPeriod': {'start': date_from, 'end': date_to},
            'nmIds': [], 'brandNames': [], 'subjectIds': [], 'tagIds': [],
            'skipDeletedNm': False, 'limit': limit, 'offset': offset
        }
        while True:
            resp = requests.post(url, json=body, headers=headers)
            if resp.status_code == 429:
                print('Лимит - ждем 60 сек...')
                time.sleep(60)
                continue
            break
        if resp.status_code != 200:
            print(f'Ошибка: {resp.text}')
            break
        data = resp.json()
        products = data.get('data', {}).get('products', [])
        if not products:
            break
        all_products.extend(products)
        print(f'Страница {page}: {len(products)} товаров, всего: {len(all_products)}')
        if len(products) < limit:
            break
        offset += limit
        page += 1
        time.sleep(20)
    if not all_products:
        print('Нет данных')
        return
    client = get_client()
    ss = client.open_by_key(SPREADSHEET_ID)
    sheet = ss.worksheet('Воронка')
    sheet.clear()
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
        s_hours = st.get('days', 0) * 24 + st.get('hours', 0)
        p_hours = pt.get('days', 0) * 24 + pt.get('hours', 0)
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
            prod.get('feedbackRating', 0), s_hours, p_hours,
        ])
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        if i == 0:
            sheet.update(chunk, 'A1')
        else:
            sheet.append_rows(chunk)
        time.sleep(2)
    print(f'Воронка загружена! Товаров: {len(all_products)}')

def run_daily_update():
    print(f'Запуск обновления: {datetime.now()}')
    update_dates()
    time.sleep(5)
    load_funnel()
    print(f'Обновление завершено: {datetime.now()}')

if __name__ == '__main__':
    print('WB Аналитика - Python бот запущен!')
    schedule.every().day.at('01:00').do(run_daily_update)
    #print('Запускаем тестовое обновление...')
    #run_daily_update()
    while True:
        schedule.run_pending()
        time.sleep(60)
