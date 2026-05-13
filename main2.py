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
 
def get_dates(ss):
    sheet = ss.worksheet('Настройки')
    return sheet.acell('B3').value, sheet.acell('B4').value
 
def update_timestamp(ss, name, status):
    try:
        sheet = ss.worksheet('Настройки')
        now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        sheet.update(values=[['Последнее обновление:', name, now, status]], range_name='D2')
        log.info(f'Индикатор: {name} — {status}')
    except Exception as e:
        log.error(f'Ошибка индикатора: {e}')
 
def wb_request(method, url, api_key, max_retries=5, **kwargs):
    """ВАЖНО: если приходит 404 — НЕ ретраим, сразу возвращаем None.
    404 = неправильный путь, ретраи бесполезны и забивают лог."""
    headers = {'Authorization': api_key}
    if 'json' in kwargs:
        headers['Content-Type'] = 'application/json'
    for attempt in range(max_retries):
        try:
            resp = getattr(requests, method)(url, headers=headers, timeout=30, **kwargs)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning(f'Лимит 429 — ждем {wait} сек...')
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                log.error(f'404 Not Found: {url[:120]} — ретраи отключены, возвращаем None')
                return None
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
 
def get_campaign_ids(api_key):
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    resp = wb_request('get', url, api_key)
    if not resp:
        return []
    all_ids = []
    for item in resp.json().get('adverts', []):
        for advert in item.get('advert_list', []):
            all_ids.append(advert.get('advertId'))
    return all_ids
 
def get_all_rk_stats(api_key, all_ids, date_from, date_to):
    """Загружаем статистику по всем кампаниям за один период"""
    all_stats = []
    log.info(f'Загружаем статистику {len(all_ids)} кампаний за {date_from} — {date_to}')
    for i in range(0, len(all_ids), 50):
        chunk = all_ids[i:i+50]
        url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, chunk))}&beginDate={date_from}&endDate={date_to}'
        resp = wb_request('get', url, api_key)
        if resp:
            data = resp.json()
            if data:
                all_stats.extend(data)
        time.sleep(22)
        if (i // 50 + 1) % 10 == 0:
            log.info(f'Прогресс: {i+50}/{len(all_ids)} кампаний')
    return all_stats
 
def build_camp_names_from_stats(all_stats):
    """Собираем мапу {advertId(str): name} из ответа fullstats.
    Если в одной кампании name пустое — берём из следующей встречи того же ID."""
    camp_names = {}
    for camp in all_stats:
        adv_id = camp.get('advertId')
        if adv_id is None:
            continue
        key = str(adv_id)
        name = camp.get('name') or camp.get('advertName') or ''
        # перезаписываем только если ещё нет ИЛИ текущее значение пустое
        if name and (key not in camp_names or not camp_names[key]):
            camp_names[key] = name
    log.info(f'Собрано названий кампаний из fullstats: {len(camp_names)}')
    return camp_names
 
def write_rk_from_stats(all_stats, date_from, date_to, ss, sheet_name, camp_names=None):
    """Записываем РК период из уже загруженной статистики, фильтруя по датам.
    Колонка «Кампания» подтягивается из camp_names (по advertId)."""
    if camp_names is None:
        camp_names = {}
    log.info(f'Записываем {sheet_name} за {date_from} — {date_to}')
    update_timestamp(ss, sheet_name, '🔄 Записываем...')
 
    dt_from = datetime.strptime(date_from, '%Y-%m-%d')
    dt_to = datetime.strptime(date_to, '%Y-%m-%d')
 
    nm_stats = {}
    for camp in all_stats:
        camp_id = str(camp.get('advertId', ''))
        for day in camp.get('days', []):
            day_date_str = day.get('date', '')[:10]
            if not day_date_str:
                continue
            try:
                day_date = datetime.strptime(day_date_str, '%Y-%m-%d')
            except ValueError:
                continue
            if not (dt_from <= day_date <= dt_to):
                continue
            for app in day.get('apps', []):
                for nm in app.get('nms', []):
                    key = (str(nm.get('nmId')), camp_id)
                    if key not in nm_stats:
                        nm_stats[key] = {
                            'nmId': nm.get('nmId'),
                            'name': nm.get('name', ''),
                            'campId': camp_id,
                            'views': 0, 'clicks': 0,
                            'sum': 0, 'orders': 0, 'orderSum': 0
                        }
                    nm_stats[key]['views']    += nm.get('views', 0)
                    nm_stats[key]['clicks']   += nm.get('clicks', 0)
                    nm_stats[key]['sum']      += nm.get('sum', 0)
                    nm_stats[key]['orders']   += nm.get('orders', 0)
                    nm_stats[key]['orderSum'] += nm.get('sum_price', 0)
 
    if not nm_stats:
        update_timestamp(ss, sheet_name, '❌ Нет данных')
        return
 
    # ФИКС: шапка отдельно, сортируем только данные
    header = ['Артикул WB', 'Название', 'Кампания', 'Показы', 'Клики',
              'CTR %', 'CPC ₽', 'Расход ₽', 'Заказы', 'Сумма заказов ₽', 'ДРР %',
              f'Период: {date_from} — {date_to}']
 
    data_rows = []
    for nm in nm_stats.values():
        ctr = round(nm['clicks'] / nm['views'] * 100, 2) if nm['views'] > 0 else 0
        cpc = round(nm['sum'] / nm['clicks'], 2) if nm['clicks'] > 0 else 0
        drr = round(nm['sum'] / nm['orderSum'] * 100, 1) if nm['orderSum'] > 0 else 0
        camp_name = camp_names.get(nm['campId'], '')
        data_rows.append([nm['nmId'], nm['name'], camp_name, nm['views'], nm['clicks'],
            ctr, cpc, nm['sum'], nm['orders'], nm['orderSum'], drr, ''])
 
    # Сортируем ТОЛЬКО данные по расходу (индекс 7 — Расход ₽)
    data_rows.sort(key=lambda x: x[7] if isinstance(x[7], (int, float)) else 0, reverse=True)
 
    rows = [header] + data_rows
    write_sheet(ss, sheet_name, rows)
    update_timestamp(ss, sheet_name, f'✅ Готово — {len(nm_stats)} строк')
 
def load_ads(api_key, date_from, date_to, ss):
    """Загружаем рекламу. Названия кампаний берём из самого ответа fullstats."""
    log.info('Загружаем рекламу...')
    update_timestamp(ss, 'Реклама', '🔄 Загружается...')
    all_ids = get_campaign_ids(api_key)
    if not all_ids:
        update_timestamp(ss, 'Реклама', '❌ Нет кампаний')
        return {}
    log.info(f'Кампаний: {len(all_ids)}')
    all_stats = []
    for i in range(0, len(all_ids), 50):
        chunk = all_ids[i:i+50]
        url = f'https://advert-api.wildberries.ru/adv/v3/fullstats?ids={",".join(map(str, chunk))}&beginDate={date_from}&endDate={date_to}'
        resp = wb_request('get', url, api_key)
        if resp:
            data = resp.json()
            if data:
                all_stats.extend(data)
        time.sleep(22)
    if not all_stats:
        update_timestamp(ss, 'Реклама', '❌ Нет данных')
        return {}
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
        camp_id = str(camp.get('advertId', ''))
        # Название кампании теперь берём прямо из ответа API
        camp_name = camp.get('name') or camp.get('advertName') or ''
        rows.append([camp_id, camp_name,
            views, clicks, ctr, cpc, spend, orders, order_sum, drr])
    write_sheet(ss, 'Реклама', rows)
    update_timestamp(ss, 'Реклама', f'✅ Готово — {len(all_stats)} кампаний')
    # Возвращаем названия — пригодится если в main2 они нужны для каких-то расчётов
    return build_camp_names_from_stats(all_stats)
 
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
    headers_row_period = f'Период: {date_from} — {date_to}'
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
        'Время доставки ч', 'Время доставки пред ч', headers_row_period
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
    log.info(f'Запуск main2: {datetime.now()}')
    ss = get_spreadsheet()
    api_key = get_api_key(ss)
    date_from, date_to = get_dates(ss)
 
    today = datetime.now()
    yesterday   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    week_from   = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    days14_from = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    days14_to   = (today - timedelta(days=8)).strftime('%Y-%m-%d')
    month_from  = today.replace(day=1).strftime('%Y-%m-%d')
 
    # 1. Реклама (названия берём прямо из ответа fullstats)
    load_ads(api_key, date_from, date_to, ss)
    time.sleep(10)
 
    # 2. Загружаем все РК за месяц ОДИН РАЗ и собираем названия из ответа
    log.info('Загружаем все РК периоды за один проход...')
    all_ids = get_campaign_ids(api_key)
    if all_ids:
        month_stats = get_all_rk_stats(api_key, all_ids, month_from, yesterday)
        camp_names = build_camp_names_from_stats(month_stats)
        time.sleep(10)
        write_rk_from_stats(month_stats, yesterday, yesterday, ss, 'РК День', camp_names)
        time.sleep(5)
        write_rk_from_stats(month_stats, week_from, yesterday, ss, 'РК Неделя', camp_names)
        time.sleep(5)
        write_rk_from_stats(month_stats, days14_from, days14_to, ss, 'РК 14 Дней', camp_names)
        time.sleep(5)
        write_rk_from_stats(month_stats, month_from, yesterday, ss, 'РК Месяц', camp_names)
 
    time.sleep(10)
 
    # 3. Воронки периодов
    load_funnel_period(api_key, yesterday, yesterday, ss, 'Воронка День')
    time.sleep(10)
    load_funnel_period(api_key, week_from, yesterday, ss, 'Воронка Неделя')
    time.sleep(10)
    load_funnel_period(api_key, days14_from, days14_to, ss, 'Воронка 14 Дней')
    time.sleep(10)
    load_funnel_period(api_key, month_from, yesterday, ss, 'Воронка Месяц')
 
    update_timestamp(ss, 'Все данные', f'✅ Всё завершено — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    log.info(f'main2 завершён: {datetime.now()}')
 
