#!/usr/bin/env python3
"""
WB Analytics — main2.py
РК по периодам + Воронка по периодам.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

import gspread
import requests
from google.oauth2.service_account import Credentials

# ── Конфигурация ───────────────────────────────────────────────────────────────

SPREADSHEET_ID   = '1a05NKURoAiCvKhM7t0jLmcAe0pc-kGQA329DiggH5_s'
CREDENTIALS_FILE = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

ADV_BASE       = 'https://advert-api.wildberries.ru'
ANALYTICS_BASE = 'https://seller-analytics-api.wildberries.ru'

CAMP_CHUNK  = 50
ADV_SLEEP   = 90
PAGE_SLEEP  = 20
WRITE_BATCH = 500

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Google Sheets ──────────────────────────────────────────────────────────────

def get_spreadsheet() -> gspread.Spreadsheet:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def get_api_key(ss: gspread.Spreadsheet) -> str:
    return ss.worksheet('Настройки').acell('B2').value.strip()


def set_date_range(ss: gspread.Spreadsheet, date_from: str, date_to: str) -> None:
    try:
        ws       = ss.worksheet('Настройки')
        from_fmt = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y-%m-%d')
        to_fmt   = datetime.strptime(date_to,   '%Y-%m-%d').strftime('%Y-%m-%d')
        ws.update(values=[[from_fmt]], range_name='B3')
        ws.update(values=[[to_fmt]],   range_name='B4')
        log.info('Дата ОТ/ДО обновлены: %s — %s', from_fmt, to_fmt)
    except Exception as exc:
        log.warning('set_date_range error: %s', exc)


def set_status(ss: gspread.Spreadsheet, name: str, status: str) -> None:
    try:
        now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        ss.worksheet('Настройки').update(
            values=[['Последнее обновление:', name, now, status]],
            range_name='D2',
        )
        log.info('%s — %s', name, status)
    except Exception as exc:
        log.warning('set_status error: %s', exc)


def write_sheet(ss: gspread.Spreadsheet, name: str, rows: list[list]) -> None:
    ws = ss.worksheet(name)
    ws.clear()
    time.sleep(2)
    for i in range(0, len(rows), WRITE_BATCH):
        chunk = rows[i : i + WRITE_BATCH]
        if i == 0:
            ws.update(values=chunk, range_name='A1')
        else:
            ws.append_rows(chunk)
        time.sleep(2)
    log.info('%s → %d rows written', name, len(rows) - 1)

# ── HTTP ───────────────────────────────────────────────────────────────────────

def wb_request(
    method:      str,
    url:         str,
    api_key:     str,
    max_retries: int = 5,
    **kwargs,
) -> requests.Response | None:
    headers = {'Authorization': api_key}
    if 'json' in kwargs:
        headers['Content-Type'] = 'application/json'

    for attempt in range(1, max_retries + 1):
        try:
            resp = getattr(requests, method)(url, headers=headers, timeout=30, **kwargs)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                log.error('404 Not Found: %s', url[:120])
                return None

            if resp.status_code == 429:
                wait = 60 * attempt
                log.warning('429 rate limit (attempt %d/%d) — sleeping %ds', attempt, max_retries, wait)
                time.sleep(wait)
                continue

            log.error('HTTP %d: %s', resp.status_code, resp.text[:300])
            time.sleep(30)

        except requests.RequestException as exc:
            log.error('Request error (attempt %d/%d): %s', attempt, max_retries, exc)
            time.sleep(30)

    log.error('All retries exhausted for %s', url)
    return None

# ── Утилиты ────────────────────────────────────────────────────────────────────

def safe_div(num: float, den: float, scale: float = 1, decimals: int = 2) -> float:
    return round(num / den * scale, decimals) if den else 0.0

# ── Кампании ───────────────────────────────────────────────────────────────────

def get_campaigns(api_key: str) -> tuple[list[int], dict[int, str]]:
    resp = wb_request('get', f'{ADV_BASE}/adv/v1/promotion/count', api_key)
    if not resp:
        return [], {}

    all_ids: list[int] = [
        int(advert['advertId'])
        for group  in resp.json().get('adverts', [])
        if group.get('status') != -1
        for advert in group.get('advert_list', [])
        if advert.get('advertId')
    ]
    log.info('Campaigns found: %d', len(all_ids))

    return all_ids, {}


def _fetch_campaign_names(api_key: str, campaign_ids: list[int]) -> dict[int, str]:
    id_to_name: dict[int, str] = {}
    for i in range(0, len(campaign_ids), CAMP_CHUNK):
        chunk   = campaign_ids[i : i + CAMP_CHUNK]
        ids_str = ','.join(map(str, chunk))
        resp    = wb_request('get', f'{ADV_BASE}/adv/v1/promotion/adverts?id={ids_str}', api_key)
        if resp:
            data = resp.json()
            if isinstance(data, list):
                for adv in data:
                    adv_id = adv.get('advertId') or adv.get('id')
                    name   = adv.get('name') or adv.get('campaignName') or '—'
                    if adv_id:
                        id_to_name[int(adv_id)] = name
        time.sleep(1)
    return id_to_name

# ── Рекламная статистика ───────────────────────────────────────────────────────

def fetch_fullstats(
    api_key:      str,
    campaign_ids: list[int],
    date_from:    str,
    date_to:      str,
) -> list[dict]:
    all_stats: list[dict] = []

    for i in range(0, len(campaign_ids), CAMP_CHUNK):
        chunk   = campaign_ids[i : i + CAMP_CHUNK]
        ids_str = ','.join(map(str, chunk))
        url     = f'{ADV_BASE}/adv/v3/fullstats?ids={ids_str}&beginDate={date_from}&endDate={date_to}'
        resp    = wb_request('get', url, api_key)
        if resp:
            data = resp.json()
            if isinstance(data, list):
                all_stats.extend(data)
        if i + CAMP_CHUNK < len(campaign_ids):
            time.sleep(ADV_SLEEP)

    log.info('Fullstats: %d campaign records', len(all_stats))
    return all_stats

# ── Загрузчики ─────────────────────────────────────────────────────────────────

def write_rk_period(
    month_stats: list[dict],
    id_to_name:  dict[int, str],
    date_from:   str,
    date_to:     str,
    ss:          gspread.Spreadsheet,
    sheet_name:  str,
) -> None:
    log.info('write_rk_period [%s]: %s → %s', sheet_name, date_from, date_to)
    set_status(ss, sheet_name, '🔄 Записываем...')

    dt_from = datetime.strptime(date_from, '%Y-%m-%d')
    dt_to   = datetime.strptime(date_to,   '%Y-%m-%d')

    nm_stats: dict[tuple, dict] = {}
    for camp in month_stats:
        adv_id    = camp.get('advertId')
        camp_name = id_to_name.get(int(adv_id), '—') if adv_id else '—'

        for day in camp.get('days', []):
            day_str = (day.get('date') or '')[:10]
            if not day_str:
                continue
            try:
                day_dt = datetime.strptime(day_str, '%Y-%m-%d')
            except ValueError:
                continue
            if not (dt_from <= day_dt <= dt_to):
                continue

            for app in day.get('apps', []):
                for nm in app.get('nms', []):
                    nm_id = nm.get('nmId')
                    if not nm_id:
                        continue
                    key = (nm_id, str(adv_id))
                    if key not in nm_stats:
                        nm_stats[key] = {
                            'nmId':     nm_id,
                            'name':     nm.get('name', '—'),
                            'campName': camp_name,
                            'views':    0, 'clicks':   0,
                            'sum':      0, 'orders':   0, 'orderSum': 0,
                        }
                    s = nm_stats[key]
                    s['views']    += nm.get('views',     0)
                    s['clicks']   += nm.get('clicks',    0)
                    s['sum']      += nm.get('sum',       0)
                    s['orders']   += nm.get('orders',    0)
                    s['orderSum'] += nm.get('sum_price', 0)

    if not nm_stats:
        set_status(ss, sheet_name, '❌ Нет данных')
        return

    headers = [
        'Артикул WB', 'Название', 'Кампания',
        'Показы', 'Клики', 'CTR, %', 'CPC, ₽',
        'Расход, ₽', 'Заказы', 'Сумма заказов, ₽', 'ДРР, %',
    ]
    rows: list[list] = [headers]

    for s in nm_stats.values():
        rows.append([
            s['nmId'], s['name'], s['campName'],
            s['views'], s['clicks'],
            safe_div(s['clicks'], s['views'],    scale=100),
            safe_div(s['sum'],    s['clicks']),
            s['sum'], s['orders'], s['orderSum'],
            safe_div(s['sum'],    s['orderSum'], scale=100, decimals=1),
        ])

    rows[1:] = sorted(rows[1:], key=lambda r: r[7], reverse=True)
    write_sheet(ss, sheet_name, rows)
    set_status(ss, sheet_name, f'✅ Готово — {len(nm_stats)} артикулов')


def load_funnel_period(
    api_key:    str,
    date_from:  str,
    date_to:    str,
    ss:         gspread.Spreadsheet,
    sheet_name: str,
) -> None:
    log.info('load_funnel_period [%s]: %s → %s', sheet_name, date_from, date_to)
    set_status(ss, sheet_name, '🔄 Загружается...')

    url = f'{ANALYTICS_BASE}/api/analytics/v3/sales-funnel/products'
    all_products: list[dict] = []
    offset, limit, page = 0, 1000, 1

    while True:
        log.info('Page %d (offset %d)', page, offset)
        body = {
            'selectedPeriod': {'start': date_from, 'end': date_to},
            'nmIds': [], 'brandNames': [], 'subjectIds': [], 'tagIds': [],
            'skipDeletedNm': False, 'limit': limit, 'offset': offset,
        }
        resp = wb_request('post', url, api_key, json=body)
        if not resp:
            break
        products = resp.json().get('data', {}).get('products', [])
        if not products:
            break
        all_products.extend(products)
        log.info('Products loaded: %d', len(all_products))
        if len(products) < limit:
            break
        offset += limit
        page   += 1
        time.sleep(PAGE_SLEEP)

    if not all_products:
        set_status(ss, sheet_name, '❌ Нет данных')
        return

    period_label = (
        f"{datetime.strptime(date_from, '%Y-%m-%d').strftime('%d.%m.%Y')} — "
        f"{datetime.strptime(date_to,   '%Y-%m-%d').strftime('%d.%m.%Y')}"
    )
    headers = [
        'Артикул продавца', 'Артикул WB', 'Название', 'Предмет', 'Бренд',
        'Переходы в карточку', 'Переходы (пред.)',
        'В корзину, шт',      'В корзину (пред.)',
        'Конв. в корзину, %', 'Конв. в корзину (пред., %)',
        'В отложенные, шт',   'В отложенные (пред.)',
        'Заказали, шт',       'Заказали (пред.)',
        'Конв. в заказ, %',   'Конв. в заказ (пред., %)',
        'Выкупили, шт',       'Выкупили (пред.)',
        '% выкупа',           '% выкупа (пред.)',
        'Отменили, шт',       'Отменили (пред.)',
        'Заказали на сумму',  'Заказали сумма (пред.)',
        'Выкупили на сумму',  'Выкупили сумма (пред.)',
        'Средняя цена',       'Средняя цена (пред.)',
        'Остатки WB', 'Рейтинг товара', 'Рейтинг отзывов',
        'Время доставки, ч',  'Время доставки (пред.), ч',
        period_label,
    ]
    rows: list[list] = [headers]

    for item in all_products:
        prod = item.get('product', {})
        s    = item.get('statistic', {}).get('selected', {})
        p    = item.get('statistic', {}).get('past',     {})
        sc   = s.get('conversions', {})
        pc   = p.get('conversions', {})
        st   = s.get('timeToReady', {})
        pt   = p.get('timeToReady', {})
        stk  = prod.get('stocks',   {})

        rows.append([
            prod.get('vendorCode',     ''), prod.get('nmId',          ''),
            prod.get('title',          ''), prod.get('subjectName',   ''),
            prod.get('brandName',      ''),
            s.get('openCount',          0), p.get('openCount',         0),
            s.get('cartCount',          0), p.get('cartCount',         0),
            sc.get('addToCartPercent',  0), pc.get('addToCartPercent', 0),
            s.get('addToWishlist',      0), p.get('addToWishlist',     0),
            s.get('orderCount',         0), p.get('orderCount',        0),
            sc.get('cartToOrderPercent',0), pc.get('cartToOrderPercent',0),
            s.get('buyoutCount',        0), p.get('buyoutCount',       0),
            sc.get('buyoutPercent',     0), pc.get('buyoutPercent',    0),
            s.get('cancelCount',        0), p.get('cancelCount',       0),
            s.get('orderSum',           0), p.get('orderSum',          0),
            s.get('buyoutSum',          0), p.get('buyoutSum',         0),
            s.get('avgPrice',           0), p.get('avgPrice',          0),
            stk.get('wb',               0), prod.get('productRating',  0),
            prod.get('feedbackRating',  0),
            st.get('days', 0) * 24 + st.get('hours', 0),
            pt.get('days', 0) * 24 + pt.get('hours', 0),
            '',
        ])

    write_sheet(ss, sheet_name, rows)
    set_status(ss, sheet_name, f'✅ Готово — {len(all_products)} товаров')

# ── Точка входа ────────────────────────────────────────────────────────────────

def main() -> None:
    log.info('=== main2 started ===')
    ss      = get_spreadsheet()
    api_key = get_api_key(ss)

    today       = datetime.now()
    yesterday   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    week_from   = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    days14_from = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    month_from  = (today - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')

    week_ago = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    set_date_range(ss, week_ago, yesterday)

    campaign_ids, id_to_name = get_campaigns(api_key)
    if not campaign_ids:
        log.warning('Нет кампаний — РК периоды пропущены')
    else:
        log.info('Fetching month stats for all RK periods...')
        month_stats = fetch_fullstats(api_key, campaign_ids, month_from, yesterday)
        time.sleep(5)

        days14_to = (today - timedelta(days=8)).strftime('%Y-%m-%d')
        for sheet_name, df, dt in [
            ('РК День',    yesterday,   yesterday),
            ('РК Неделя',  week_from,   yesterday),
            ('РК 14 Дней', days14_from, days14_to),
            ('РК Месяц',   month_from,  yesterday),
        ]:
            write_rk_period(month_stats, id_to_name, df, dt, ss, sheet_name)
            time.sleep(5)

    time.sleep(10)

    for sheet_name, df, dt in [
        ('Воронка День',    yesterday,   yesterday),
        ('Воронка Неделя',  week_from,   yesterday),
        ('Воронка 14 Дней', days14_from, yesterday),
        ('Воронка Месяц',   month_from,  yesterday),
    ]:
        load_funnel_period(api_key, df, dt, ss, sheet_name)
        time.sleep(10)

    set_status(ss, 'Все данные', f'✅ Завершено — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    log.info('=== main2 complete ===')


if __name__ == '__main__':
    main()
