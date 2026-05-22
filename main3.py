#!/usr/bin/env python3
"""
WB Analytics — main3.py
Воронка по периодам: День, Неделя, 14 Дней, Месяц.
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

ANALYTICS_BASE = 'https://seller-analytics-api.wildberries.ru'
PAGE_SLEEP     = 20   # между страницами (было 60 — избыточно)
WRITE_BATCH    = 500

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
    """Записывает rows в лист. Пробрасывает исключение при ошибке."""
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

# ── Загрузчик ──────────────────────────────────────────────────────────────────

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
    log.info('=== main3 started ===')
    ss      = get_spreadsheet()
    api_key = get_api_key(ss)

    today       = datetime.now()
    yesterday   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    week_from   = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    days14_from = (today - timedelta(days=14)).strftime('%Y-%m-%d')
    month_from  = today.replace(day=1).strftime('%Y-%m-%d')

    for sheet_name, df, dt in [
        ('Воронка День',    yesterday,   yesterday),
        ('Воронка Неделя',  week_from,   yesterday),
        ('Воронка 14 Дней', days14_from, yesterday),
        ('Воронка Месяц',   month_from,  yesterday),
    ]:
        load_funnel_period(api_key, df, dt, ss, sheet_name)
        time.sleep(10)

    set_status(ss, 'Все данные', f'✅ Завершено — {datetime.now().strftime("%d.%m.%Y %H:%M")}')
    log.info('=== main3 complete ===')


if __name__ == '__main__':
    main()
