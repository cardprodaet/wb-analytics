#!/usr/bin/env python3
"""Выгрузка за произвольный период.

Даты берутся из листа «Настройки»: B10 — ОТ, B11 — ДО.
Пишет в листы «Воронка Период» и «РК Период» — ночной прогон их не трогает.
"""

import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import main2
from gspread.exceptions import APIError, WorksheetNotFound

FUNNEL_SHEET = 'Воронка Период'
RK_SHEET     = 'РК Период'
CACHE_FILE   = 'period_cache.json'
ADVERTS_URL  = 'https://advert-api.wildberries.ru/api/advert/v2/adverts?limit=1000'

log = main2.log


def read_dates(sheet):
    raw = [(sheet.acell(c).value or '').strip() for c in ('B10', 'B11')]
    if not all(raw):
        return None, None
    out = []
    for r in raw:
        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
            try:
                out.append(datetime.strptime(r, fmt))
                break
            except ValueError:
                continue
        else:
            log.error('Непонятная дата: %s — пиши как 2026-09-01', r)
            sys.exit(1)
    return out


def campaign_names(api_key):
    try:
        r = requests.get(ADVERTS_URL, headers={'Authorization': api_key}, timeout=60)
        if r.status_code != 200:
            log.warning('Справочник кампаний: HTTP %s', r.status_code)
            return {}
        names = {int(a['id']): (a.get('settings') or {}).get('name', '')
                 for a in r.json().get('adverts', [])}
        log.info('Справочник кампаний: %d', len(names))
        return names
    except Exception as e:
        log.warning('Справочник кампаний недоступен: %s', e)
        return {}


def ensure_sheet(ss, name):
    try:
        return ss.worksheet(name)
    except WorksheetNotFound:
        log.info('Создаю лист «%s»', name)
        return ss.add_worksheet(title=name, rows=2000, cols=60)


def main():
    ss = main2.get_spreadsheet()
    settings = ss.worksheet('Настройки')

    if not (settings.acell('A10').value or '').strip():
        settings.update(values=[['Период ОТ:'], ['Период ДО:']], range_name='A10:A11')
        log.info('В «Настройки» добавлены подписи в A10 и A11')

    dt_from, dt_to = read_dates(settings)
    if not dt_from:
        print('\n  Период не задан. Открой лист «Настройки» и впиши:')
        print('      B10 — дата ОТ   например 2026-09-01')
        print('      B11 — дата ДО   например 2026-09-30')
        print('  Потом запусти скрипт снова.\n')
        return

    if dt_to < dt_from:
        log.error('Дата ДО раньше даты ОТ')
        return

    date_from = dt_from.strftime('%Y-%m-%d')
    date_to   = dt_to.strftime('%Y-%m-%d')
    days      = (dt_to - dt_from).days + 1
    api_key   = main2.get_api_key(ss)

    log.info('=== Период %s → %s (%d дн.) ===', date_from, date_to, days)
    ensure_sheet(ss, FUNNEL_SHEET)
    ensure_sheet(ss, RK_SHEET)

    log.info('--- Воронка ---')
    main2.load_funnel_period(api_key, date_from, date_to, ss, FUNNEL_SHEET)

    if days > 31:
        log.warning('РК пропускаю: WB отдаёт максимум 31 день, у тебя %d', days)
        main2.set_status(ss, RK_SHEET, f'⚠️ {days} дн. — WB отдаёт максимум 31')
        return

    log.info('--- РК ---')
    key_now = f'{date_from}_{date_to}'
    stats = None

    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            blob = json.load(f)
        if blob.get('key') == key_now:
            stats = blob['stats']
            log.info('Беру РК из кэша: %d записей', len(stats))

    if stats is None:
        ids, _ = main2.get_campaigns(api_key)
        stats = main2.fetch_fullstats(api_key, ids, date_from, date_to)
        with open(CACHE_FILE, 'w') as f:
            json.dump({'key': key_now, 'stats': stats}, f)
        log.info('Скачал и сохранил: %d записей', len(stats))

    names = campaign_names(api_key)

    for attempt in range(1, 11):
        try:
            main2.write_rk_period(stats, names, date_from, date_to, ss, RK_SHEET)
            log.info('ГОТОВО')
            return
        except APIError as e:
            log.warning('Google квота (%d/10): %s — ждём 70 сек', attempt, e)
            time.sleep(70)
    log.error('Записать РК не удалось — запусти скрипт ещё раз')


if __name__ == '__main__':
    main()
