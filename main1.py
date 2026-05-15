function wbRequest_(apiKey, url, opts, attempt) {
  attempt = attempt || 1;
  const options = {
    ...opts,
    headers:            { Authorization: apiKey },
    muteHttpExceptions: true,
  };
  try {
    const resp = UrlFetchApp.fetch(url, options);
    const code = resp.getResponseCode();
    if (code === 200) return resp;
    if (code === 429 && attempt <= 3) {
      // Фиксированные 65с — WB обновляет лимит каждую минуту.
      // Максимум 3 попытки × 65с = 195с < 6-мин лимита Apps Script.
      Logger.log(`429 — ждём 65с (попытка ${attempt}/3)`);
      Utilities.sleep(65000);
      return wbRequest_(apiKey, url, opts, attempt + 1);
    }
    Logger.log(`❌ HTTP ${code}: ${resp.getContentText().slice(0, 300)}`);
    return null;
  } catch (e) {
    Logger.log(`❌ ${e.message}`);
    return null;
  }
}
