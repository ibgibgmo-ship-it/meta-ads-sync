"""
TikTok広告（複数アカウント）の日別・広告レベル実績を取得して
Googleスプレッドシートの月別タブ（tiktok_ads_YYYY-MM）に書き込むスクリプト。

取得粒度: 日付 × 広告アカウント × キャンペーン × 広告グループ × 広告（ランディングページURL）
指標:     impressions / 消化金額 / クリック / リーチ / コンバージョン

必要な環境変数:
  TIKTOK_ACCESS_TOKEN      Marketing API のアクセストークン（無期限）
  TIKTOK_ADVERTISER_IDS    "1234567890,2345678901"（広告アカウントID、カンマ区切り）
  GOOGLE_SA_JSON           Googleサービスアカウントの鍵JSON
  SHEET_ID                 スプレッドシートID
任意:
  SHEET_TAB                既定 "tiktok_ads"（月別に tiktok_ads_2026-11 のように作成）
  DAYS_BACK                既定 7

使い方:
  python tiktok_ads_to_sheets.py                       # 直近 DAYS_BACK 日（今日を含む）
  python tiktok_ads_to_sheets.py 2026-10-01 2026-11-05 # 期間指定
"""

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

BASE = "https://business-api.tiktok.com/open_api/v1.3"
TOKEN = os.environ["TIKTOK_ACCESS_TOKEN"]
ADVERTISERS = [a.strip() for a in os.environ["TIKTOK_ADVERTISER_IDS"].split(",") if a.strip()]
SHEET_ID = os.environ["SHEET_ID"]
SHEET_TAB = os.environ.get("SHEET_TAB", "tiktok_ads")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "7"))
URL_TAB = "tiktok_ad_urls"

HEADER = [
    "date", "account_id", "account_name",
    "campaign_id", "campaign_name",
    "adgroup_id", "adgroup_name",
    "ad_id", "ad_name", "landing_page_url",
    "impressions", "spend", "clicks", "reach", "conversions",
    "updated_at",
]


# ------------------------------------------------------------- TikTok API ---
def get(path, params, retries=8):
    for attempt in range(retries):
        r = requests.get(f"{BASE}{path}", params=params, headers={"Access-Token": TOKEN}, timeout=120)
        try:
            body = r.json()
        except Exception:
            body = {}
        code = body.get("code")
        if r.status_code == 200 and code == 0:
            return body.get("data") or {}
        msg = body.get("message") or r.text[:300]
        # 40100 = レート制限。50000番台 = サーバー側の一時エラー
        if code in (40100, 40110) or (isinstance(code, int) and code >= 50000) or r.status_code >= 500:
            wait = min(30 * (attempt + 1), 300)
            print(f"  rate limit / server error ({code}): {msg}. wait {wait}s", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"TikTok API error (code {code}): {msg}")
    raise RuntimeError("TikTok API: retry exhausted")


def advertiser_names(ids):
    data = get("/advertiser/info/", {"advertiser_ids": json.dumps(ids), "fields": json.dumps(["advertiser_id", "name"])})
    return {str(a["advertiser_id"]): a.get("name", "") for a in data.get("list", [])}


def fetch_report(advertiser_id, start, end):
    """広告レベル・日別のレポート（1回の期間は最大30日）"""
    rows = []
    page = 1
    while True:
        params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": json.dumps(["ad_id", "stat_time_day"]),
            "metrics": json.dumps([
                "campaign_id", "campaign_name", "adgroup_id", "adgroup_name", "ad_name",
                "impressions", "spend", "clicks", "reach", "conversion",
            ]),
            "start_date": start,
            "end_date": end,
            "page": page,
            "page_size": 1000,
        }
        data = get("/report/integrated/get/", params)
        rows.extend(data.get("list", []))
        info = data.get("page_info") or {}
        if page >= int(info.get("total_page", 1) or 1):
            return rows
        page += 1


def fetch_ad_urls(advertiser_id, ad_ids, cache):
    """キャッシュに無い広告のランディングページURLを取得（100件ずつ）"""
    missing = [a for a in ad_ids if a and not cache.get(a)]
    for i in range(0, len(missing), 100):
        chunk = missing[i:i + 100]
        data = get("/ad/get/", {
            "advertiser_id": advertiser_id,
            "filtering": json.dumps({"ad_ids": chunk}),
            "fields": json.dumps(["ad_id", "landing_page_url", "ad_format", "ad_name"]),
            "page_size": 100,
        })
        for ad in data.get("list", []):
            url = ad.get("landing_page_url") or ""
            if url:
                cache[str(ad["ad_id"])] = url
    return len(missing)


def daterange_chunks(start, end, days=30):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)


# ------------------------------------------------------------- Google Sheets ---
def open_spreadsheet():
    info = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(SHEET_ID)


def open_tab(sh, name, header):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(name, rows=1000, cols=len(header))
        ws.append_row(header)
        return ws


def write_in_chunks(ws, rows, chunk=4000):
    need = len(rows) + 10
    if ws.row_count < need:
        ws.add_rows(need - ws.row_count)
    for i in range(0, len(rows), chunk):
        ws.update(range_name=f"A{i + 1}", values=rows[i:i + chunk], value_input_option="RAW")
        time.sleep(1)


def load_url_cache(sh):
    ws = open_tab(sh, URL_TAB, ["ad_id", "landing_page_url"])
    cache = {}
    for row in ws.get_all_values()[1:]:
        if row and row[0] and len(row) > 1 and row[1]:
            cache[row[0]] = row[1]
    return ws, cache


def save_url_cache(ws, cache):
    ws.clear()
    write_in_chunks(ws, [["ad_id", "landing_page_url"]] + [[k, v] for k, v in sorted(cache.items()) if v])


def months_between(start, end):
    cur = date(start.year, start.month, 1)
    while cur <= end:
        yield cur.strftime("%Y-%m")
        cur = date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)


def upsert(ws, new_rows, start, end, accounts):
    existing = ws.get_all_values()
    body = existing[1:] if existing else []
    acc = set(accounts)
    keep = [r for r in body if not (start <= str(r[0]) <= end and str(r[1]) in acc)]
    all_rows = keep + new_rows
    all_rows.sort(key=lambda r: (str(r[0]), str(r[1]), str(r[4]), str(r[6]), str(r[8])))
    ws.clear()
    write_in_chunks(ws, [HEADER] + all_rows)
    ws.freeze(rows=1)
    print(f"sheet updated: kept {len(keep)} rows, added {len(new_rows)} rows", flush=True)


# ------------------------------------------------------------------- main ---
def main():
    today_jst = (datetime.now(timezone.utc) + timedelta(hours=9)).date()
    if len(sys.argv) >= 3:
        start, end = date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
    else:
        end = today_jst
        start = end - timedelta(days=DAYS_BACK - 1)
    print(f"range: {start} .. {end}, accounts: {len(ADVERTISERS)}", flush=True)

    names = advertiser_names(ADVERTISERS)
    sh = open_spreadsheet()
    url_ws, url_cache = load_url_cache(sh)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    rows, done, failed = [], [], []
    for adv in ADVERTISERS:
        try:
            acc_rows = []
            for s_, e_ in daterange_chunks(start, end):
                print(f"[{adv}] report {s_}..{e_}", flush=True)
                acc_rows.extend(fetch_report(adv, s_, e_))
            ad_ids = sorted({str(r["dimensions"]["ad_id"]) for r in acc_rows})
            n = fetch_ad_urls(adv, ad_ids, url_cache)
            for r in acc_rows:
                d, m = r.get("dimensions", {}), r.get("metrics", {})
                imp, spend = int(float(m.get("impressions", 0) or 0)), float(m.get("spend", 0) or 0)
                clicks, conv = int(float(m.get("clicks", 0) or 0)), float(m.get("conversion", 0) or 0)
                if imp == 0 and spend == 0 and clicks == 0 and conv == 0:
                    continue
                ad_id = str(d.get("ad_id", ""))
                rows.append([
                    str(d.get("stat_time_day", ""))[:10], adv, names.get(adv, ""),
                    str(m.get("campaign_id", "")), m.get("campaign_name", ""),
                    str(m.get("adgroup_id", "")), m.get("adgroup_name", ""),
                    ad_id, m.get("ad_name", ""), url_cache.get(ad_id, ""),
                    imp, round(spend, 2), clicks, int(float(m.get("reach", 0) or 0)), conv,
                    now,
                ])
            print(f"[{adv}] done: {len(acc_rows)} rows, {n} new URLs", flush=True)
            done.append(adv)
        except Exception as e:
            print(f"[{adv}] FAILED: {e}", flush=True)
            failed.append(adv)

    if url_cache:
        save_url_cache(url_ws, url_cache)
    print(f"fetched {len(rows)} rows from {len(done)} accounts", flush=True)
    if done:
        for month in months_between(start, end):
            m_start, m_end = max(start.isoformat(), f"{month}-01"), min(end.isoformat(), f"{month}-31")
            ws = open_tab(sh, f"{SHEET_TAB}_{month}", HEADER)
            print(f"[{SHEET_TAB}_{month}] ", end="", flush=True)
            upsert(ws, [r for r in rows if str(r[0])[:7] == month], m_start, m_end, done)
    if failed:
        raise SystemExit(f"failed accounts (次回実行で取り直されます): {failed}")


if __name__ == "__main__":
    main()
