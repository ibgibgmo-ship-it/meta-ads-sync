"""
Meta広告（複数アカウント）の日別・広告レベル実績を取得して
Googleスプレッドシートに書き込むスクリプト。

取得粒度: 日付 × アカウント × キャンペーン × 広告セット × 広告（設定URL）
指標:     impressions / spend / clicks / reach / results

必要な環境変数:
  META_ACCESS_TOKEN        システムユーザーのアクセストークン
  META_AD_ACCOUNT_IDS      "act_111,act_222,..."（カンマ区切り、act_付き）
  GOOGLE_SA_JSON           Googleサービスアカウントの鍵JSON（文字列そのまま）
  SHEET_ID                 書き込み先スプレッドシートのID（URLの /d/ と /edit の間）
任意:
  META_API_VERSION         既定 v25.0
  SHEET_TAB                既定 "data"
  DAYS_BACK                毎日実行時に遡って取り直す日数（既定 3。Metaは数日間数値が変動するため）
  RESULT_ACTION_OVERRIDES  "campaign_id:action_type,..." で結果の定義を上書き

使い方:
  python meta_ads_to_sheets.py                        # 直近 DAYS_BACK 日を取り直し
  python meta_ads_to_sheets.py 2026-09-01 2026-10-17  # 期間を指定して過去分を一括取得
"""

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

API_VERSION = os.environ.get("META_API_VERSION", "v25.0")
BASE = f"https://graph.facebook.com/{API_VERSION}"
TOKEN = os.environ["META_ACCESS_TOKEN"]
ACCOUNT_IDS = [a.strip() for a in os.environ.get("META_AD_ACCOUNT_IDS", "").split(",") if a.strip()]
# アカウント名の一部で対象を絞る（例: "1day小顔,A式切開リフト"）。指定時は META_AD_ACCOUNT_IDS より優先
ACCOUNT_NAMES = [a.strip() for a in os.environ.get("META_ACCOUNT_NAMES", "").split(",") if a.strip()]
# 固定の開始日（例: 2026-10-01）。指定時は毎回この日から「今日」までを取り直す
START_DATE_FIXED = os.environ.get("META_START_DATE", "").strip()
SHEET_ID = os.environ["SHEET_ID"]
SHEET_TAB = os.environ.get("SHEET_TAB", "data")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "3"))

HEADER = [
    "date", "account_id", "account_name",
    "campaign_id", "campaign_name",
    "adset_id", "adset_name",
    "ad_id", "ad_name", "link_url",
    "impressions", "spend", "link_clicks", "reach",
    "results", "result_type", "objective", "optimization_goal",
    "updated_at",
]

# 最適化目標 → 「結果」として数えるアクションタイプ
OPT_GOAL_TO_ACTION = {
    "LEAD_GENERATION": "lead",
    "QUALITY_LEAD": "lead",
    "OFFSITE_CONVERSIONS": "offsite_conversion.fb_pixel_purchase",
    "VALUE": "offsite_conversion.fb_pixel_purchase",
    "LINK_CLICKS": "link_click",
    "LANDING_PAGE_VIEWS": "landing_page_view",
    "POST_ENGAGEMENT": "post_engagement",
    "PAGE_LIKES": "like",
    "APP_INSTALLS": "mobile_app_install",
    "THRUPLAY": "video_thruplay_watched_actions",
    "CONVERSATIONS": "onsite_conversion.messaging_conversation_started_7d",
    "REPLIES": "onsite_conversion.messaging_conversation_started_7d",
}
# キャンペーン目的 → フォールバック
OBJECTIVE_TO_ACTION = {
    "OUTCOME_LEADS": "lead",
    "OUTCOME_SALES": "offsite_conversion.fb_pixel_purchase",
    "OUTCOME_TRAFFIC": "link_click",
    "OUTCOME_ENGAGEMENT": "post_engagement",
    "OUTCOME_APP_PROMOTION": "mobile_app_install",
}

OVERRIDES = {}
for pair in os.environ.get("RESULT_ACTION_OVERRIDES", "").split(","):
    if ":" in pair:
        cid, atype = pair.split(":", 1)
        OVERRIDES[cid.strip()] = atype.strip()


# ---------------------------------------------------------------- Meta API ---
class TooMuchData(Exception):
    """Metaが「要求データ量が多すぎる」(code 1 / 2) を返したとき"""


def get(url, params, retries=6):
    params = {**params, "access_token": TOKEN}
    for attempt in range(retries):
        r = requests.get(url, params=params, timeout=180)
        if r.status_code == 200:
            return r.json()
        try:
            err = r.json().get("error", {})
        except Exception:
            err = {}
        code = err.get("code")
        msg = (err.get("message") or r.text[:200]).replace("\n", " ")
        # 1 / 2 = データ量過多・一時的に利用不可 → 呼び出し側で件数を減らして再試行
        if code in (1, 2):
            print(f"  too much data ({code}): {msg}", flush=True)
            raise TooMuchData(msg)
        # 4 / 17 / 32 / 613 / 80004 = レート制限
        if code in (4, 17, 32, 613, 80004) or r.status_code >= 500:
            wait = min(60 * (attempt + 1), 300)
            print(f"  rate limit / server error ({code}): {msg}. wait {wait}s", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"Meta API error {r.status_code} (code {code}): {msg}")
    raise RuntimeError("Meta API: retry exhausted")


def paginate(url, params, limit=100, min_limit=10):
    """ページ送りで全件取得。データ量過多エラーが出たら limit を半分にしてやり直す"""
    while True:
        try:
            results = []
            u, p = url, {**params, "limit": limit}
            while True:
                data = get(u, p)
                results.extend(data.get("data", []))
                nxt = data.get("paging", {}).get("next")
                if not nxt:
                    return results
                u, p = nxt, {}
        except TooMuchData:
            if limit <= min_limit:
                raise RuntimeError("Meta API: data too large even at minimum page size")
            limit = max(min_limit, limit // 2)
            print(f"  retry with limit={limit}", flush=True)
            time.sleep(5)


def resolve_accounts():
    """対象アカウントの act_ID リストを決める"""
    if not ACCOUNT_NAMES:
        if not ACCOUNT_IDS:
            raise RuntimeError("META_AD_ACCOUNT_IDS か META_ACCOUNT_NAMES のどちらかを設定してください")
        return ACCOUNT_IDS
    found = []
    for acc in paginate(f"{BASE}/me/adaccounts", {"fields": "id,name,account_status"}, limit=100):
        name = acc.get("name") or ""
        if any(n in name for n in ACCOUNT_NAMES):
            found.append(acc["id"])
            print(f"target: {acc['id']} {name}", flush=True)
    if not found:
        raise RuntimeError(f"名前に一致する広告アカウントが見つかりません: {ACCOUNT_NAMES}")
    return found


def fetch_ad_urls(account):
    """広告ID → 設定URL（リンク先）を取得"""
    fields = "id,creative{link_url,object_story_spec,asset_feed_spec}"
    urls = {}
    for ad in paginate(f"{BASE}/{account}/ads", {"fields": fields}, limit=100):
        urls[ad["id"]] = extract_link(ad.get("creative") or {})
    return urls


def extract_link(creative):
    if creative.get("link_url"):
        return creative["link_url"]
    oss = creative.get("object_story_spec") or {}
    ld = oss.get("link_data") or {}
    if ld.get("link"):
        return ld["link"]
    for child in ld.get("child_attachments") or []:
        if child.get("link"):
            return child["link"]
    vd = oss.get("video_data") or {}
    cta_link = ((vd.get("call_to_action") or {}).get("value") or {}).get("link")
    if cta_link:
        return cta_link
    afs = creative.get("asset_feed_spec") or {}
    for lu in afs.get("link_urls") or []:
        if lu.get("website_url"):
            return lu["website_url"]
    return ""


def pick_result(row):
    """actions配列から「結果」を1つ選ぶ"""
    actions = {a["action_type"]: float(a.get("value", 0)) for a in row.get("actions") or []}
    atype = (
        OVERRIDES.get(row.get("campaign_id"))
        or OPT_GOAL_TO_ACTION.get(row.get("optimization_goal", ""))
        or OBJECTIVE_TO_ACTION.get(row.get("objective", ""))
        or "link_click"
    )
    # 購入系はpixel/offsite_conversionの名寄せ
    if atype == "offsite_conversion.fb_pixel_purchase":
        val = actions.get(atype) or actions.get("purchase") or actions.get("omni_purchase") or 0
    else:
        val = actions.get(atype, 0)
    return int(val), atype


def fetch_insights(account, start, end):
    fields = ",".join([
        "account_id", "account_name",
        "campaign_id", "campaign_name",
        "adset_id", "adset_name",
        "ad_id", "ad_name",
        "impressions", "spend", "inline_link_clicks", "reach",
        "actions", "objective", "optimization_goal",
    ])
    params = {
        "level": "ad",
        "time_increment": 1,
        "time_range": json.dumps({"since": start, "until": end}),
        "fields": fields,
    }
    return paginate(f"{BASE}/{account}/insights", params, limit=100)


def daterange_chunks(start, end, days=7):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)


# ------------------------------------------------------------- Google Sheets ---
def open_sheet():
    info = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(SHEET_TAB, rows=1000, cols=len(HEADER))
        ws.append_row(HEADER)
    return ws


def upsert(ws, new_rows, start, end, accounts):
    """対象期間・対象アカウントの既存行を削除して新しい行に差し替える"""
    existing = ws.get_all_values()
    body = existing[1:] if existing else []
    acc_ids = {a.replace("act_", "") for a in accounts}
    keep = [
        r for r in body
        if not (start <= r[0] <= end and r[1] in acc_ids)
    ]
    all_rows = keep + new_rows
    all_rows.sort(key=lambda r: (r[0], r[1], r[4], r[6], r[8]))
    ws.clear()
    ws.update(range_name="A1", values=[HEADER] + all_rows, value_input_option="RAW")
    ws.freeze(rows=1)
    print(f"sheet updated: kept {len(keep)} rows, added {len(new_rows)} rows", flush=True)


# ------------------------------------------------------------------- main ---
def main():
    # 日付は日本時間基準
    today_jst = (datetime.now(timezone.utc) + timedelta(hours=9)).date()
    if len(sys.argv) >= 3:
        start = date.fromisoformat(sys.argv[1])
        end = date.fromisoformat(sys.argv[2])
    elif START_DATE_FIXED:
        start = date.fromisoformat(START_DATE_FIXED)
        end = today_jst
    else:
        end = today_jst
        start = end - timedelta(days=DAYS_BACK - 1)
    accounts = resolve_accounts()
    print(f"range: {start} .. {end}, accounts: {len(accounts)}", flush=True)

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for acc in accounts:
        print(f"[{acc}] fetching ad URLs", flush=True)
        urls = fetch_ad_urls(acc)
        for s, e in daterange_chunks(start, end):
            print(f"[{acc}] insights {s}..{e}", flush=True)
            for r in fetch_insights(acc, s, e):
                results, rtype = pick_result(r)
                rows.append([
                    r["date_start"], r["account_id"], r.get("account_name", ""),
                    r.get("campaign_id", ""), r.get("campaign_name", ""),
                    r.get("adset_id", ""), r.get("adset_name", ""),
                    r.get("ad_id", ""), r.get("ad_name", ""), urls.get(r.get("ad_id", ""), ""),
                    int(r.get("impressions", 0)), float(r.get("spend", 0)),
                    int(r.get("inline_link_clicks", 0)), int(r.get("reach", 0)),
                    results, rtype, r.get("objective", ""), r.get("optimization_goal", ""),
                    now,
                ])
    print(f"fetched {len(rows)} rows", flush=True)

    ws = open_sheet()
    upsert(ws, rows, start.isoformat(), end.isoformat(), accounts)


if __name__ == "__main__":
    main()
