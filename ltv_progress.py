r"""
生涯顧客LTV進捗管理システム v4.0
==============================
オートウェーブ 顧客育成トラッカー

【フォルダ構成】
 C:\SATO\LTV\
   ltv_progress.py         ← このプログラム
   data\
     Master_Data.csv       ← 顧客ID, 車両ID, 登録番号, 車検満了日, 入庫店舗ID
     車検.csv              ← 車両ID, 前回取引日
     タイヤ交換.csv
     オイル交換.csv
     12か月点検.csv
     バッテリー交換.csv
     コーティング.csv
     自動車販売.csv        ← 登録番号, 前回取引日（キーが登録番号）
     Reservation.csv       ← 登録番号, サービス種別, 予約ステータス, 予約日

【動作モード】
 ① 自動読込モード：dataフォルダにCSVを置くだけで起動時に自動読込
 ② 手動アップロード：サイドバーから直接アップロードも可能（上書き優先）
 ③ ハイブリッド：マスターはdataフォルダ＋予約だけ毎日アップロード等も可能
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO
import os
import uuid
import json
from pathlib import Path

# ──────────────────────────────────────────────
st.set_page_config(
    page_title="生涯顧客LTV進捗管理 | オートウェーブ",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# パスワード認証
# ──────────────────────────────────────────────
def check_password():
    try:
        correct_password = st.secrets["password"]
    except Exception:
        return True  # secrets未設定（ローカル環境）はスルー

    if st.session_state.get("authenticated"):
        return True

    st.markdown("""
    <div style="
        max-width:400px; margin:80px auto; padding:2rem;
        background:#fff; border-radius:12px;
        box-shadow:0 4px 20px rgba(0,0,0,0.1);
        text-align:center;
    ">
        <div style="font-size:3rem;">📈</div>
        <h2 style="color:#1a237e; margin:0.5rem 0;">生涯顧客LTV進捗管理</h2>
        <p style="color:#666; font-size:0.9rem;">株式会社オートウェーブ</p>
    </div>
    """, unsafe_allow_html=True)

    with st.form("login_form"):
        st.markdown("<div style='max-width:400px;margin:0 auto;'>", unsafe_allow_html=True)
        password = st.text_input("パスワード", type="password", placeholder="パスワードを入力してください")
        submitted = st.form_submit_button("ログイン", use_container_width=True, type="primary")
        st.markdown("</div>", unsafe_allow_html=True)

        if submitted:
            if password == correct_password:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("パスワードが違います。")

    return False

if not check_password():
    st.stop()

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700;900&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans JP', 'Meiryo', sans-serif; }
.main-header {
    background: linear-gradient(135deg, #0a1628 0%, #1a2744 100%);
    color: white; padding: 1.4rem 2rem; border-radius: 12px;
    margin-bottom: 1.2rem; border: 1px solid rgba(255,255,255,0.06);
}
.main-header h1 { margin:0; font-size:1.55rem; font-weight:900; }
.main-header p  { margin:0.3rem 0 0; font-size:0.8rem; opacity:0.6; }
.kpi-card {
    background:#fff; border-radius:10px; padding:1rem 1.1rem;
    box-shadow:0 2px 10px rgba(0,0,0,0.07); text-align:center;
    border-top:4px solid #1a56db; height:100%;
}
.kpi-card.gold   { border-top-color:#d97706; }
.kpi-card.green  { border-top-color:#059669; }
.kpi-card.red    { border-top-color:#dc2626; }
.kpi-card.purple { border-top-color:#7c3aed; }
.kpi-card.gray   { border-top-color:#64748b; }
.kpi-card .lbl { font-size:0.72rem; color:#888; margin-bottom:0.3rem; font-weight:600; }
.kpi-card .val { font-size:1.8rem; font-weight:900; color:#0a1628; line-height:1.1; }
.kpi-card .sub { font-size:0.7rem; color:#aaa; margin-top:0.25rem; }
.sec { font-size:1rem; font-weight:800; color:#0a1628;
       border-left:4px solid #1a56db; padding-left:0.65rem;
       margin:1.5rem 0 0.9rem; }
.story-panel {
    background:linear-gradient(135deg,#0a1628,#1a2744);
    color:white; border-radius:12px; padding:1.4rem 1.6rem; margin:0.8rem 0;
}
.story-panel h3 { color:#fbbf24; margin:0 0 0.7rem; font-size:0.95rem; }
.story-panel p  { margin:0.3rem 0; font-size:0.83rem; line-height:1.75; opacity:0.9; }
.upload-status { padding:0.38rem 0.75rem; border-radius:6px; font-size:0.8rem; margin:0.2rem 0; }
.upload-ok   { background:#ecfdf5; color:#065f46; }
.upload-wait { background:#fffbeb; color:#92400e; }
.sec-banner {
    background:#ecfdf5; border:1px solid #6ee7b7; border-radius:8px;
    padding:0.45rem 1rem; font-size:0.77rem; color:#065f46;
    text-align:center; margin-bottom:1rem;
}
div[data-testid="stMetricValue"] { font-size:1.4rem !important; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>📈 生涯顧客LTV進捗管理システム</h1>
    <p>マスターデータ × 取引履歴 × 予約データを突合 ─ 生涯顧客の育成進捗を4軸でリアルタイム可視化</p>
</div>
<div class="sec-banner">🔒 データはセッション中のみ処理されます。ブラウザを閉じると自動消去されます。</div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# 定数
# ──────────────────────────────────────────────
DEFAULT_SERVICES = ["車検", "タイヤ交換", "オイル交換", "12ヶ月点検", "バッテリー交換", "コーティング", "自動車販売", "ワイパー交換", "保険"]

# ファイル名→サービス名のマッピング（実際のファイル名が異なる場合）
FILE_TO_SERVICE = {
    "12か月点検"        : "12ヶ月点検",
    "ボディーコート（silver）": "コーティング",
}

# 売上単価
SERVICE_PRICE = {
    "車検"      : 50000,
    "タイヤ交換" : 60000,
    "オイル交換" : 5000,
    "12ヶ月点検" : 10000,
    "バッテリー交換": 20000,
    "コーティング"  : 11000,
    "自動車販売" : 2100000,
    "ワイパー交換": 3500,
    "保険"      : 60000,
}

# 粗利単価（LTV計算に使用）
SERVICE_GROSS = {
    "車検"      : 30000,
    "タイヤ交換" : 18000,
    "オイル交換" : 3500,
    "12ヶ月点検" : 10000,
    "バッテリー交換": 8000,
    "コーティング"  : 10000,
    "自動車販売" : 250000,
    "ワイパー交換": 2000,
    "保険"      : 12000,
}


def to_csv(df):
    buf = BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    return buf.getvalue()


def read_csv(f, name=""):
    for enc in ["utf-8-sig", "utf-8", "cp932", "shift_jis", "latin1"]:
        try:
            f.seek(0)
            return pd.read_csv(f, encoding=enc)
        except Exception:
            continue
    raise ValueError(f"{name} を読み込めませんでした")


def norm(df):
    df.columns = df.columns.str.strip().str.replace("\u3000", "")
    return df


def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def assign_rank(n, thr):
    """旧ロジック互換（サンプルデータ生成等で使用）"""
    for r, t in thr.items():
        if n >= t:
            return r
    return "D"

def normalize_plate(plate: str) -> str:
    """
    登録番号を正規化する（全角→半角・スペース除去・大文字統一）
    例：「千葉　４８０　て　９５７９」→「千葉480て9579」
    """
    import unicodedata
    if not plate or str(plate) in ["nan", "None", ""]:
        return ""
    s = str(plate).strip()
    # 全角→半角変換（数字・英字）
    s = unicodedata.normalize("NFKC", s)
    # スペース（全角・半角）除去
    s = s.replace(" ", "").replace("　", "").replace("　", "")
    return s

def assign_rank_customer(cust_row):
    """
    顧客ID起点のランク定義（確定版）
    ─────────────────────────────────
    SSS：複数台で車検取引あり かつ 自動車販売あり
    S  ：車検あり かつ 自動車販売あり（1台）
    A  ：車検あり かつ オイル交換あり かつ タイヤ交換あり
    B  ：車検あり（A未満）
    C  ：車検なし かつ メンテ来店あり（オイル/タイヤ/バッテリー/コーティング/ワイパー）
    D  ：ほぼ未取引
    """
    shaken    = cust_row.get("cust_車検", 0) >= 1
    sales     = cust_row.get("cust_自動車販売", 0) >= 1
    multi_shaken = cust_row.get("cust_車検台数", 0) >= 2
    oil       = cust_row.get("cust_オイル交換", 0) >= 1
    tire      = cust_row.get("cust_タイヤ交換", 0) >= 1
    maint     = (cust_row.get("cust_オイル交換", 0) +
                 cust_row.get("cust_タイヤ交換", 0) +
                 cust_row.get("cust_バッテリー交換", 0) +
                 cust_row.get("cust_コーティング", 0) +
                 cust_row.get("cust_ワイパー交換", 0)) >= 1

    if multi_shaken and sales: return "SSS"
    if shaken and sales:       return "S"
    if shaken and oil and tire: return "A"
    if shaken:                 return "B"
    if maint:                  return "C"
    return "D"

RANK_LABEL = {
    "SSS": "🏆SSS：複数台完全囲い込み",
    "S"  : "🥇S：車検＋乗り換え獲得",
    "A"  : "🥈A：車検＋来店習慣定着",
    "B"  : "🥉B：車検定着",
    "C"  : "C：車検未獲得・メンテ来店",
    "D"  : "D：ほぼ未取引",
}
RANK_ORDER = ["SSS", "S", "A", "B", "C", "D"]

# ──────────────────────────────────────────────
# 営業活動ログ：Storage API ユーティリティ
# ──────────────────────────────────────────────
# フェーズ定義
SHAKEN_PHASES = {
    "0": "─ 未記録",
    "1": "提案できなかった",
    "2": "提案した",
    "3": "興味あり ⭐",
    "4": "仮予約 📅",
    "5": "本予約 ✅",
}
SERVICE_PHASES = {
    "0": "─ 未記録",
    "1": "提案できなかった",
    "2": "提案した",
    "3": "興味あり ⭐",
    "4": "売れた ✅",
}
PHASE_WEIGHT = {"0":0,"1":1,"2":2,"3":3,"4":4,"5":5}

@st.cache_data(ttl=300, show_spinner=False)
def load_staff_from_gdrive(file_id: str) -> list:
    """GoogleドライブからCSVで担当者マスターを読み込む"""
    try:
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        df_s = pd.read_csv(url, encoding="utf-8-sig", header=None)
        return df_s.iloc[:, 0].dropna().astype(str).str.strip().tolist()
    except Exception:
        return []

def load_staff_master() -> list:
    """担当者マスターを読み込む（Googleドライブ優先 → Storage API → デフォルト）"""
    # セッションキャッシュ
    cached = st.session_state.get("_staff_cache")
    if cached is not None:
        return cached

    # ① Googleドライブから読み込み（ファイルIDが設定されている場合）
    if GDRIVE_STAFF_ID:
        staff = load_staff_from_gdrive(GDRIVE_STAFF_ID)
        if staff:
            st.session_state["_staff_cache"] = staff
            return staff

    # ② セッション内の追加分
    local = st.session_state.get("_staff_master_local")
    if local:
        st.session_state["_staff_cache"] = local
        return local

    # ③ デフォルト
    return ["佐藤", "田中", "鈴木", "高橋", "伊藤"]

def save_staff_master(staff_list: list):
    """担当者マスターをセッションに保存（Notion APIトークン不要）"""
    st.session_state["_staff_master_local"] = staff_list
    st.session_state["_staff_cache"] = staff_list

# ──────────────────────────────────────────────
# Notion APIによる活動ログ管理
# ──────────────────────────────────────────────
# NotionデータベースID（営業活動ログ）
NOTION_DB_ID = "4483fbb4-098a-485d-a329-206386c4c588"
NOTION_API_URL = "https://api.anthropic.com/v1/messages"  # Claude経由は不使用

def _notion_headers() -> dict:
    """Notion APIヘッダーを生成（SecretsからTokenを取得）"""
    try:
        token = st.secrets.get("NOTION_TOKEN", "")
    except Exception:
        token = ""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

@st.cache_data(ttl=60, show_spinner=False)
def load_activity_logs() -> list:
    """NotionデータベースからログをすべてAPIで取得"""
    import requests
    headers = _notion_headers()
    if not headers["Authorization"].replace("Bearer ", ""):
        return []
    try:
        logs = []
        has_more = True
        cursor = None
        while has_more:
            body = {"page_size": 100, "sorts": [{"property": "記録日", "direction": "descending"}]}
            if cursor:
                body["start_cursor"] = cursor
            resp = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
                headers=headers, json=body, timeout=10
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            for page in data.get("results", []):
                props = page.get("properties", {})
                def _txt(p): return (props.get(p,{}).get("rich_text",[{}]) or [{}])[0].get("plain_text","") if props.get(p,{}).get("rich_text") else ""
                def _sel(p): return (props.get(p,{}).get("select") or {}).get("name","")
                def _date(p): return (props.get(p,{}).get("date") or {}).get("start","")
                def _title(p): return (props.get(p,{}).get("title",[{}]) or [{}])[0].get("plain_text","")
                logs.append({
                    "log_id"   : _txt("log_id"),
                    "記録日"   : _date("記録日"),
                    "顧客ID"   : _txt("顧客ID"),
                    "車両ID"   : _txt("車両ID"),
                    "登録番号" : _txt("登録番号"),
                    "担当者"   : _txt("担当者"),
                    "店舗"     : _txt("店舗"),
                    "ランク"   : _sel("ランク"),
                    "提案項目" : _sel("提案項目"),
                    "フェーズ" : _sel("フェーズ"),
                    "車検満了日": _date("車検満了日"),
                    "備考"     : _txt("備考"),
                    "提案内容" : _title("提案内容"),
                    "_page_id" : page.get("id",""),
                })
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")
        return logs
    except Exception as e:
        return []

def save_activity_log(log: dict) -> bool:
    """活動ログをNotion Databaseに1件保存（複数拠点対応）"""
    import requests
    headers = _notion_headers()
    if not headers["Authorization"].replace("Bearer ", ""):
        st.warning("⚠️ Notion APIトークンが設定されていません。SecretsにNOTION_TOKENを設定してください。")
        return False
    try:
        # 登録番号を正規化
        if "登録番号" in log:
            log["登録番号"] = normalize_plate(log.get("登録番号", ""))

        # 提案内容タイトルを生成
        title = f"{log.get('記録日','')} {log.get('提案項目','')} {log.get('フェーズ','')}"

        # フェーズ・提案項目の絵文字を除去（Notionのselectと合わせる）
        def clean_phase(p):
            return p.replace(" ⭐","").replace(" 📅","").replace(" ✅","").strip()

        phase_clean = clean_phase(log.get("フェーズ",""))
        item_clean  = log.get("提案項目","")

        # 車検満了日
        shaken_date = log.get("車検満了日","")

        body = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": {
                "提案内容": {"title": [{"text": {"content": title[:100]}}]},
                "記録日"  : {"date": {"start": str(log.get("記録日",""))[:10].replace("/","-")}},
                "顧客ID"  : {"rich_text": [{"text": {"content": str(log.get("顧客ID",""))}}]},
                "車両ID"  : {"rich_text": [{"text": {"content": str(log.get("車両ID",""))}}]},
                "登録番号": {"rich_text": [{"text": {"content": str(log.get("登録番号",""))}}]},
                "担当者"  : {"rich_text": [{"text": {"content": str(log.get("担当者",""))}}]},
                "店舗"    : {"rich_text": [{"text": {"content": str(log.get("店舗",""))}}]},
                "ランク"  : {"select": {"name": str(log.get("ランク","D"))}},
                "提案項目": {"select": {"name": item_clean}},
                "フェーズ": {"select": {"name": phase_clean}},
                "備考"    : {"rich_text": [{"text": {"content": str(log.get("備考",""))[:200]}}]},
                "log_id"  : {"rich_text": [{"text": {"content": str(log.get("log_id",""))}}]},
            }
        }
        shaken_date_iso = str(shaken_date or "")[:10].replace("/", "-") if shaken_date else None
        if shaken_date_iso and len(shaken_date_iso) == 10:
            body["properties"]["車検満了日"] = {"date": {"start": shaken_date_iso}}

        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers, json=body, timeout=10
        )
        if resp.status_code == 200:
            load_activity_logs.clear()  # キャッシュクリア
            return True
        else:
            st.warning(f"Notion保存エラー: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        st.warning(f"ログ保存エラー: {e}")
        return False

def get_latest_log(cust_id: str, veh_id: str) -> dict:
    """顧客IDと車両IDで最新の活動ログを取得"""
    logs = load_activity_logs()
    matched = [l for l in logs
               if l.get("顧客ID")==str(cust_id) and l.get("車両ID")==str(veh_id)]
    if matched:
        return sorted(matched, key=lambda x: x.get("記録日",""), reverse=True)[0]
    return {}

def logs_to_df(logs: list) -> pd.DataFrame:
    """ログリストをDataFrameに変換"""
    if not logs:
        return pd.DataFrame(columns=["記録日","顧客ID","車両ID","登録番号",
                                      "担当者","店舗","ランク","提案項目",
                                      "フェーズ","車検満了日","備考"])
    return pd.DataFrame(logs)


# ──────────────────────────────────────────────
# GoogleドライブファイルID定義（Streamlit Cloud用）
# ──────────────────────────────────────────────
GDRIVE_IDS_LTV = {
    "Master_Data.csv"              : "1x8G8rfKrWlMRQHk24YXj1njFgliyh90a",
    "車検.csv"                      : "1mUuntuW0XQQmlKnxCZTc1VkIMNX128ny",
    "タイヤ交換.csv"                 : "1KOaf52KjimPybOAti_LV9h1g1i3SflBc",
    "オイル交換.csv"                 : "1f006hLzchj5AYSTMnFD4Rq9P7T5fvD4q",
    "12ヶ月点検.csv"                 : "1LeODLrzZ85NpCbbXyEQSrumO6Uj3ejSs",
    "バッテリー交換.csv"              : "1Ik3JoDV2SRpC-u5GBc21SYqxwBm2xVFW",
    "ボディーコート（silver）.csv"    : "1JYtBXW3xh4Wd9pDicn3Y3RtD1NVRxsMu",
    "ワイパー交換.csv"                : "1piJojvcbgK6ZzhZAB7QdrVnbdMNaP64G",
    "自動車販売.csv"                 : "1iKwaN3AK_XmVqpvXNupBlF10Apgkw4d6",
    "Reservation.csv"               : "1qUHPbUHusS40jT0nAcNaaY2vEo0AfT7i",
    "保険.csv"                      : "",  # 作成後に設定
    "staff_master.csv"              : "",  # 担当者マスター（設定後に記入）
}

# Googleドライブ上の担当者マスターCSVファイルID
# ※ GoogleドライブにCSVをアップロードしてIDを設定してください
GDRIVE_STAFF_ID = ""  # ← 設定後にファイルIDを記入

@st.cache_data(ttl=300, show_spinner=False)
def load_gdrive_csv(file_id: str, name: str) -> pd.DataFrame:
    """GoogleドライブからCSVを読み込む（5分キャッシュ）"""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    for enc in ["cp932", "utf-8-sig", "utf-8", "shift_jis"]:
        try:
            return pd.read_csv(url, encoding=enc)
        except Exception:
            continue
    raise ValueError(f"{name} をGoogleドライブから読み込めませんでした。")

# ──────────────────────────────────────────────
# データフォルダ自動読込ユーティリティ
# ──────────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"

# マスターデータのファイル名候補
MASTER_NAMES = ["Master_Data.csv", "マスターデータ.csv", "master_data.csv", "master.csv"]
# 予約データのファイル名候補
RSV_NAMES    = ["Reservation.csv", "予約データ.csv", "reservation.csv", "予約.csv"]
# 仮予約データのファイル名候補
KARI_RSV_NAMES = ["仮予約.csv", "仮予約データ.csv", "kari_reservation.csv", "provisional.csv"]

# 車検と判定するメニュー名キーワード（元メニュー名で判定）
SHAKEN_KEYWORDS = [
    "車検", "軽自動車", "国産乗用車", "輸入車", "貨物車",
    "特種用途", "特殊用途", "一日作業枠", "６時間枠", "6時間枠",
    "４時間枠", "4時間枠", "継続検査", "新規検査"
]


def scan_data_dir():
    """dataフォルダをスキャンしてCSVファイルを分類する"""
    if not DATA_DIR.exists():
        return None, [], None, None

    all_csvs = list(DATA_DIR.glob("*.csv")) + list(DATA_DIR.glob("*.CSV"))

    # マスターデータ特定
    master_path = None
    for name in MASTER_NAMES:
        p = DATA_DIR / name
        if p.exists():
            master_path = p
            break

    # 予約データ特定
    rsv_path = None
    for name in RSV_NAMES:
        p = DATA_DIR / name
        if p.exists():
            rsv_path = p
            break

    # 仮予約データ特定
    kari_path = None
    for name in KARI_RSV_NAMES:
        p = DATA_DIR / name
        if p.exists():
            kari_path = p
            break

    # 取引CSVリスト（マスター・予約・仮予約以外）
    exclude = set(MASTER_NAMES + RSV_NAMES + KARI_RSV_NAMES)
    txn_paths = [
        p for p in all_csvs
        if p.name not in exclude
    ]

    return master_path, txn_paths, rsv_path, kari_path


def read_path(p, name=""):
    """Pathオブジェクトからデータフレームを読み込む"""
    for enc in ["utf-8-sig", "utf-8", "cp932", "shift_jis", "latin1"]:
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    raise ValueError(f"{name} を読み込めませんでした")


# ──────────────────────────────────────────────
# サイドバー
# ──────────────────────────────────────────────
# dataフォルダのスキャン
auto_master_path, auto_txn_paths, auto_rsv_path, auto_kari_path = scan_data_dir()
data_dir_exists = DATA_DIR.exists()

with st.sidebar:
    # ── データ読込モード表示 ──
    _gdrive_ready = bool(GDRIVE_IDS_LTV.get("Master_Data.csv"))
    if data_dir_exists and auto_master_path:
        st.markdown('''<div style="background:#ecfdf5;border:1px solid #6ee7b7;border-radius:8px;
        padding:0.5rem 0.8rem;font-size:0.78rem;color:#065f46;margin-bottom:0.5rem;">
        📁 <strong>自動読込モード</strong>：dataフォルダのCSVを検出しました
        </div>''', unsafe_allow_html=True)
    elif _gdrive_ready:
        st.markdown('''<div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:8px;
        padding:0.5rem 0.8rem;font-size:0.78rem;color:#1e40af;margin-bottom:0.5rem;">
        ☁️ <strong>Googleドライブ読込モード</strong>：クラウドからデータを自動取得します
        </div>''', unsafe_allow_html=True)
    else:
        st.markdown('''<div style="background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;
        padding:0.5rem 0.8rem;font-size:0.78rem;color:#92400e;margin-bottom:0.5rem;">
        📤 <strong>アップロードモード</strong>：dataフォルダが未作成またはCSV未配置
        </div>''', unsafe_allow_html=True)

    st.markdown("### 📂 手動アップロード（任意）")
    st.caption("dataフォルダのCSVを上書きしたい場合のみアップロード")
    f_master = st.file_uploader(
        "① マスターデータ.csv", type=["csv"], key="master",
        help="列：顧客ID, 車両ID, 登録番号, 車検満了日, 入庫店舗ID"
    )
    f_txns = st.file_uploader(
        "② 取引CSV（複数選択可）", type=["csv"], key="txn",
        accept_multiple_files=True,
        help="列：車両ID, 前回取引日\nファイル名がサービス名になります（例：車検.csv）"
    )
    f_rsv = st.file_uploader(
        "③ 予約データ.csv", type=["csv"], key="rsv",
        help="列：登録番号, サービス種別, 予約ステータス, 予約日"
    )
    f_kari = st.file_uploader(
        "④ 仮予約.csv（任意）", type=["csv"], key="kari",
        help="仮予約データ。登録番号列があれば自動で突合します。\ndataフォルダに「仮予約.csv」を置いても自動読込されます。"
    )

    st.markdown("---")
    st.markdown("### ⚙️ ランク定義（顧客ID起点・確定版）")
    st.markdown("""
| ランク | 定義 |
|---|---|
| 🏆 **SSS** | 複数台車検＋自動車販売 |
| 🥇 **S** | 車検＋自動車販売 |
| 🥈 **A** | 車検＋オイル＋タイヤ |
| 🥉 **B** | 車検あり（A未満） |
| **C** | 車検なし・メンテ来店 |
| **D** | ほぼ未取引 |
""")
    st.caption("※ランクは顧客ID単位で判定します")
    # 旧ロジック互換（フォールバック用）
    thresholds = {"S": 5, "A": 4, "B": 3, "C": 2, "D": 0}

    # ── 担当者マスター管理 ──
    st.markdown("---")
    st.markdown("### 👤 担当者マスター")
    staff_list = load_staff_master()

    # Googleドライブ設定状況を表示
    if GDRIVE_STAFF_ID:
        st.markdown('<div style="background:#eff6ff;border:1px solid #93c5fd;border-radius:6px;padding:0.4rem 0.8rem;font-size:0.78rem;color:#1e40af;">☁️ Googleドライブから読み込み中</div>', unsafe_allow_html=True)
    else:
        st.caption("💡 staff_master.csvをGoogleドライブにアップロードしてIDを設定するとCSVで一括管理できます")

    with st.expander("担当者を追加・削除（アプリ内）", expanded=False):
        st.caption("Googleドライブ未設定時または追加分の管理に使用")
        new_staff = st.text_input("担当者名", key="new_staff_input", placeholder="例：佐藤")
        ca, cd = st.columns(2)
        with ca:
            if st.button("➕ 追加", use_container_width=True, key="add_staff"):
                name = new_staff.strip()
                if name and name not in staff_list:
                    staff_list.append(name)
                    save_staff_master(staff_list)
                    st.success(f"追加しました：{name}")
        with cd:
            del_t = st.selectbox("削除対象", ["─"]+staff_list, key="del_staff_sel")
            if st.button("🗑️ 削除", use_container_width=True, key="del_staff_btn"):
                if del_t != "─":
                    staff_list = [s for s in staff_list if s != del_t]
                    save_staff_master(staff_list)
                    st.success(f"削除しました：{del_t}")
        st.caption(f"現在の担当者：{' / '.join(staff_list) if staff_list else '未登録'}")

        # 担当者CSVダウンロード（Googleドライブ用テンプレート）
        if staff_list:
            import io as _io
            df_staff_dl = pd.DataFrame(staff_list, columns=["担当者名"])
            buf_staff = _io.BytesIO()
            df_staff_dl.to_csv(buf_staff, index=False, header=False, encoding="utf-8-sig")
            st.download_button(
                "📥 staff_master.csvダウンロード（Googleドライブ用）",
                buf_staff.getvalue(),
                "staff_master.csv",
                "text/csv",
                use_container_width=True,
                key="dl_staff"
            )

    # ── 活動ログ管理 ──
    st.markdown("---")
    st.markdown("### 📊 活動ログ")
    if st.button("🔄 ログを読み込む", use_container_width=True, key="reload_logs_btn"):
        load_activity_logs.clear()
        st.rerun()
    logs_all = load_activity_logs()
    st.caption(f"累計記録件数：{len(logs_all):,}件（Notionデータベース）")
    if st.button("📥 CSVダウンロード", use_container_width=True, key="dl_logs_btn"):
        if logs_all:
            df_log_dl = logs_to_df(logs_all)
            buf_log = BytesIO()
            df_log_dl.to_csv(buf_log, index=False, encoding="utf-8-sig")
            st.download_button(
                "⬇️ ダウンロード実行",
                buf_log.getvalue(),
                f"activity_log_{datetime.today().strftime('%Y%m%d')}.csv",
                "text/csv",
                use_container_width=True,
                key="dl_logs_exec"
            )
        else:
            st.info("まだ活動ログがありません（NOTION_TOKENが設定されているか確認してください）")

    st.markdown("---")
    st.markdown("### 読込状況")

    _gd = GDRIVE_IDS_LTV  # 短縮参照

    # マスター
    if f_master:
        st.markdown('<div class="upload-status upload-ok">✅ マスター（手動アップロード）</div>', unsafe_allow_html=True)
    elif auto_master_path:
        st.markdown(f'<div class="upload-status upload-ok">📁 マスター：{auto_master_path.name}</div>', unsafe_allow_html=True)
    elif _gd.get("Master_Data.csv"):
        st.markdown('<div class="upload-status upload-ok">☁️ マスター（Googleドライブ）</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="upload-status upload-wait">⏳ マスターデータ（未検出）</div>', unsafe_allow_html=True)

    # 取引CSV
    if f_txns:
        for f in f_txns:
            st.markdown(f'<div class="upload-status upload-ok">✅ {f.name}（手動）</div>', unsafe_allow_html=True)
    elif auto_txn_paths:
        for p in auto_txn_paths:
            st.markdown(f'<div class="upload-status upload-ok">📁 {p.name}</div>', unsafe_allow_html=True)
    else:
        # Googleドライブの取引CSV件数を表示
        gdrive_txn_count = sum(1 for k, v in _gd.items()
            if v and k not in ("Master_Data.csv", "Reservation.csv", "保険.csv"))
        if gdrive_txn_count > 0:
            st.markdown(f'<div class="upload-status upload-ok">☁️ 取引CSV：{gdrive_txn_count}ファイル（Googleドライブ）</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="upload-status upload-wait">⏳ 取引CSV（未検出）</div>', unsafe_allow_html=True)

    # 予約
    if f_rsv:
        st.markdown('<div class="upload-status upload-ok">✅ 予約データ（手動アップロード）</div>', unsafe_allow_html=True)
    elif auto_rsv_path:
        st.markdown(f'<div class="upload-status upload-ok">📁 予約：{auto_rsv_path.name}</div>', unsafe_allow_html=True)
    elif _gd.get("Reservation.csv"):
        st.markdown('<div class="upload-status upload-ok">☁️ 予約データ（Googleドライブ）</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="upload-status upload-wait">⏳ 予約データ（未検出）</div>', unsafe_allow_html=True)

    # 仮予約
    if f_kari:
        st.markdown('<div class="upload-status upload-ok">✅ 仮予約データ（手動アップロード）</div>', unsafe_allow_html=True)
    elif auto_kari_path:
        st.markdown(f'<div class="upload-status upload-ok">📁 仮予約：{auto_kari_path.name}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="upload-status upload-wait">─ 仮予約データ（任意）</div>', unsafe_allow_html=True)

    # dataフォルダのパス表示
    st.markdown("---")
    st.caption(f"📁 dataフォルダ：{DATA_DIR}")
    if not data_dir_exists:
        st.caption("☁️ Googleドライブからデータを読み込みます")


# ──────────────────────────────────────────────
# サンプルCSV生成
# ──────────────────────────────────────────────
# ── マスターデータの決定（手動アップロード優先 > dataフォルダ > Googleドライブ）──
_has_master = (
    f_master is not None
    or auto_master_path is not None
    or bool(GDRIVE_IDS_LTV.get("Master_Data.csv"))  # GoogleドライブIDが設定済み
)

if not _has_master:
    st.info("CSVが見つかりません。以下のいずれかの方法でデータを読み込んでください。")
    st.markdown("""
**方法① dataフォルダに置く（推奨・毎回不要）**
```
C:/SATO/LTV/data\\Master_Data.csv   ← マスターデータ
C:/SATO/LTV/data\\車検.csv           ← 取引CSV（ファイル名=サービス名）
C:/SATO/LTV/data\\Reservation.csv   ← 予約データ
```
起動するたびに自動で読み込まれます。予約データは毎日上書き保存するだけでOK。

**方法② サイドバーからアップロード**
毎回アップロードする従来の方法。
""")
    with st.expander("📋 サンプルCSVをダウンロード（テスト用）", expanded=True):
        np.random.seed(42)
        N = 300
        STORES = ["川口店", "草加店", "越谷店", "春日部店", "浦和店", "大宮店"]
        TODAY = datetime(2026, 3, 1)
        START = datetime(2023, 1, 1)

        master = pd.DataFrame({
            "顧客ID": [f"C{i:06d}" for i in range(1, N + 1)],
            "車両ID": [f"V{i:06d}" for i in range(1, N + 1)],
            "登録番号": [
                f"大宮{np.random.randint(100,999)}-{np.random.choice(['あ','い','う','え'])}-{np.random.randint(1,9999):04d}"
                for _ in range(N)
            ],
            "車検満了日": [
                (TODAY + timedelta(days=int(np.random.randint(-180, 545)))).strftime("%Y/%m/%d")
                for _ in range(N)
            ],
            "入庫店舗ID": [np.random.choice(STORES) for _ in range(N)],
        })
        vids = master["車両ID"].tolist()
        regs = master["登録番号"].tolist()

        rates = {"車検": 0.78, "タイヤ交換": 0.62, "オイル交換": 0.71,
                 "12か月点検": 0.55, "バッテリー交換": 0.43,
                 "コーティング": 0.28, "自動車販売": 0.22}
        txn_dfs = {}
        for svc, rate in rates.items():
            n = int(N * rate)
            days_back = np.random.randint(0, (TODAY - START).days, n)
            dates = [(START + timedelta(days=int(d))).strftime("%Y/%m/%d") for d in days_back]
            if svc == "自動車販売":
                # 自動車販売のみキーが「登録番号」
                chosen = np.random.choice(regs, n, replace=False)
                txn_dfs[svc] = pd.DataFrame({"登録番号": chosen, "前回取引日": dates})
            else:
                chosen = np.random.choice(vids, n, replace=False)
                txn_dfs[svc] = pd.DataFrame({"車両ID": chosen, "前回取引日": dates})

        n_rsv = int(N * 0.40)
        rsv_df = pd.DataFrame({
            "登録番号": np.random.choice(regs, n_rsv, replace=True),
            "サービス種別": np.random.choice(DEFAULT_SERVICES, n_rsv),
            "予約ステータス": np.random.choice(["本予約", "仮予約", "相談中"], n_rsv, p=[0.45, 0.35, 0.20]),
            "予約日": [(TODAY + timedelta(days=int(np.random.randint(0, 180)))).strftime("%Y/%m/%d") for _ in range(n_rsv)],
        })

        st.markdown("**① マスターデータ.csv**")
        st.dataframe(master.head(5), use_container_width=True, hide_index=True)
        st.download_button("📥 Master_Data.csv", to_csv(master), "Master_Data.csv", "text/csv", use_container_width=True)

        st.markdown("**② 取引CSV（サービス別）**")
        cols_dl = st.columns(4)
        for i, (svc, df_t) in enumerate(txn_dfs.items()):
            with cols_dl[i % 4]:
                st.download_button(f"📥 {svc}.csv", to_csv(df_t), f"{svc}.csv", "text/csv", use_container_width=True)

        st.markdown("**③ 予約データ.csv**")
        st.dataframe(rsv_df.head(5), use_container_width=True, hide_index=True)
        st.download_button("📥 Reservation.csv", to_csv(rsv_df), "Reservation.csv", "text/csv", use_container_width=True)

        st.markdown("""
---
### 📌 CSVフォーマット仕様

| ファイル | 必須列 | 備考 |
|----------|--------|------|
| Master_Data.csv | 顧客ID, 車両ID, 登録番号, 車検満了日, 入庫店舗ID | 1ファイル |
| 取引CSV（自動車販売以外） | 車両ID, 前回取引日 | ファイル名がサービス名 |
| 自動車販売.csv | 登録番号, 前回取引日 | キーが登録番号 |
| Reservation.csv | 登録番号, サービス種別, 予約ステータス, 予約日 | 毎日上書きOK |

### 💡 dataフォルダの作り方
コマンドプロンプトで：
```
mkdir C:/SATO/LTV/data
```
その中にCSVを配置してください。
""")
    st.stop()


# ══════════════════════════════════════════════
# データ読み込み & 突合
# ══════════════════════════════════════════════

# ── マスターデータ読み込み（手動優先 > Googleドライブ > dataフォルダ）──
try:
    if f_master is not None:
        df_master = norm(read_csv(f_master, "マスターデータ"))
    elif auto_master_path is not None:
        df_master = norm(read_path(auto_master_path, "マスターデータ"))
    elif GDRIVE_IDS_LTV.get("Master_Data.csv"):
        df_master = norm(load_gdrive_csv(GDRIVE_IDS_LTV["Master_Data.csv"], "Master_Data.csv"))
    else:
        st.error("マスターデータが見つかりません。")
        st.stop()
except Exception as e:
    st.error(f"マスターデータ読み込みエラー: {e}")
    st.stop()

# ── Master_Data 英字列名 → 日本語列名の変換（基幹システム出力形式に対応）──
MASTER_COL_RENAME = {
    "SYARYO_ID"    : "車両ID",
    "TOROKUBANGO"  : "登録番号",
    "MANRYOBI"     : "車検満了日",
    "SYARYO_KYOTEN": "入庫店舗ID",
    "SHONENDO"     : "初年度登録",
    "TOROKUBI"     : "登録日",
    "SHAMEI"       : "車名",
    "TSUSHOMEI"    : "通称名",
    "KATASHIKI"    : "型式",
    "KYOTEN_ID"    : "拠点ID",
}
df_master.rename(columns={k: v for k, v in MASTER_COL_RENAME.items()
                           if k in df_master.columns}, inplace=True)

# 初年度登録列の候補（複数の列名に対応）
shonendo_col = find_col(df_master, ["初年度登録", "SHONENDO", "初年度", "登録年度", "first_year"])
if shonendo_col and shonendo_col != "初年度登録":
    df_master.rename(columns={shonendo_col: "初年度登録"}, inplace=True)

# 初年度登録を日付型に変換
if "初年度登録" in df_master.columns:
    df_master["初年度登録"] = pd.to_datetime(df_master["初年度登録"], errors="coerce")

df_master["車検満了日"] = pd.to_datetime(df_master["車検満了日"], errors="coerce")
store_col = find_col(df_master, ["入庫店舗ID", "店舗ID", "店舗"])
if store_col and store_col != "入庫店舗ID":
    df_master.rename(columns={store_col: "入庫店舗ID"}, inplace=True)
elif "入庫店舗ID" not in df_master.columns:
    df_master["入庫店舗ID"] = "全店"
df_master["車両ID"] = df_master["車両ID"].astype(str).str.strip()

# ── 取引CSV突合 ──
# 自動車販売のみキーが「登録番号」、それ以外は「車両ID」で突合する
CAR_SALES_SVC = "自動車販売"  # 登録番号キーを使うサービス名

all_services = []
txn_sets = {}    # {svc: set(キー値)}
txn_dates = {}   # {svc: {キー値: 最終取引日}}
txn_key = {}     # {svc: "車両ID" or "登録番号"}  マスター側の結合キー

# 手動アップロード優先 > dataフォルダ > Googleドライブ
_txn_sources = []  # (モード, ソース)
if f_txns:
    _txn_sources = [("upload", f) for f in f_txns]
elif auto_txn_paths:
    _txn_sources = [("auto", p) for p in auto_txn_paths]
else:
    # Googleドライブからすべての取引CSVを読み込む
    for fname, fid in GDRIVE_IDS_LTV.items():
        if fname in ("Master_Data.csv", "Reservation.csv", "保険.csv") or not fid:
            continue
        _txn_sources.append(("gdrive", (fname, fid)))

if _txn_sources:
    for mode, src in _txn_sources:
        if mode == "upload":
            f = src
            raw_svc = f.name.replace(".csv", "").replace(".CSV", "").strip()
            svc = FILE_TO_SERVICE.get(raw_svc, raw_svc)
            try:
                df_t = norm(read_csv(f, svc))
            except Exception:
                st.warning(f"⚠️ {f.name} を読み込めませんでした")
                continue
        elif mode == "gdrive":
            fname, fid = src
            raw_svc = fname.replace(".csv", "").replace(".CSV", "").strip()
            svc = FILE_TO_SERVICE.get(raw_svc, raw_svc)
            try:
                df_t = norm(load_gdrive_csv(fid, fname))
            except Exception:
                st.warning(f"⚠️ {fname}（Googleドライブ）を読み込めませんでした")
                continue
        else:
            # auto mode: src is a Path
            p = src
            raw_svc = p.name.replace(".csv", "").replace(".CSV", "").strip()
            svc = FILE_TO_SERVICE.get(raw_svc, raw_svc)
            try:
                df_t = norm(read_path(p, svc))
            except Exception:
                st.warning(f"⚠️ {p.name} を読み込めませんでした")
                continue

        # ── キー列の決定 ──
        if svc == CAR_SALES_SVC:
            # 自動車販売：登録番号キー
            vc = find_col(df_t, ["登録番号", "vehicle_number", "ナンバー", "reg_num"])
            master_key = "登録番号"
            if not vc:
                st.warning(f"⚠️ {svc}：「登録番号」列が見つかりません（自動車販売は登録番号キーが必要です）")
                continue
        elif svc == "保険":
            # 保険：登録番号キー・保険満期日列を使用
            vc = find_col(df_t, ["登録番号", "vehicle_number", "ナンバー"])
            master_key = "登録番号"
            if not vc:
                st.warning(f"⚠️ 保険.csv：「登録番号」列が見つかりません")
                continue
            # 保険満期日をマスターに追加
            dc_ins = find_col(df_t, ["保険満期日", "満期日", "insurance_expiry"])
            if dc_ins:
                df_t[vc] = df_t[vc].astype(str).str.replace(" ", "", regex=False).str.strip()
                ins_map = df_t.dropna(subset=[dc_ins]).groupby(vc)[dc_ins].max().to_dict()
                if "登録番号" not in df_master.columns:
                    df_master["登録番号"] = df_master.get("登録番号", "")
                df_master["登録番号_str"] = df_master["登録番号"].astype(str).str.replace(" ", "", regex=False).str.strip()
                df_master["保険満期日"] = df_master["登録番号_str"].map(ins_map)
                df_master.drop(columns=["登録番号_str"], inplace=True, errors="ignore")
        else:
            # その他サービス：車両IDキー
            vc = find_col(df_t, ["車両ID", "VehicleID", "vehicle_id"])
            master_key = "車両ID"
            if not vc:
                st.warning(f"⚠️ {svc}：「車両ID」列が見つかりません")
                continue

        dc = find_col(df_t, ["前回取引日", "取引日", "最終取引日", "日付", "date", "Date", "保険満期日", "満期日"])
        df_t[vc] = df_t[vc].astype(str).str.strip()
        txn_sets[svc]  = set(df_t[vc].dropna())
        txn_key[svc]   = master_key
        if dc:
            df_t[dc] = pd.to_datetime(df_t[dc], errors="coerce")
            txn_dates[svc] = df_t.groupby(vc)[dc].max().to_dict()
        all_services.append(svc)

if not all_services:
    all_services = DEFAULT_SERVICES
    for svc in all_services:
        txn_key[svc] = "車両ID"

# 重複サービス名を排除（同名ファイルが複数読み込まれた場合の安全策）
seen = []
for s in all_services:
    if s not in seen:
        seen.append(s)
all_services = seen

# マスター側キーを文字列化（登録番号も念のため）
df_master["車両ID"]   = df_master["車両ID"].astype(str).str.strip()
if "登録番号" in df_master.columns:
    df_master["登録番号"] = df_master["登録番号"].astype(str).str.strip()

for svc in all_services:
    mk = txn_key.get(svc, "車両ID")  # マスター側の結合キー
    if mk in df_master.columns:
        df_master[f"取引_{svc}"] = df_master[mk].isin(txn_sets.get(svc, set())).astype(int)
    else:
        df_master[f"取引_{svc}"] = 0
    if svc in txn_dates:
        df_master[f"最終取引日_{svc}"] = df_master[mk].map(txn_dates[svc])

txn_cols = [f"取引_{s}" for s in all_services]

# ── 取引フラグ列を確実にint型の独立列として再構築 ──
# （列名重複や型混入を完全に排除するため、新しいDataFrameとして再作成）
flag_dict = {}
for svc in all_services:
    col = f"取引_{svc}"
    if col in df_master.columns:
        flag_dict[col] = pd.to_numeric(
            df_master[col].squeeze(),  # Series/DataFrame両対応
            errors="coerce"
        ).fillna(0).astype(int).values
    else:
        flag_dict[col] = 0

for col, vals in flag_dict.items():
    df_master[col] = vals

# ── 取引フラグMatrix・LTV計算 ──
flag_matrix = np.column_stack([df_master[c].values for c in txn_cols])
df_master["取引サービス数"] = flag_matrix.sum(axis=1).astype(int)

sales_arr = np.array([SERVICE_PRICE.get(s, 0) for s in all_services])
gross_arr = np.array([SERVICE_GROSS.get(s, 0) for s in all_services])
df_master["売上LTV"] = flag_matrix.dot(sales_arr).astype(int)
df_master["粗利LTV"] = flag_matrix.dot(gross_arr).astype(int)

# 未取引サービス列
untreated_list = []
for i in range(len(df_master)):
    row_flags = flag_matrix[i]
    untreated_list.append(
        "、".join([s for s, f in zip(all_services, row_flags) if int(f) == 0])
    )
df_master["未取引サービス"] = untreated_list

# ──────────────────────────────────────────────
# 顧客ID起点のランク計算（確定版）
# ──────────────────────────────────────────────
if "顧客ID" in df_master.columns:
    # 顧客単位で集計
    svc_flag_cols = {s: f"取引_{s}" for s in all_services}
    agg_dict = {"粗利LTV": "sum", "売上LTV": "sum", "取引サービス数": "sum"}

    # 車検台数（顧客単位での車検取引台数）
    if "取引_車検" in df_master.columns:
        agg_dict["cust_車検台数"] = ("取引_車検", "sum")

    for svc in all_services:
        col = f"取引_{svc}"
        if col in df_master.columns:
            agg_dict[f"cust_{svc}"] = (col, "max")  # 1台でもあればOK

    cust_df = df_master.groupby("顧客ID").agg(**{
        k: v if isinstance(v, tuple) else pd.NamedAgg(column=v[0], aggfunc=v[1])
        if isinstance(v, tuple) else pd.NamedAgg(column=k, aggfunc=v)
        for k, v in agg_dict.items()
    }).reset_index() if False else df_master.groupby("顧客ID").agg(
        粗利LTV_顧客=("粗利LTV", "sum"),
        売上LTV_顧客=("売上LTV", "sum"),
        cust_車検台数=("取引_車検", "sum") if "取引_車検" in df_master.columns else ("取引サービス数", "first"),
        **{f"cust_{s}": (f"取引_{s}", "max") for s in all_services if f"取引_{s}" in df_master.columns},
    ).reset_index()

    # 顧客単位のランク付け
    cust_df["ランク"] = cust_df.apply(assign_rank_customer, axis=1)

    # マスターに顧客ランクをマージ
    df_master = df_master.merge(
        cust_df[["顧客ID", "ランク", "粗利LTV_顧客", "売上LTV_顧客"]],
        on="顧客ID", how="left"
    )
    df_master["ランク"] = df_master["ランク"].fillna("D")
    df_master["粗利LTV_顧客"] = df_master["粗利LTV_顧客"].fillna(df_master["粗利LTV"])
else:
    # 顧客IDがない場合のフォールバック（車両単位の旧ロジック）
    df_master["ランク"] = df_master["取引サービス数"].apply(
        lambda n: assign_rank(n, thresholds))
    df_master["粗利LTV_顧客"] = df_master["粗利LTV"]

# ── 予約CSV突合（手動優先 > dataフォルダ > Googleドライブ）──
has_rsv = (
    (f_rsv is not None)
    or (auto_rsv_path is not None)
    or bool(GDRIVE_IDS_LTV.get("Reservation.csv"))  # GoogleドライブIDが設定済み
)
df_rsv = None
if has_rsv:
    try:
        if f_rsv is not None:
            df_rsv = norm(read_csv(f_rsv, "予約データ"))
        elif auto_rsv_path is not None:
            df_rsv = norm(read_path(auto_rsv_path, "予約データ"))
        elif GDRIVE_IDS_LTV.get("Reservation.csv"):
            df_rsv = norm(load_gdrive_csv(GDRIVE_IDS_LTV["Reservation.csv"], "Reservation.csv"))
        for orig, std in [
            (["登録番号", "vehicle_number", "ナンバー", "reg_num"], "登録番号"),
            (["サービス種別", "service", "サービス", "取引種別", "種別"], "サービス種別"),
            (["予約ステータス", "status", "Status", "ステータス"], "予約ステータス"),
            (["予約日", "date", "Date", "booking_date"], "予約日"),
        ]:
            c = find_col(df_rsv, orig)
            if c and c != std:
                df_rsv.rename(columns={c: std}, inplace=True)
        if "予約日" in df_rsv.columns:
            df_rsv["予約日"] = pd.to_datetime(df_rsv["予約日"], errors="coerce")
            df_rsv["予約月"] = df_rsv["予約日"].dt.to_period("M").astype(str)
    except Exception as e:
        st.warning(f"予約データ読み込みエラー: {e}")
        has_rsv = False
        df_rsv = None

if has_rsv and df_rsv is not None and "登録番号" in df_rsv.columns and "登録番号" in df_master.columns:
    rsv_sum = df_rsv.groupby("登録番号").agg(
        予約件数=("予約ステータス", "count"),
        本予約数=("予約ステータス", lambda x: (x == "本予約").sum()),
        仮予約数=("予約ステータス", lambda x: (x == "仮予約").sum()),
        予約サービス=("サービス種別", lambda x: "、".join(x.dropna().unique()[:4])),
    ).reset_index()
    df_master = df_master.merge(rsv_sum, on="登録番号", how="left")
    for c in ["予約件数", "本予約数", "仮予約数"]:
        df_master[c] = df_master[c].fillna(0).astype(int)
    df_master["予約サービス"] = df_master["予約サービス"].fillna("")
else:
    df_master["予約件数"] = 0
    df_master["本予約数"] = 0
    df_master["仮予約数"] = 0
    df_master["予約サービス"] = ""

# ── 仮予約CSV読み込み・突合 ──
# 仮予約CSVの登録番号と突合して「仮予約あり」フラグを付与
df_kari = None
has_kari = (f_kari is not None) or (auto_kari_path is not None)
if has_kari:
    try:
        if f_kari is not None:
            df_kari = norm(read_csv(f_kari, "仮予約データ"))
        else:
            df_kari = norm(read_path(auto_kari_path, "仮予約データ"))
        # 登録番号列の特定
        kari_reg_col = find_col(df_kari, ["登録番号", "vehicle_number", "ナンバー"])
        if kari_reg_col:
            df_kari[kari_reg_col] = df_kari[kari_reg_col].astype(str).str.strip()
            kari_regs = set(df_kari[kari_reg_col].dropna())
        else:
            kari_regs = set()
            has_kari = False
    except Exception as e:
        st.warning(f"仮予約データ読み込みエラー: {e}")
        has_kari = False
        df_kari = None
        kari_regs = set()
else:
    kari_regs = set()

# ── 車検予約ステータスをベクトル演算で一括付与 ──
# apply+全行ループを排除し、辞書引き（O(1)）で高速化
def build_shaken_dict(df_rsv_data, kari_regs_set):
    """登録番号 → 車検予約ステータス の辞書を一括構築"""
    result = {}
    if df_rsv_data is None or "登録番号" not in df_rsv_data.columns:
        return result

    df_r = df_rsv_data.copy()
    df_r["登録番号"] = df_r["登録番号"].astype(str).str.strip()
    # 登録番号が空・nan・NaNの行を除外
    df_r = df_r[~df_r["登録番号"].isin(["", "nan", "NaN", "None", "NULL"])]
    df_r = df_r[df_r["登録番号"].str.len() > 0]

    # 車検行を特定（元メニュー名またはサービス種別）
    if "元メニュー名" in df_r.columns:
        _kws = SHAKEN_KEYWORDS
        is_shaken = df_r["元メニュー名"].astype(str).apply(
            lambda m: any(kw in m for kw in _kws)
        )
    elif "サービス種別" in df_r.columns:
        is_shaken = df_r["サービス種別"].astype(str) == "車検"
    else:
        is_shaken = pd.Series(False, index=df_r.index)

    df_shaken = df_r[is_shaken]

    if not df_shaken.empty and "予約ステータス" in df_shaken.columns:
        # 本予約優先 → 仮予約 → 相談中 の順で集約
        STATUS_RANK = {"本予約": 3, "仮予約": 2, "相談中": 1}
        df_shaken = df_shaken.copy()
        df_shaken["_rank"] = df_shaken["予約ステータス"].map(STATUS_RANK).fillna(0)
        # 登録番号ごとに最高ランクのステータスを取得
        best = df_shaken.groupby("登録番号")["_rank"].max()
        rank_to_label = {3: "車検予約済", 2: "仮予約あり", 1: "相談中", 0: "予約なし"}
        for reg, rank in best.items():
            result[str(reg)] = rank_to_label.get(int(rank), "予約なし")

    # 仮予約CSVで上書き
    for reg in kari_regs_set:
        result[str(reg)] = "仮予約あり"

    return result

if has_rsv and df_rsv is not None and "登録番号" in df_master.columns:
    df_master["登録番号"] = df_master["登録番号"].astype(str).str.strip()
    _shaken_dict = build_shaken_dict(df_rsv, kari_regs)
    df_master["車検予約状況"] = df_master["登録番号"].map(_shaken_dict).fillna("予約なし")
    # 登録番号がnanの行は「─」にリセット
    _nan_mask = df_master["登録番号"].isin(["", "nan", "NaN", "None", "NULL"])
    df_master.loc[_nan_mask, "車検予約状況"] = "─"
else:
    df_master["車検予約状況"] = "─"

date_cols_exist = [f"最終取引日_{s}" for s in all_services if f"最終取引日_{s}" in df_master.columns]
if date_cols_exist:
    df_master["最終取引日"] = df_master[date_cols_exist].max(axis=1)
    df_master["最終取引月"] = df_master["最終取引日"].dt.to_period("M").astype(str)
else:
    df_master["最終取引日"] = pd.NaT
    df_master["最終取引月"] = ""

total = len(df_master)
rank_counts = df_master["ランク"].value_counts()
stores = sorted(df_master["入庫店舗ID"].dropna().unique())


# ══════════════════════════════════════════════
# タブ
# ══════════════════════════════════════════════
tab0, tab1, tab2, tab3, tab4, tab5, tab6, tab_kpi = st.tabs([
    "📋 本日の接客指示",
    "📊 全体ダッシュボード",
    "📅 月別取引推移",
    "🔗 クロスセル進捗",
    "🏪 店舗別達成状況",
    "🏆 顧客ランク一覧",
    "📈 生涯顧客ストーリー",
    "🎯 営業KPI",
])


# ── TAB0：接客指示シート ──────────────────────
with tab0:
    st.markdown('<div class="sec">📋 来店予定顧客 × 接客指示シート</div>', unsafe_allow_html=True)
    st.caption("日付と店舗を選択すると、来店予定顧客ごとに「ランク・未取引サービス・提案アクション・車検満期」が自動表示されます。PDFでA4印刷も可能です。")

    today_dt = datetime.today()

    # ── 日付 + 店舗 フィルタ ──
    col_d1, col_d2 = st.columns([1, 1])
    with col_d1:
        selected_date = st.date_input(
            "📅 日付を選択",
            value=today_dt.date(),
            key="daily_date",
        )
    with col_d2:
        # 店舗一覧は予約データの入庫店舗IDから生成（実際の入庫店舗を優先）
        # 予約データがない場合はマスターデータの店舗一覧を使用
        if has_rsv and df_rsv is not None and "入庫店舗ID" in df_rsv.columns:
            rsv_stores = sorted(df_rsv["入庫店舗ID"].dropna().astype(str).unique().tolist())
            store_options = ["全店舗"] + rsv_stores
            store_caption = "※予約データの入庫店舗"
        else:
            store_options = ["全店舗"] + stores
            store_caption = "※マスターデータの拠点店舗"
        target_store = st.selectbox(
            f"🏪 店舗を選択（{store_caption}）",
            store_options,
            key="daily_store"
        )
    today_str = selected_date.strftime("%Y/%m/%d")

    if not has_rsv or df_rsv is None:
        st.warning("③ 予約データCSVをアップロードすると本日の接客指示が表示されます。")
    elif "予約日" not in df_rsv.columns:
        st.warning("予約データに「予約日」列が必要です。")
    else:
        # 本日の予約を抽出
        df_today = df_rsv[
            df_rsv["予約日"].dt.date == selected_date
        ].copy()

        # 店舗フィルタ：予約データの入庫店舗IDで絞り込む
        # ※マスターデータの車両拠点ではなく、実際に予約が入っている店舗で絞る
        if target_store != "全店舗" and "入庫店舗ID" in df_today.columns:
            df_today = df_today[df_today["入庫店舗ID"] == target_store]

        # マスター情報を結合
        # 予約データの「入庫店舗ID」（実際の入庫店舗）を優先するため
        # マスター側の「入庫店舗ID」は「拠点店舗ID」として取り込む
        if "登録番号" in df_today.columns and "登録番号" in df_master.columns:
            master_cols = ["登録番号", "顧客ID", "車両ID", "入庫店舗ID",
                           "ランク", "取引サービス数", "未取引サービス", "車検満了日",
                           "初年度登録"] + \
                          [f"取引_{s}" for s in all_services]
            master_cols = [c for c in master_cols if c in df_master.columns]
            df_today = df_today.merge(
                df_master[master_cols].rename(columns={"入庫店舗ID": "拠点店舗ID"}),
                on="登録番号", how="left"
            )
            # 予約データに入庫店舗IDがなければマスターの拠点店舗IDで補完
            if "入庫店舗ID" not in df_today.columns and "拠点店舗ID" in df_today.columns:
                df_today["入庫店舗ID"] = df_today["拠点店舗ID"]

        # 車検予約状況を辞書引きで高速付与（applyループを排除）
        if "登録番号" in df_today.columns and "_shaken_dict" in dir():
            df_today["車検予約状況_最新"] = df_today["登録番号"].astype(str).map(_shaken_dict).fillna("予約なし")
        elif "登録番号" in df_today.columns:
            _local_shaken = build_shaken_dict(df_rsv, kari_regs)
            df_today["車検予約状況_最新"] = df_today["登録番号"].astype(str).map(_local_shaken).fillna("予約なし")
        else:
            df_today["車検予約状況_最新"] = "─"

        # ── 今日来店なし ──
        if len(df_today) == 0:
            st.info(f"本日（{today_str}）の来店予定は0件です。\n\n予約データの「予約日」列が本日の日付になっているか確認してください。")

            # デモ用：予約データ全体から直近の日付で表示するか確認
            if len(df_rsv) > 0 and "予約日" in df_rsv.columns:
                latest_date = df_rsv["予約日"].dropna().dt.date.max()
                st.caption(f"💡 予約データの最新日付：{latest_date}　← この日付の来店予定を確認したい場合は下のボタンを押してください")
                if st.button("最新日付の来店予定を表示（デモ用）", key="demo_btn"):
                    df_today = df_rsv[df_rsv["予約日"].dt.date == latest_date].copy()
                    selected_date = latest_date
                    if "登録番号" in df_today.columns and "登録番号" in df_master.columns:
                        df_today = df_today.merge(
                            df_master[master_cols],
                            on="登録番号", how="left"
                        )

        # ── 来店予定あり ──
        if len(df_today) > 0:

            # ── 提案アクション生成ロジック ──
            def generate_action(row):
                """ランク・未取引サービス・車検満期からアクション指示を生成"""
                rank = row.get("ランク", "D")
                untreated = str(row.get("未取引サービス", ""))
                untreated_list = [s.strip() for s in untreated.split("、") if s.strip()]

                # 車検満期残日数
                exp = row.get("車検満了日", pd.NaT)
                days_to_exp = None
                if pd.notna(exp):
                    days_to_exp = (pd.Timestamp(exp) - pd.Timestamp(today_dt)).days

                actions = []

                # 経過年数を取得
                reg_date_action = row.get("初年度登録", None)
                elapsed_action = calc_elapsed(reg_date_action) if reg_col else None
                elapsed_years_action = elapsed_action[0] if elapsed_action else 0

                # ── 顧客ID起点ランクに応じた接客指示（確定版）──
                if rank == "SSS":
                    actions.append("🏆【VIP対応】担当者が直接挨拶。複数台・家族全体のカーライフをヒアリング")
                    actions.append("🚗 次の1台・家族の車の相談を自然な会話の中で確認する")
                    if elapsed_years_action >= 10:
                        actions.append(f"🚨 車齢{elapsed_years_action}年！乗り換えニーズ最高。積極的に次の1台を提案する")

                elif rank == "S":
                    actions.append("🥇【S対応】乗り換えまで獲得した重要顧客。2台目・家族の車検を確認する")
                    if days_to_exp is not None and 0 <= days_to_exp <= 180:
                        actions.append(f"🔴 車検満期まで{days_to_exp}日。次の乗り換えタイミングを相談する")
                    actions.append("💡 「次のオイル交換・12ヶ月点検もぜひ当店で」（来店習慣の強化）")
                    if elapsed_years_action >= 10:
                        actions.append(f"🚨 車齢{elapsed_years_action}年！次の乗り換え提案を今日切り出す")

                elif rank == "A":
                    actions.append("🥈【A対応】来店習慣が定着した顧客。次のステップは乗り換え提案")
                    if days_to_exp is not None and 0 <= days_to_exp <= 90:
                        actions.append(f"⚡ 車検満期まで{days_to_exp}日！本日必ず車検予約を取る")
                    actions.append("🚗 最重要提案：自動車販売「そろそろ乗り換えを考えていませんか？今なら〇〇がおすすめです」")
                    if elapsed_years_action >= 10:
                        actions.append(f"🚨 車齢{elapsed_years_action}年！乗り換え提案の絶好のタイミング")
                    elif elapsed_years_action >= 7:
                        actions.append(f"⚠️ 車齢{elapsed_years_action}年。乗り換えニーズを探る会話を始める")

                elif rank == "B":
                    if days_to_exp is not None and 0 <= days_to_exp <= 90:
                        actions.append(f"⚡ 車検満期まで{days_to_exp}日！本日必ず車検予約を取る")
                    actions.append("🥉【B対応】車検は定着。目標はオイル＋タイヤのセット化（来店頻度アップ）")
                    if "オイル交換" in untreated_list:
                        actions.append("💡 「次回オイル交換の予約を今日取りましょう。タイヤの確認も一緒に」")
                    elif "タイヤ交換" in untreated_list:
                        actions.append("💡 「タイヤの溝を確認しましたが、次の交換時期が近づいています」")
                    if elapsed_years_action >= 10:
                        actions.append(f"🚨 車齢{elapsed_years_action}年！乗り換え提案も視野に入れる")

                elif rank == "C":
                    actions.append("【C対応】車検未獲得が最大課題。信頼関係を築きながら必ず一声かける")
                    if days_to_exp is not None and 0 <= days_to_exp <= 90:
                        actions.append(f"⚡ 車検満期まで{days_to_exp}日！「次回の車検はぜひ当店で」と伝える")
                    else:
                        actions.append("🔑 「次回の車検はぜひ当店で。まとめてお願いいただくと手間が省けますよ」")
                    actions.append("👋 今日は売ろうとしない。名前を覚えて「顔を覚えている店」を演出する")

                else:  # D
                    actions.append("【D対応】まず接触頻度を上げることが最優先")
                    actions.append("📞 次回来店予約を今日取る（オイル交換・タイヤ点検などで理由をつくる）")
                    actions.append("👋 名前を呼んで前回の会話を思い出させる（「顔を覚えている店」を演出）")

                # 車検満期アラート（全ランク共通）
                if days_to_exp is not None:
                    if days_to_exp < 0:
                        actions.append(f"⚠️ 車検満期が{abs(days_to_exp)}日超過しています！早急に対応")
                    elif days_to_exp <= 30 and rank not in ["SSS", "S"]:
                        actions.append(f"🔴 車検満期まで{days_to_exp}日！本日予約必須")

                return " ／ ".join(actions) if actions else "通常対応"

            def rank_badge(rank):
                colors = {
                    "SSS": "#7c2d12",  # 最上位・濃い赤茶
                    "S"  : "#d97706",  # 金
                    "A"  : "#2563eb",  # 青
                    "B"  : "#059669",  # 緑
                    "C"  : "#64748b",  # グレー
                    "D"  : "#94a3b8",  # 薄グレー
                }
                bg = colors.get(rank, "#94a3b8")
                label = "SSSランク" if rank == "SSS" else f"{rank}ランク"
                return f'<span style="background:{bg};color:white;padding:2px 10px;border-radius:10px;font-weight:900;font-size:0.82rem;">{label}</span>'

            def days_badge(days):
                try:
                    if days is None:
                        return "─"
                    import math
                    if isinstance(days, float) and math.isnan(days):
                        return "─"
                    days = int(float(days))
                except (ValueError, TypeError):
                    return "─"
                if days < 0:
                    return f'<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:8px;font-weight:700;">超過{abs(days)}日</span>'
                elif days <= 30:
                    return f'<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:8px;font-weight:700;">{days}日後</span>'
                elif days <= 90:
                    return f'<span style="background:#fffbeb;color:#d97706;padding:2px 8px;border-radius:8px;font-weight:700;">{days}日後</span>'
                else:
                    return f'<span style="background:#f0fdf4;color:#059669;padding:2px 8px;border-radius:8px;">{days}日後</span>'

            # 車検残日数を計算
            if "車検満了日" in df_today.columns:
                df_today["車検残日数"] = df_today["車検満了日"].apply(
                    lambda d: (pd.Timestamp(d) - pd.Timestamp(today_dt)).days
                    if pd.notna(d) else None
                )
            else:
                df_today["車検残日数"] = None

            # 経過年数を計算（初年度登録列から）
            def calc_elapsed(reg_date):
                """初年度登録日から今日までの経過年数・月数を返す"""
                if pd.isna(reg_date):
                    return None
                try:
                    reg = pd.Timestamp(reg_date)
                    today_ts = pd.Timestamp(today_dt)
                    years = today_ts.year - reg.year
                    months = today_ts.month - reg.month
                    if months < 0:
                        years -= 1
                        months += 12
                    return (years, months)
                except Exception:
                    return None

            def elapsed_badge(reg_date):
                """経過年数をバッジHTML形式で返す（10年超は赤）"""
                result = calc_elapsed(reg_date)
                if result is None:
                    return "─"
                years, months = result
                label = f"{years}年{months}ヶ月"
                if years >= 10:
                    return f'<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:8px;font-weight:700;font-size:0.82rem;">🚨 {label}</span>'
                elif years >= 7:
                    return f'<span style="background:#fffbeb;color:#d97706;padding:2px 8px;border-radius:8px;font-weight:600;font-size:0.82rem;">{label}</span>'
                else:
                    return f'<span style="background:#f0fdf4;color:#059669;padding:2px 8px;border-radius:8px;font-size:0.82rem;">{label}</span>'

            reg_col = "初年度登録" if "初年度登録" in df_today.columns else None
            if reg_col:
                df_today["経過年数_tuple"] = df_today[reg_col].apply(calc_elapsed)
                df_today["経過年数_years"] = df_today["経過年数_tuple"].apply(
                    lambda x: x[0] if x else None)

            # アクション生成
            df_today["接客指示"] = df_today.apply(generate_action, axis=1)

            # ── KPIサマリー ──
            n_today = len(df_today)
            n_s = (df_today.get("ランク", pd.Series()) == "S").sum()
            n_ab = df_today.get("ランク", pd.Series()).isin(["A", "B"]).sum()
            n_urgent = (df_today["車検残日数"].fillna(999) <= 90).sum() if "車検残日数" in df_today.columns else 0

            st.markdown(f"### 📅 {today_str}（{target_store}）の来店予定：**{n_today}件**")
            k1, k2, k3, k4 = st.columns(4)
            for col, (lbl, val, cls) in zip([k1, k2, k3, k4], [
                ("本日 来店件数",      f"{n_today}件",  ""),
                ("Sランク（要特別対応）", f"{n_s}件",    "gold"),
                ("A+Bランク（提案対象）", f"{n_ab}件",   "purple"),
                ("車検90日以内（要予約）", f"{n_urgent}件", "red"),
            ]):
                with col:
                    st.markdown(f"""<div class="kpi-card {cls}">
                        <div class="lbl">{lbl}</div>
                        <div class="val">{val}</div>
                    </div>""", unsafe_allow_html=True)

            st.markdown("")

            # ── Sランク顧客を最上部に強調表示 ──
            df_s_today = df_today[df_today.get("ランク", pd.Series("D", index=df_today.index)) == "S"]
            if len(df_s_today) > 0:
                st.markdown('<div class="sec" style="border-left-color:#d97706;">🌟 Sランク顧客（特別対応必須）</div>', unsafe_allow_html=True)
                for _, row in df_s_today.iterrows():
                    exp_days = row.get("車検残日数", None)
                    # 入庫店舗（予約由来）と拠点店舗（マスター由来）を両方表示
                    nyuko_store = row.get('入庫店舗ID', '')
                    kiten_store = row.get('拠点店舗ID', '')
                    store_disp = nyuko_store
                    if kiten_store and kiten_store != nyuko_store:
                        store_disp = f"{nyuko_store} <span style='color:#aaa;font-size:0.75rem;'>（拠点:{kiten_store}）</span>"
                    st.markdown(f"""
<div style="background:linear-gradient(135deg,#fffbeb,#fef3c7);border:2px solid #d97706;border-radius:12px;padding:1rem 1.4rem;margin:0.5rem 0;">
<div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:0.5rem;">
  {rank_badge("S")}
  <strong style="font-size:1rem;">{row.get('登録番号','─')}</strong>
  <span style="color:#888;font-size:0.82rem;">{store_disp}</span>
  <span style="margin-left:auto;font-size:0.82rem;">車検満期：{days_badge(exp_days)}</span>
</div>
<div style="font-size:0.85rem;color:#444;margin-bottom:0.4rem;">
  <strong>本日のサービス：</strong>{row.get('サービス種別','─')}　
  <strong>未取引：</strong>{row.get('未取引サービス','─')}　
  <strong>経過年数：</strong>{elapsed_badge(row.get('初年度登録')) if reg_col else '─'}
</div>
<div style="background:#fff8e1;border-radius:8px;padding:0.5rem 0.8rem;font-size:0.85rem;color:#92400e;font-weight:600;">
  📌 接客指示：{row.get('接客指示','通常対応')}
</div>
</div>
""", unsafe_allow_html=True)

            # ── 全来店予定一覧 ──
            st.markdown('<div class="sec">本日 全来店予定一覧（予約時刻順）</div>', unsafe_allow_html=True)

            # 時刻順にソート → 同時刻内はランク順
            rank_order_map = {"SSS": 0, "S": 1, "A": 2, "B": 3, "C": 4, "D": 5}
            if "ランク" in df_today.columns:
                df_today["ランク順"] = df_today["ランク"].map(rank_order_map).fillna(5)
            else:
                df_today["ランク順"] = 5

            # 予約時刻列があれば時刻でソート、なければランク順
            if "予約時刻" in df_today.columns:
                df_today = df_today.sort_values(
                    ["予約時刻", "ランク順", "車検残日数"],
                    ascending=[True, True, True],
                    na_position="last"
                )
            else:
                df_today = df_today.sort_values(
                    ["ランク順", "車検残日数"], ascending=[True, True]
                )

            for _, row in df_today.iterrows():
                rank = row.get("ランク", "─")
                exp_days = row.get("車検残日数", None)

                # カードの背景色をランク別に
                bg_color = {
                    "S": "linear-gradient(135deg,#fffbeb,#fef3c7)",
                    "A": "linear-gradient(135deg,#eff6ff,#dbeafe)",
                    "B": "linear-gradient(135deg,#f0fdf4,#dcfce7)",
                    "C": "#f8fafc",
                    "D": "#f8fafc",
                }.get(rank, "#f8fafc")

                border_color = {
                    "S": "#d97706", "A": "#2563eb", "B": "#059669",
                    "C": "#94a3b8", "D": "#e2e8f0"
                }.get(rank, "#e2e8f0")

                # 予約時刻の取得
                rsv_time = row.get("予約時刻", "")
                time_badge = (
                    f'<span style="background:#1e293b;color:#e2e8f0;padding:2px 8px;'
                    f'border-radius:6px;font-weight:700;font-size:0.82rem;">🕐 {rsv_time}</span>'
                    if rsv_time else ""
                )

                # 入庫店舗（予約由来）と拠点店舗（マスター由来）を両方表示
                nyuko = row.get('入庫店舗ID', '')
                kiten = row.get('拠点店舗ID', '')
                store_txt = nyuko
                if kiten and kiten != nyuko and str(kiten) != 'nan':
                    store_txt = f"{nyuko}（拠点:{kiten}）"

                # 追加情報
                customer_name = str(row.get('顧客名', '')).strip()
                customer_name = '' if customer_name == 'nan' else customer_name
                car_model = str(row.get('車種', '')).strip()
                car_model = '' if car_model == 'nan' else car_model
                備考_disp = str(row.get('備考', '')).strip()
                備考_disp = '' if 備考_disp == 'nan' else 備考_disp
                app_id_disp = str(row.get('アプリID', '')).strip()
                app_id_disp = '' if app_id_disp == 'nan' else app_id_disp
                app_badge = (
                    '<span style="background:#7c3aed;color:white;padding:1px 7px;border-radius:8px;font-size:0.75rem;font-weight:700;">アプリ会員</span>'
                    if app_id_disp else
                    '<span style="background:#e2e8f0;color:#64748b;padding:1px 7px;border-radius:8px;font-size:0.75rem;">アプリ未登録</span>'
                )

                # 車検予約状況バッジ
                shaken_status = str(row.get('車検予約状況_最新', '─'))
                shaken_colors = {
                    "車検予約済":  ("background:#dcfce7;color:#166534", "✅ 車検予約済"),
                    "仮予約あり":  ("background:#fef9c3;color:#854d0e", "🔶 車検仮予約"),
                    "予約なし":    ("background:#fee2e2;color:#991b1b", "❌ 車検予約なし"),
                    "相談中":      ("background:#e0f2fe;color:#075985", "💬 車検相談中"),
                }
                sh_style, sh_label = shaken_colors.get(shaken_status, ("background:#f1f5f9;color:#475569", shaken_status))

                st.markdown(f"""
<div style="background:{bg_color};border:1.5px solid {border_color};border-radius:10px;padding:0.85rem 1.2rem;margin:0.35rem 0;">
<div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:0.35rem;flex-wrap:wrap;">
  {rank_badge(rank)}
  {time_badge}
  <strong>{row.get('登録番号','─')}</strong>
  {'<span style="font-weight:700;font-size:0.88rem;">'+customer_name+'</span>' if customer_name else ''}
  {'<span style="color:#555;font-size:0.8rem;">'+car_model+'</span>' if car_model else ''}
  <span style="color:#888;font-size:0.8rem;">{store_txt}</span>
  <span style="margin-left:auto;">{app_badge}</span>
</div>
<div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:0.3rem;flex-wrap:wrap;">
  <span style="font-size:0.8rem;color:#555;">本日：<strong>{row.get('サービス種別','─')}</strong></span>
  <span style="{sh_style};padding:2px 9px;border-radius:8px;font-size:0.78rem;font-weight:700;">{sh_label}</span>
  <span style="font-size:0.8rem;">経過：{elapsed_badge(row.get('初年度登録')) if reg_col else '─'}</span>
  <span style="margin-left:auto;font-size:0.8rem;">車検満期：{days_badge(exp_days)}</span>
</div>
<div style="font-size:0.8rem;color:#666;margin-bottom:0.3rem;">
  未取引サービス：<span style="color:#dc2626;font-weight:600;">{row.get('未取引サービス','─')}</span>
</div>
{'<div style="font-size:0.78rem;color:#555;background:#f8fafc;border-radius:5px;padding:2px 8px;margin-bottom:0.25rem;">📝 '+備考_disp+'</div>' if 備考_disp else ''}
<div style="font-size:0.82rem;color:#1e3a5f;font-weight:600;background:rgba(255,255,255,0.7);border-radius:6px;padding:0.3rem 0.6rem;">
  📌 {row.get('接客指示','通常対応')}
</div>
</div>
""", unsafe_allow_html=True)

                # ── 前回提案履歴の表示 ──
                cust_id_str = str(row.get("顧客ID", ""))
                veh_id_str  = str(row.get("車両ID", ""))
                prev_log = get_latest_log(cust_id_str, veh_id_str) if cust_id_str else {}
                if prev_log:
                    prev_phase = prev_log.get("フェーズ", "")
                    prev_item  = prev_log.get("提案項目", "")
                    prev_date  = prev_log.get("記録日", "")
                    prev_staff = prev_log.get("担当者", "")
                    is_followup = "興味あり" in prev_phase or "仮予約" in prev_phase
                    bg_prev = "#fef3c7" if is_followup else "#f0fdf4"
                    icon_prev = "⭐" if is_followup else "🗂️"
                    st.markdown(f"""
<div style="background:{bg_prev};border-radius:6px;padding:0.3rem 0.8rem;margin-top:0.3rem;font-size:0.78rem;">
  {icon_prev} <strong>前回提案({prev_date} 担当:{prev_staff})</strong>：
  {prev_item} → <span style="font-weight:700;">{prev_phase}</span>
  {'　<span style="color:#d97706;font-weight:700;">▶ 今回フォローアップを！</span>' if is_followup else ''}
</div>
""", unsafe_allow_html=True)

                # ── 顧客詳細情報（PC画面で確認できるよう全表示）──
                with st.expander("🔍 顧客詳細情報", expanded=False):
                    d1, d2, d3 = st.columns(3)
                    with d1:
                        st.markdown(f"**顧客ID：** {cust_id_str}")
                        st.markdown(f"**車両ID：** {veh_id_str}")
                        st.markdown(f"**登録番号：** {row.get('登録番号','─')}")
                        st.markdown(f"**ランク：** {rank}")
                        st.markdown(f"**車検残日数：** {exp_days}日" if exp_days else "**車検残日数：** ─")
                    with d2:
                        st.markdown(f"**取引サービス数：** {row.get('取引サービス数','─')}")
                        st.markdown(f"**経過年数：** {str(row.get('経過年数_tuple','─'))[:10]}")
                        st.markdown(f"**車検満了日：** {str(row.get('車検満了日','─'))[:10]}")
                        st.markdown(f"**初年度登録：** {str(row.get('初年度登録','─'))[:10]}")
                    with d3:
                        st.markdown(f"**入庫店舗：** {nyuko}")
                        st.markdown(f"**車検予約：** {shaken_status}")
                    # 取引済みサービス一覧
                    st.markdown("**取引済みサービス：**")
                    svc_marks = []
                    for svc in all_services:
                        col_k = f"取引_{svc}"
                        if col_k in row.index:
                            try:
                                v = int(float(row[col_k]))
                                svc_marks.append(f"{'✅' if v else '─'} {svc}")
                            except:
                                svc_marks.append(f"─ {svc}")
                    if svc_marks:
                        sc1, sc2, sc3 = st.columns(3)
                        for si, sm in enumerate(svc_marks):
                            [sc1, sc2, sc3][si % 3].markdown(sm)
                    st.markdown(f"**未取引サービス：** {row.get('未取引サービス','─')}")
                    st.markdown(f"**接客指示：** {row.get('接客指示','通常対応')}")

                # ── 活動記録ボタン ──
                act_key = f"act_{veh_id_str}_{_}"
                with st.expander("📝 活動記録", expanded=False):
                    staff_list_card = load_staff_master()

                    # フルナンバー入力（登録番号が不完全な場合）
                    reg_no_display = str(row.get("登録番号", "")).strip()
                    is_incomplete = len(reg_no_display.replace(" ","")) < 6
                    if is_incomplete:
                        st.warning("⚠️ 登録番号が不完全です。フルナンバーを入力してください。")
                    full_plate_input = st.text_input(
                        "登録番号（フルナンバー）",
                        value=reg_no_display if not is_incomplete else "",
                        key=f"plate_{act_key}",
                        placeholder="例：千葉480て9579",
                        help="全角・半角・スペースありでも自動正規化します"
                    )

                    col_s1, col_s2 = st.columns([1, 2])
                    with col_s1:
                        # 予測変換（部分一致フィルタ）
                        staff_input = st.text_input("担当者", key=f"staff_{act_key}",
                                                     placeholder="名前を入力")
                        filtered_staff = [s for s in staff_list_card
                                          if staff_input.lower() in s.lower()] if staff_input else staff_list_card
                        staff_sel = st.selectbox("候補", filtered_staff or ["─"],
                                                  key=f"staff_sel_{act_key}")
                    with col_s2:
                        # 車検フェーズ（車検未予約の場合のみ）
                        shaken_status_now = str(row.get("車検予約状況_最新", "予約なし"))
                        if shaken_status_now not in ["車検予約済"]:
                            st.markdown("**🔑 車検提案フェーズ**")
                            shaken_phase_sel = st.radio(
                                "車検",
                                options=list(SHAKEN_PHASES.values()),
                                index=0,
                                key=f"shaken_{act_key}",
                                horizontal=True,
                            )
                        else:
                            shaken_phase_sel = "本予約 ✅"

                    # サービス別フェーズ（未取引サービスのみ表示）
                    untreated_svcs = [s.strip() for s in
                                      str(row.get("未取引サービス","")).split("、") if s.strip()]
                    svc_phases = {}
                    if untreated_svcs:
                        st.markdown("**💡 サービス提案フェーズ**")
                        svc_cols = st.columns(min(len(untreated_svcs), 3))
                        for si, svc in enumerate(untreated_svcs):
                            if svc == "車検":
                                continue
                            with svc_cols[si % 3]:
                                svc_phases[svc] = st.radio(
                                    svc,
                                    options=list(SERVICE_PHASES.values()),
                                    index=0,
                                    key=f"svc_{act_key}_{si}",
                                )

                    memo_input = st.text_input("備考（任意）", key=f"memo_{act_key}",
                                               placeholder="次回への申し送り等")

                    # 保存済みフラグ（セッション内で管理・rerun不要）
                    saved_key = f"saved_{act_key}"
                    if st.session_state.get(saved_key):
                        st.success("✅ 保存済み")
                    elif st.button("💾 保存", key=f"save_{act_key}", type="primary",
                                   use_container_width=True):
                        today_log_date = datetime.today().strftime("%Y-%m-%d")
                        staff_name = staff_sel if staff_sel != "─" else staff_input
                        shaken_expiry = str(row.get("車検満了日", ""))

                        # 登録番号を正規化（フルナンバー入力対応）
                        reg_no_raw = str(row.get("登録番号", ""))
                        reg_no_save = normalize_plate(full_plate_input) if full_plate_input.strip() else normalize_plate(reg_no_raw)

                        saved_count = 0

                        # 車検ログ保存
                        if shaken_status_now not in ["車検予約済"]:
                            ok = save_activity_log({
                                "log_id"   : str(uuid.uuid4())[:8],
                                "記録日"   : today_log_date,
                                "顧客ID"   : cust_id_str,
                                "車両ID"   : veh_id_str,
                                "登録番号" : reg_no_save,
                                "担当者"   : staff_name,
                                "店舗"     : str(row.get("入庫店舗ID", "")),
                                "ランク"   : str(row.get("ランク", "")),
                                "提案項目" : "車検",
                                "フェーズ" : shaken_phase_sel,
                                "車検満了日": shaken_expiry,
                                "備考"     : memo_input,
                            })
                            if ok: saved_count += 1

                        # サービス別ログ保存
                        for svc, phase in svc_phases.items():
                            if phase != "─ 未記録":
                                ok = save_activity_log({
                                    "log_id"   : str(uuid.uuid4())[:8],
                                    "記録日"   : today_log_date,
                                    "顧客ID"   : cust_id_str,
                                    "車両ID"   : veh_id_str,
                                    "登録番号" : reg_no_save,
                                    "担当者"   : staff_name,
                                    "店舗"     : str(row.get("入庫店舗ID", "")),
                                    "ランク"   : str(row.get("ランク", "")),
                                    "提案項目" : svc,
                                    "フェーズ" : phase,
                                    "車検満了日": "",
                                    "備考"     : memo_input,
                                })
                                if ok: saved_count += 1

                        if saved_count > 0:
                            # 保存成功：セッションにフラグを立てるだけ（rerun不要）
                            st.session_state[saved_key] = True
                            st.success(f"✅ {saved_count}件をNotionに保存しました")
                        else:
                            st.error("保存に失敗しました。NOTION_TOKENを確認してください。")

            # ── CSV ダウンロード ──
            st.markdown("")
            dl_cols = ["予約時刻", "登録番号", "サービス種別", "予約ステータス", "入庫店舗ID",
                       "ランク", "取引サービス数", "未取引サービス", "車検残日数", "経過年数_years", "接客指示"]
            dl_cols_exist = [c for c in dl_cols if c in df_today.columns]

            btn_col1, btn_col2 = st.columns(2)
            if len(dl_cols_exist) > 0:
                with btn_col1:
                    st.download_button(
                        "📥 CSVダウンロード",
                        to_csv(df_today[dl_cols_exist]),
                        f"daily_action_{today_str.replace('/','')}.csv",
                        "text/csv",
                        use_container_width=True,
                    )

            # ── PDF生成 ──
            with btn_col2:
                if st.button("🖨️ A4接客指示シートをPDF出力", use_container_width=True, key="pdf_btn"):
                    try:
                        import io
                        from reportlab.lib.pagesizes import A4
                        from reportlab.lib import colors
                        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                        from reportlab.lib.units import mm
                        from reportlab.platypus import (
                            SimpleDocTemplate, Paragraph, Spacer, Table,
                            TableStyle, HRFlowable, KeepTogether
                        )
                        from reportlab.pdfbase import pdfmetrics
                        from reportlab.pdfbase.cidfonts import UnicodeCIDFont

                        # 日本語フォント登録
                        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiKakuGo-W5'))
                        FONT = 'HeiseiKakuGo-W5'

                        pdf_buf = io.BytesIO()
                        doc = SimpleDocTemplate(
                            pdf_buf,
                            pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm,
                        )

                        # スタイル定義
                        RANK_BG = {
                            "SSS": colors.HexColor("#fef2f2"),
                            "S": colors.HexColor("#fef3c7"),
                            "A": colors.HexColor("#dbeafe"),
                            "B": colors.HexColor("#dcfce7"),
                            "C": colors.HexColor("#f1f5f9"),
                            "D": colors.HexColor("#f8fafc"),
                        }
                        RANK_FG = {
                            "SSS": colors.HexColor("#7c2d12"),
                            "S": colors.HexColor("#92400e"),
                            "A": colors.HexColor("#1e40af"),
                            "B": colors.HexColor("#065f46"),
                            "C": colors.HexColor("#475569"),
                            "D": colors.HexColor("#94a3b8"),
                        }

                        def ps(name, size, bold=False, color=colors.black, leading=None):
                            return ParagraphStyle(
                                name,
                                fontName=FONT,
                                fontSize=size,
                                textColor=color,
                                leading=leading or size * 1.4,
                                spaceAfter=0,
                            )

                        story = []

                        # ── 表紙ヘッダー ──
                        story.append(Paragraph(
                            f"接客指示シート　{today_str}　{target_store}",
                            ps("title", 14, bold=True)
                        ))
                        story.append(Spacer(1, 3*mm))
                        story.append(HRFlowable(width="100%", thickness=1.5,
                                                color=colors.HexColor("#1a56db")))
                        story.append(Spacer(1, 3*mm))

                        # ── サマリー行 ──
                        n_s_pdf  = (df_today.get("ランク", pd.Series()) == "S").sum()
                        n_ab_pdf = df_today.get("ランク", pd.Series()).isin(["A","B"]).sum()
                        n_urg_pdf = (df_today["車検残日数"].fillna(999) <= 90).sum() if "車検残日数" in df_today.columns else 0

                        summary_data = [
                            ["来店件数", "Sランク", "A+Bランク", "車検90日以内"],
                            [str(len(df_today)), str(n_s_pdf), str(n_ab_pdf), str(n_urg_pdf)],
                        ]
                        summary_tbl = Table(summary_data, colWidths=[43*mm]*4)
                        summary_tbl.setStyle(TableStyle([
                            ("FONTNAME",    (0,0), (-1,-1), FONT),
                            ("FONTSIZE",    (0,0), (-1,0),  8),
                            ("FONTSIZE",    (0,1), (-1,1), 14),
                            ("ALIGN",       (0,0), (-1,-1), "CENTER"),
                            ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
                            ("BACKGROUND",  (0,0), (-1,0), colors.HexColor("#1a56db")),
                            ("TEXTCOLOR",   (0,0), (-1,0), colors.white),
                            ("BACKGROUND",  (0,1), (-1,1), colors.HexColor("#f0f4ff")),
                            ("BOX",         (0,0), (-1,-1), 0.5, colors.HexColor("#c7d2fe")),
                            ("INNERGRID",   (0,0), (-1,-1), 0.3, colors.HexColor("#c7d2fe")),
                            ("ROWBACKGROUNDS", (0,0), (-1,-1), [None]),
                            ("TOPPADDING",  (0,0), (-1,-1), 3),
                            ("BOTTOMPADDING",(0,0),(-1,-1), 3),
                        ]))
                        story.append(summary_tbl)
                        story.append(Spacer(1, 5*mm))

                        # ── 顧客カード（ランク順） ──
                        df_pdf = df_today.copy()
                        rank_order_pdf = {"S":0,"A":1,"B":2,"C":3,"D":4}
                        if "ランク" in df_pdf.columns:
                            df_pdf["_ord"] = df_pdf["ランク"].map(rank_order_pdf).fillna(5)
                        else:
                            df_pdf["_ord"] = 5
                        # 時刻順 → 同時刻内はランク順
                        if "予約時刻" in df_pdf.columns:
                            df_pdf = df_pdf.sort_values(
                                ["予約時刻", "_ord", "車検残日数"],
                                ascending=[True, True, True], na_position="last"
                            )
                        else:
                            df_pdf = df_pdf.sort_values(["_ord", "車検残日数"], ascending=[True, True])

                        for _, row in df_pdf.iterrows():
                            rank    = str(row.get("ランク", "─"))
                            reg_no  = str(row.get("登録番号", "─"))
                            svc_today = str(row.get("サービス種別", "─"))
                            rsv_time_pdf = str(row.get("予約時刻", ""))
                            store_name= str(row.get("入庫店舗ID", ""))
                            untreated = str(row.get("未取引サービス", "─"))
                            action    = str(row.get("接客指示", "通常対応"))
                            exp_days_raw = row.get("車検残日数", None)

                            # 車検残日数テキスト（NaN・None・非数値を安全に処理）
                            try:
                                if exp_days_raw is None:
                                    raise ValueError
                                import math
                                if isinstance(exp_days_raw, float) and math.isnan(exp_days_raw):
                                    raise ValueError
                                exp_days = int(float(exp_days_raw))
                                if exp_days < 0:
                                    exp_txt = f"超過{abs(exp_days)}日"
                                else:
                                    exp_txt = f"{exp_days}日後"
                            except (ValueError, TypeError):
                                exp_txt = "─"

                            # 取引済み・未取引サービスを◯/─で表示（NaN対応）
                            svc_marks = []
                            for svc in all_services:
                                col_key = f"取引_{svc}"
                                if col_key in row.index:
                                    try:
                                        val = row[col_key]
                                        if val is None or (isinstance(val, float) and math.isnan(val)):
                                            mark = "─"
                                        else:
                                            mark = "◯" if int(float(val)) == 1 else "─"
                                    except (ValueError, TypeError):
                                        mark = "─"
                                    svc_marks.append(f"{svc}:{mark}")
                            svc_status_txt = "　".join(svc_marks) if svc_marks else "─"

                            bg  = RANK_BG.get(rank, colors.white)
                            fg  = RANK_FG.get(rank, colors.black)

                            # 追加情報の取得
                            customer_name_pdf = str(row.get("顧客名", "")).strip()
                            customer_name_pdf = "" if customer_name_pdf == "nan" else customer_name_pdf
                            car_model_pdf = str(row.get("車種", "")).strip()
                            car_model_pdf = "" if car_model_pdf == "nan" else car_model_pdf
                            biko_pdf = str(row.get("備考", "")).strip()
                            biko_pdf = "" if biko_pdf == "nan" else biko_pdf
                            app_id_pdf = str(row.get("アプリID", "")).strip()
                            app_id_pdf = "" if app_id_pdf == "nan" else app_id_pdf
                            app_txt = f"[アプリ:{app_id_pdf}]" if app_id_pdf else "[アプリ未登録]"

                            # 車検予約状況
                            shaken_st = str(row.get("車検予約状況_最新", "─"))
                            shaken_label_map = {
                                "車検予約済": "✅車検予約済",
                                "仮予約あり": "△車検仮予約",
                                "予約なし":   "×車検予約なし",
                                "相談中":     "?車検相談中",
                            }
                            shaken_label_pdf = shaken_label_map.get(shaken_st, shaken_st)
                            shaken_color_pdf = {
                                "車検予約済": colors.HexColor("#166534"),
                                "仮予約あり": colors.HexColor("#854d0e"),
                                "予約なし":   colors.HexColor("#991b1b"),
                                "相談中":     colors.HexColor("#075985"),
                            }.get(shaken_st, colors.HexColor("#475569"))

                            # 経過年数テキスト（PDF用）
                            reg_date_pdf = row.get("初年度登録", None)
                            elapsed_pdf = calc_elapsed(reg_date_pdf) if reg_col else None
                            if elapsed_pdf:
                                years_p, months_p = elapsed_pdf
                                elapsed_txt = f"{years_p}年{months_p}ヶ月"
                                elapsed_color_pdf = colors.HexColor("#dc2626") if years_p >= 10 else colors.HexColor("#475569")
                                elapsed_alert = "🚨" if years_p >= 10 else ""
                            else:
                                elapsed_txt = "─"
                                elapsed_color_pdf = colors.HexColor("#475569")
                                elapsed_alert = ""

                            # 時刻表示（あれば）
                            time_txt = f"[{rsv_time_pdf}] " if rsv_time_pdf else ""
                            name_car = f"{customer_name_pdf}　{car_model_pdf}".strip("　")

                            header_data = [[
                                Paragraph(f"{rank}ランク", ParagraphStyle("rk", fontName=FONT, fontSize=9, textColor=fg, leading=12)),
                                Paragraph(f"{time_txt}{reg_no}", ParagraphStyle("hd", fontName=FONT, fontSize=9, leading=12)),
                                Paragraph(name_car or store_name, ParagraphStyle("nm", fontName=FONT, fontSize=8.5, leading=12)),
                                Paragraph(f"車検満期：{exp_txt}　経過：{elapsed_alert}{elapsed_txt}",
                                          ParagraphStyle("ex", fontName=FONT, fontSize=8, textColor=elapsed_color_pdf, leading=11)),
                            ]]
                            # 車検ステータス＋サービス行
                            status_svc_data = [[
                                Paragraph(
                                    shaken_label_pdf,
                                    ParagraphStyle("sh", fontName=FONT, fontSize=8, textColor=shaken_color_pdf, leading=11)
                                ),
                                Paragraph(
                                    f"本日：{svc_today}　{app_txt}",
                                    ParagraphStyle("st2", fontName=FONT, fontSize=7.5, textColor=colors.HexColor("#475569"), leading=11)
                                ),
                                Paragraph(
                                    f"店舗：{store_name}",
                                    ParagraphStyle("st3", fontName=FONT, fontSize=7.5, textColor=colors.HexColor("#475569"), leading=11)
                                ),
                                Paragraph("", ParagraphStyle("blank", fontName=FONT, fontSize=7, leading=10)),
                            ]]
                            # 取引状況行
                            svc_status_data = [[
                                Paragraph(
                                    f"取引状況：{svc_status_txt}",
                                    ParagraphStyle("ss", fontName=FONT, fontSize=7.5,
                                                   textColor=colors.HexColor("#334155"), leading=11)
                                ),
                            ]]
                            detail_data = [[
                                Paragraph(
                                    f"未取引：{untreated}",
                                    ParagraphStyle("un", fontName=FONT, fontSize=8,
                                                   textColor=colors.HexColor("#dc2626"), leading=11)
                                ),
                            ]]
                            # 備考行（あれば）
                            biko_data = []
                            if biko_pdf:
                                biko_data = [[
                                    Paragraph(
                                        f"備考：{biko_pdf[:80]}{'...' if len(biko_pdf)>80 else ''}",
                                        ParagraphStyle("bk", fontName=FONT, fontSize=7.5,
                                                       textColor=colors.HexColor("#475569"), leading=10)
                                    ),
                                ]]
                            action_data = [[
                                Paragraph(
                                    f"【接客指示】{action}",
                                    ParagraphStyle("ac", fontName=FONT, fontSize=8.5,
                                                   textColor=colors.HexColor("#1e3a5f"), leading=12)
                                ),
                            ]]

                            all_rows = header_data + status_svc_data + svc_status_data + detail_data + biko_data + action_data
                            n_rows = len(all_rows)

                            card = Table(
                                all_rows,
                                colWidths=[22*mm, 45*mm, 55*mm, 50*mm] if len(header_data[0])==4 else [172*mm],
                            )
                            style_cmds = [
                                ("FONTNAME",      (0,0), (-1,-1), FONT),
                                ("BACKGROUND",    (0,0), (-1,0), bg),
                                ("BACKGROUND",    (0,1), (-1,1), colors.HexColor("#f0fdf4")),
                                ("BACKGROUND",    (0,2), (-1,2), colors.HexColor("#f8fafc")),
                                ("BACKGROUND",    (0,3), (-1,3), colors.HexColor("#fff8f8")),
                                ("BACKGROUND",    (0, n_rows-1), (-1, n_rows-1), colors.HexColor("#eff6ff")),
                                ("BOX",           (0,0), (-1,-1), 0.8, fg),
                                ("INNERGRID",     (0,0), (-1,0), 0.3, colors.HexColor("#e2e8f0")),
                                ("LINEBELOW",     (0,0), (-1,0), 0.5, colors.HexColor("#e2e8f0")),
                                ("LINEBELOW",     (0,1), (-1,1), 0.5, colors.HexColor("#d1fae5")),
                                ("LINEBELOW",     (0,2), (-1,2), 0.5, colors.HexColor("#e2e8f0")),
                                ("LINEBELOW",     (0,3), (-1,3), 0.5, colors.HexColor("#fecaca")),
                                ("SPAN",          (0,2), (-1,2)),
                                ("SPAN",          (0,3), (-1,3)),
                                ("SPAN",          (0, n_rows-1), (-1, n_rows-1)),
                                ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
                                ("TOPPADDING",    (0,0), (-1,-1), 3),
                                ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                                ("LEFTPADDING",   (0,0), (-1,-1), 4),
                            ]
                            if biko_data:
                                style_cmds.append(("SPAN", (0, n_rows-2), (-1, n_rows-2)))
                            card.setStyle(TableStyle(style_cmds))
                            story.append(KeepTogether([card, Spacer(1, 2*mm)]))

                        # ── フッター ──
                        story.append(Spacer(1, 3*mm))
                        story.append(HRFlowable(width="100%", thickness=0.5,
                                                color=colors.HexColor("#94a3b8")))
                        story.append(Paragraph(
                            f"オートウェーブ 生涯顧客LTV管理システム　出力日時：{datetime.today().strftime('%Y/%m/%d %H:%M')}",
                            ps("foot", 7, color=colors.HexColor("#94a3b8"))
                        ))

                        doc.build(story)
                        pdf_bytes = pdf_buf.getvalue()

                        st.download_button(
                            label="📄 PDFをダウンロード（A4印刷用）",
                            data=pdf_bytes,
                            file_name=f"接客指示_{today_str.replace('/','')}{('_'+target_store) if target_store!='全店舗' else ''}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                        )
                        st.success("✅ PDF生成完了。上のボタンからダウンロードしてください。")

                    except ImportError:
                        st.error("PDF生成にはreportlabが必要です。コマンドプロンプトで以下を実行してください：\npip install reportlab")
                    except Exception as e:
                        st.error(f"PDF生成エラー: {e}")


# ── TAB1：全体ダッシュボード ──────────────────
with tab1:
    st.markdown('<div class="sec">📊 全社サマリー</div>', unsafe_allow_html=True)

    s_cnt  = rank_counts.get("S", 0)
    a_cnt  = rank_counts.get("A", 0)
    b_cnt  = rank_counts.get("B", 0)
    cd_cnt = rank_counts.get("C", 0) + rank_counts.get("D", 0)
    rsv_cnt = (df_master["予約件数"] > 0).sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    for col, (lbl, val, sub, cls) in zip([c1, c2, c3, c4, c5], [
        ("管理車両台数",         f"{total:,}台",       "CSVロード総数",                   ""),
        ("Sランク（生涯顧客）",  f"{s_cnt:,}台",       f"全体の{s_cnt/total*100:.1f}%",   "gold"),
        ("A+Bランク（育成層）",  f"{a_cnt+b_cnt:,}台", f"全体の{(a_cnt+b_cnt)/total*100:.1f}%", "purple"),
        ("C+Dランク（未育成）",  f"{cd_cnt:,}台",      f"全体の{cd_cnt/total*100:.1f}%",  "gray"),
        ("予約取得済み顧客",     f"{rsv_cnt:,}台",     f"全体の{rsv_cnt/total*100:.1f}%", "green"),
    ]):
        with col:
            st.markdown(f"""<div class="kpi-card {cls}">
                <div class="lbl">{lbl}</div><div class="val">{val}</div><div class="sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("")

    # ① ランク別台数グラフ
    st.markdown('<div class="sec">① ランク別台数構成 ─ Sランクが「生涯顧客」</div>', unsafe_allow_html=True)
    col_l, col_r = st.columns(2)
    with col_l:
        rnk_order = ["S", "A", "B", "C", "D"]
        rnk_df = pd.DataFrame({
            "ランク": rnk_order,
            "台数": [rank_counts.get(r, 0) for r in rnk_order],
        }).set_index("ランク")
        st.bar_chart(rnk_df, color="#1a56db")
    with col_r:
        rows = []
        for r in rnk_order:
            dr = df_master[df_master["ランク"] == r]
            if len(dr) == 0:
                continue
            rows.append({
                "ランク": r, "台数": len(dr),
                "割合": f"{len(dr)/total*100:.1f}%",
                "平均取引数": f"{dr['取引サービス数'].mean():.1f}",
                "予約取得率": f"{(dr['予約件数']>0).sum()/len(dr)*100:.1f}%",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.markdown('<div class="sec">サービス別 取引台数</div>', unsafe_allow_html=True)
    svc_chart = pd.DataFrame({
        "サービス": all_services,
        "取引台数": [df_master[f"取引_{s}"].sum() for s in all_services],
        "未取引台数": [total - df_master[f"取引_{s}"].sum() for s in all_services],
    }).set_index("サービス")
    st.bar_chart(svc_chart, color=["#1a56db", "#e2e8f0"])

    # 店舗別 管理台数
    st.markdown('<div class="sec">店舗別 管理車両台数</div>', unsafe_allow_html=True)

    store_summary = []
    for st_name in stores:
        ds = df_master[df_master["入庫店舗ID"] == st_name]
        n = len(ds)
        store_summary.append({
            "店舗":     st_name,
            "Sランク":  (ds["ランク"] == "S").sum(),
            "Aランク":  (ds["ランク"] == "A").sum(),
            "Bランク":  (ds["ランク"] == "B").sum(),
            "Cランク":  (ds["ランク"] == "C").sum(),
            "Dランク":  (ds["ランク"] == "D").sum(),
        })

    df_store_summary = pd.DataFrame(store_summary).set_index("店舗")

    # グラフ（ランク別に色分け積み上げ）
    st.bar_chart(
        df_store_summary,
        color=["#d97706", "#2563eb", "#059669", "#94a3b8", "#e2e8f0"],
    )

    # 店舗別 台数テーブル（KPI付き）
    tbl_rows = []
    for st_name in stores:
        ds = df_master[df_master["入庫店舗ID"] == st_name]
        n = len(ds)
        s_n = (ds["ランク"] == "S").sum()
        tbl_rows.append({
            "店舗":       st_name,
            "管理台数":   n,
            "Sランク":    s_n,
            "Aランク":    (ds["ランク"] == "A").sum(),
            "Bランク":    (ds["ランク"] == "B").sum(),
            "Cランク":    (ds["ランク"] == "C").sum(),
            "Dランク":    (ds["ランク"] == "D").sum(),
            "S率":        f"{s_n/n*100:.1f}%",
            "平均取引数": f"{ds['取引サービス数'].mean():.2f}",
        })
    st.dataframe(
        pd.DataFrame(tbl_rows),
        use_container_width=True,
        hide_index=True,
    )

    # ── ランク定義の説明（顧客ID起点・確定版）──
    st.markdown('<div class="sec">📖 ランク定義（顧客ID起点・確定版）</div>', unsafe_allow_html=True)
    rank_def_rows = [
        {"ランク": "🏆 SSS", "意味": "完全囲い込み", "判定条件": "複数台で車検取引あり かつ 自動車販売あり",
         "対応方針": "VIP対応。次の1台・家族の車を必ず確認。担当者固定"},
        {"ランク": "🥇 S",   "意味": "乗り換え獲得", "判定条件": "車検あり かつ 自動車販売あり",
         "対応方針": "2台目・家族の車検確認。来店習慣をさらに強化する"},
        {"ランク": "🥈 A",   "意味": "来店習慣定着", "判定条件": "車検あり かつ オイル交換あり かつ タイヤ交換あり",
         "対応方針": "乗り換え提案が最重要。車齢・走行距離を確認してタイミングを探る"},
        {"ランク": "🥉 B",   "意味": "車検定着",     "判定条件": "車検あり（A未満）",
         "対応方針": "オイル＋タイヤのセット化が目標。次回来店予約を今日取る"},
        {"ランク": "C",       "意味": "車検未獲得",   "判定条件": "車検なし・メンテ来店あり",
         "対応方針": "車検獲得が最優先。信頼関係を築きながら必ず一声かける"},
        {"ランク": "D",       "意味": "ほぼ未取引",   "判定条件": "メンテ来店もほぼなし",
         "対応方針": "次回来店予約を今日取る。まず接触頻度を上げることが最優先"},
    ]
    df_rank_def = pd.DataFrame(rank_def_rows)
    st.caption("※ ランクは顧客ID単位で判定します（同一顧客の複数台を合算）")
    st.dataframe(df_rank_def, use_container_width=True, hide_index=True)

    # ── ダッシュボードPDF出力 ──
    st.markdown('<div class="sec">🖨️ ダッシュボードPDF出力</div>', unsafe_allow_html=True)
    if st.button("📄 ダッシュボードをPDF出力（A4印刷用）", key="dash_pdf_btn", use_container_width=True):
        try:
            import io, math as _math
            from reportlab.lib.pagesizes import A4
            from reportlab.lib import colors as rl_colors
            from reportlab.lib.styles import ParagraphStyle
            from reportlab.lib.units import mm
            from reportlab.platypus import (
                SimpleDocTemplate, Paragraph, Spacer, Table,
                TableStyle, HRFlowable, KeepTogether
            )
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.cidfonts import UnicodeCIDFont
            pdfmetrics.registerFont(UnicodeCIDFont('HeiseiKakuGo-W5'))
            FONT = 'HeiseiKakuGo-W5'

            def _p(text, size=9, color=rl_colors.black, bold=False, leading=None):
                return Paragraph(str(text), ParagraphStyle(
                    'x', fontName=FONT, fontSize=size,
                    textColor=color, leading=leading or size*1.4
                ))

            pdf_buf = io.BytesIO()
            doc = SimpleDocTemplate(pdf_buf, pagesize=A4,
                leftMargin=15*mm, rightMargin=15*mm,
                topMargin=15*mm, bottomMargin=15*mm)
            story = []

            # タイトル
            story.append(_p(f"LTV進捗ダッシュボード　出力日：{datetime.today().strftime('%Y/%m/%d %H:%M')}",
                            size=13, color=rl_colors.HexColor("#0a1628")))
            story.append(Spacer(1, 2*mm))
            story.append(HRFlowable(width="100%", thickness=2, color=rl_colors.HexColor("#1a56db")))
            story.append(Spacer(1, 4*mm))

            # KPIサマリー
            s_cnt_pdf  = rank_counts.get("S", 0)
            a_cnt_pdf  = rank_counts.get("A", 0)
            b_cnt_pdf  = rank_counts.get("B", 0)
            cd_cnt_pdf = rank_counts.get("C", 0) + rank_counts.get("D", 0)
            rsv_cnt_pdf = (df_master["予約件数"] > 0).sum()

            kpi_data = [
                ["管理車両台数", "Sランク（生涯顧客）", "A+Bランク（育成層）", "C+Dランク（未育成）", "予約取得済"],
                [f"{total:,}台", f"{s_cnt_pdf:,}台", f"{a_cnt_pdf+b_cnt_pdf:,}台",
                 f"{cd_cnt_pdf:,}台", f"{rsv_cnt_pdf:,}台"],
                ["", f"{s_cnt_pdf/total*100:.1f}%", f"{(a_cnt_pdf+b_cnt_pdf)/total*100:.1f}%",
                 f"{cd_cnt_pdf/total*100:.1f}%", f"{rsv_cnt_pdf/total*100:.1f}%"],
            ]
            kpi_tbl = Table(kpi_data, colWidths=[35*mm]*5)
            kpi_tbl.setStyle(TableStyle([
                ("FONTNAME",    (0,0), (-1,-1), FONT),
                ("FONTSIZE",    (0,0), (-1,0), 8),
                ("FONTSIZE",    (0,1), (-1,1), 14),
                ("FONTSIZE",    (0,2), (-1,2), 8),
                ("ALIGN",       (0,0), (-1,-1), "CENTER"),
                ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
                ("BACKGROUND",  (0,0), (-1,0), rl_colors.HexColor("#1a56db")),
                ("TEXTCOLOR",   (0,0), (-1,0), rl_colors.white),
                ("BACKGROUND",  (1,1), (1,2), rl_colors.HexColor("#fef3c7")),
                ("BACKGROUND",  (2,1), (2,2), rl_colors.HexColor("#eff6ff")),
                ("BACKGROUND",  (4,1), (4,2), rl_colors.HexColor("#dcfce7")),
                ("BOX",         (0,0), (-1,-1), 0.5, rl_colors.HexColor("#c7d2fe")),
                ("INNERGRID",   (0,0), (-1,-1), 0.3, rl_colors.HexColor("#e2e8f0")),
                ("TOPPADDING",  (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ]))
            story.append(kpi_tbl)
            story.append(Spacer(1, 5*mm))

            # ランク別台数テーブル
            story.append(_p("■ ランク別 台数構成", size=10, color=rl_colors.HexColor("#0a1628")))
            story.append(Spacer(1, 2*mm))
            rank_colors_pdf = {"SSS":"#7c2d12","S":"#d97706","A":"#2563eb","B":"#059669","C":"#64748b","D":"#94a3b8"}
            rank_hdr = [_p("ランク",8), _p("台数",8), _p("割合",8),
                        _p("意味",8), _p("判定条件",8), _p("平均取引数",8), _p("予約取得率",8)]
            rank_rows_pdf = [rank_hdr]
            for r in ["SSS","S","A","B","C","D"]:
                dr = df_master[df_master["ランク"]==r]
                if len(dr) == 0: continue
                fg = rl_colors.HexColor(rank_colors_pdf.get(r, "#374151"))
                meanings = {
                    "SSS":"完全囲い込み","S":"乗り換え獲得",
                    "A":"来店習慣定着","B":"車検定着","C":"車検未獲得","D":"ほぼ未取引"}
                conds = {
                    "SSS":"複数台車検＋車販","S":"車検＋車販",
                    "A":"車検＋オイル＋タイヤ","B":"車検あり（A未満）",
                    "C":"車検なし・メンテ来店","D":"ほぼ未取引"}
                rank_rows_pdf.append([
                    _p(f"{r}ランク", 9, fg),
                    _p(f"{len(dr):,}人", 9),
                    _p(f"{len(dr)/total*100:.1f}%", 9),
                    _p(meanings.get(r,""), 8),
                    _p(conds.get(r,""), 8),
                    _p(f"{dr['取引サービス数'].mean():.1f}", 9),
                    _p(f"{(dr['予約件数']>0).sum()/len(dr)*100:.1f}%", 9),
                ])
            rank_tbl = Table(rank_rows_pdf,
                colWidths=[20*mm, 18*mm, 14*mm, 22*mm, 22*mm, 22*mm, 22*mm])
            rank_tbl.setStyle(TableStyle([
                ("FONTNAME",   (0,0),(-1,-1), FONT),
                ("BACKGROUND", (0,0),(-1,0), rl_colors.HexColor("#1a56db")),
                ("TEXTCOLOR",  (0,0),(-1,0), rl_colors.white),
                ("ALIGN",      (0,0),(-1,-1), "CENTER"),
                ("VALIGN",     (0,0),(-1,-1), "MIDDLE"),
                ("BOX",        (0,0),(-1,-1), 0.5, rl_colors.HexColor("#e2e8f0")),
                ("INNERGRID",  (0,0),(-1,-1), 0.3, rl_colors.HexColor("#e2e8f0")),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),
                 [rl_colors.HexColor("#fffbeb"), rl_colors.HexColor("#eff6ff"),
                  rl_colors.HexColor("#f0fdf4"), rl_colors.HexColor("#f8fafc"),
                  rl_colors.HexColor("#f8fafc")]),
                ("TOPPADDING", (0,0),(-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ]))
            story.append(rank_tbl)
            story.append(Spacer(1, 5*mm))

            # 店舗別台数テーブル
            story.append(_p("■ 店舗別 管理台数", size=10, color=rl_colors.HexColor("#0a1628")))
            story.append(Spacer(1, 2*mm))
            store_hdr = [_p(h,8) for h in ["店舗","管理台数","S","A","B","C","D","S率","平均取引数"]]
            store_rows_pdf = [store_hdr]
            for row in tbl_rows:
                store_rows_pdf.append([_p(str(row.get(k,"")), 8) for k in
                    ["店舗","管理台数","Sランク","Aランク","Bランク","Cランク","Dランク","S率","平均取引数"]])
            col_w = [30*mm, 18*mm, 12*mm, 12*mm, 12*mm, 12*mm, 12*mm, 14*mm, 18*mm]
            store_tbl = Table(store_rows_pdf, colWidths=col_w)
            store_tbl.setStyle(TableStyle([
                ("FONTNAME",   (0,0),(-1,-1), FONT),
                ("BACKGROUND", (0,0),(-1,0), rl_colors.HexColor("#1a56db")),
                ("TEXTCOLOR",  (0,0),(-1,0), rl_colors.white),
                ("ALIGN",      (0,0),(-1,-1), "CENTER"),
                ("VALIGN",     (0,0),(-1,-1), "MIDDLE"),
                ("BOX",        (0,0),(-1,-1), 0.5, rl_colors.HexColor("#e2e8f0")),
                ("INNERGRID",  (0,0),(-1,-1), 0.3, rl_colors.HexColor("#e2e8f0")),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),
                 [rl_colors.white, rl_colors.HexColor("#f8fafc")]),
                ("TOPPADDING", (0,0),(-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ]))
            story.append(store_tbl)
            story.append(Spacer(1, 5*mm))

            # サービス別取引台数テーブル
            story.append(_p("■ サービス別 取引台数", size=10, color=rl_colors.HexColor("#0a1628")))
            story.append(Spacer(1, 2*mm))
            svc_hdr = [_p(h,8) for h in ["サービス","取引台数","未取引台数","取引率","潜在売上推計"]]
            svc_rows_pdf = [svc_hdr]
            for s in all_services:
                done = int(df_master[f"取引_{s}"].sum())
                undone = total - done
                pot = undone * SERVICE_PRICE.get(s, 30000)
                svc_rows_pdf.append([
                    _p(s, 8), _p(f"{done:,}台", 8),
                    _p(f"{undone:,}台", 8, rl_colors.HexColor("#dc2626")),
                    _p(f"{done/total*100:.1f}%", 8),
                    _p(f"¥{pot/1e6:.1f}M", 8),
                ])
            svc_tbl = Table(svc_rows_pdf, colWidths=[35*mm, 25*mm, 25*mm, 20*mm, 35*mm])
            svc_tbl.setStyle(TableStyle([
                ("FONTNAME",   (0,0),(-1,-1), FONT),
                ("BACKGROUND", (0,0),(-1,0), rl_colors.HexColor("#1a56db")),
                ("TEXTCOLOR",  (0,0),(-1,0), rl_colors.white),
                ("ALIGN",      (0,0),(-1,-1), "CENTER"),
                ("VALIGN",     (0,0),(-1,-1), "MIDDLE"),
                ("BOX",        (0,0),(-1,-1), 0.5, rl_colors.HexColor("#e2e8f0")),
                ("INNERGRID",  (0,0),(-1,-1), 0.3, rl_colors.HexColor("#e2e8f0")),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),
                 [rl_colors.white, rl_colors.HexColor("#f8fafc")]),
                ("TOPPADDING", (0,0),(-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
            ]))
            story.append(svc_tbl)

            # フッター
            story.append(Spacer(1, 4*mm))
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=rl_colors.HexColor("#94a3b8")))
            story.append(_p(
                f"オートウェーブ 生涯顧客LTV管理システム　出力日時：{datetime.today().strftime('%Y/%m/%d %H:%M')}",
                size=7, color=rl_colors.HexColor("#94a3b8")
            ))

            doc.build(story)
            pdf_bytes = pdf_buf.getvalue()
            st.download_button(
                "📄 ダッシュボードPDFをダウンロード",
                pdf_bytes,
                f"LTVダッシュボード_{datetime.today().strftime('%Y%m%d')}.pdf",
                "application/pdf",
                use_container_width=True,
            )
            st.success("✅ PDF生成完了。上のボタンからダウンロードしてください。")

        except ImportError:
            st.error("PDF生成にはreportlabが必要です。\npip install reportlab を実行してください。")
        except Exception as e:
            st.error(f"PDF生成エラー: {e}")


# ── TAB2：月別取引推移 ────────────────────────
with tab2:
    st.markdown('<div class="sec">② 月別 取引件数推移</div>', unsafe_allow_html=True)
    st.caption("各サービスの「前回取引日」を月別に集計。取引件数の積み上がり推移を確認できます。")

    if not date_cols_exist:
        st.info("取引CSVに「前回取引日」列が含まれている場合に月別推移が表示されます。")
    else:
        monthly_rows = []
        for svc in all_services:
            dc = f"最終取引日_{svc}"
            if dc not in df_master.columns:
                continue
            tmp = df_master[df_master[dc].notna()][dc].dt.to_period("M").value_counts().sort_index()
            for period, cnt in tmp.items():
                monthly_rows.append({"月": str(period), "サービス": svc, "件数": cnt})

        if monthly_rows:
            df_monthly = pd.DataFrame(monthly_rows)
            df_monthly_pivot = df_monthly.pivot_table(
                index="月", columns="サービス", values="件数", aggfunc="sum", fill_value=0
            ).sort_index()

            months_all = df_monthly_pivot.index.tolist()
            if len(months_all) >= 2:
                col_m1, col_m2 = st.columns(2)
                with col_m1:
                    m_start = st.selectbox("開始月", months_all, index=max(0, len(months_all) - 13), key="ms")
                with col_m2:
                    m_end = st.selectbox("終了月", months_all, index=len(months_all) - 1, key="me")
                df_mf = df_monthly_pivot.loc[m_start:m_end]
            else:
                df_mf = df_monthly_pivot

            st.markdown('<div class="sec">サービス別 月次取引件数</div>', unsafe_allow_html=True)
            st.bar_chart(df_mf)

            st.markdown('<div class="sec">月別 全サービス合計</div>', unsafe_allow_html=True)
            st.bar_chart(df_mf.sum(axis=1).rename("合計取引件数"), color="#059669")

            # 直近3か月 vs 前3か月
            if len(months_all) >= 6:
                st.markdown('<div class="sec">直近3か月 vs 前3か月 比較</div>', unsafe_allow_html=True)
                recent3 = df_monthly_pivot.iloc[-3:].sum()
                prev3   = df_monthly_pivot.iloc[-6:-3].sum()
                cmp_df = pd.DataFrame({
                    "直近3か月": recent3, "前3か月": prev3,
                    "増減": recent3 - prev3,
                    "増減率": ((recent3 - prev3) / prev3.replace(0, np.nan) * 100).round(1).astype(str) + "%",
                }).sort_values("増減", ascending=False)
                st.dataframe(cmp_df, use_container_width=True)

            # 予約の月別推移（予約日ベース）
            if has_rsv and df_rsv is not None and "予約月" in df_rsv.columns:
                st.markdown('<div class="sec">予約件数 月別推移（予約日ベース）</div>', unsafe_allow_html=True)
                rsv_monthly = df_rsv.groupby("予約月").size().rename("予約件数")
                st.bar_chart(rsv_monthly, color="#7c3aed")
        else:
            st.info("月別データが取得できませんでした。取引CSVに「前回取引日」列を含めてください。")


# ── TAB3：クロスセル進捗 ──────────────────────
with tab3:
    st.markdown('<div class="sec">③ サービス別クロスセル進捗</div>', unsafe_allow_html=True)
    st.caption("縦：起点サービス ／ 横：クロスセル先 ／ 数値：両方取引している台数と取引率")

    cc1, cc2 = st.columns(2)
    with cc1:
        cs_rank = st.multiselect("対象ランク", ["S","A","B","C","D"], default=["S","A","B"], key="cs_rank")
    with cc2:
        cs_store = st.multiselect("対象店舗", stores, default=stores, key="cs_store")

    df_cs = df_master[
        (df_master["ランク"].isin(cs_rank)) &
        (df_master["入庫店舗ID"].isin(cs_store))
    ]

    if len(all_services) < 2:
        st.info("クロスセル分析には2種類以上の取引CSVが必要です。現在読み込まれているサービス：" + "、".join(all_services) if all_services else "なし")
    else:
        # クロスセルマトリクス：行インデックス＝起点、列＝提案先
        # 列名の重複を防ぐため unique なサービスリストを使用
        unique_svcs = list(dict.fromkeys(all_services))
        cross_rows = []
        row_labels = []
        for svc_a in unique_svcs:
            df_a = df_cs[df_cs[f"取引_{svc_a}"].astype(int) == 1]
            n_a = len(df_a)
            row = []
            for svc_b in unique_svcs:
                if svc_a == svc_b:
                    row.append("─")
                elif n_a == 0:
                    row.append("0台(0%)")
                else:
                    both = int((df_a[f"取引_{svc_b}"].astype(int) == 1).sum())
                    row.append(f"{both}台({both/n_a*100:.0f}%)")
            cross_rows.append(row)
            row_labels.append(f"{svc_a}({n_a}台)")

        df_cross = pd.DataFrame(cross_rows, index=row_labels, columns=unique_svcs)
        st.markdown("**数字が高い組み合わせ = クロスセル成功パターン。低い組み合わせ = 未開拓の機会**")
        st.dataframe(df_cross, use_container_width=True)

    # クロスセル機会ランキング
    st.markdown('<div class="sec">クロスセル機会ランキング（未取引台数の多い組み合わせ）</div>', unsafe_allow_html=True)
    if len(all_services) < 2:
        st.info("2種類以上の取引CSVが必要です。")
    else:
        opps = []
        for svc_a in all_services:
            df_a = df_master[df_master[f"取引_{svc_a}"].astype(int) == 1]
            for svc_b in all_services:
                if svc_a == svc_b:
                    continue
                untreated = int((df_a[f"取引_{svc_b}"].astype(int) == 0).sum())
                potential = untreated * SERVICE_PRICE.get(svc_b, 30000)
                opps.append({
                    "起点（取引済）": svc_a,
                    "提案（未取引）": svc_b,
                    "対象台数": untreated,
                    "潜在売上": f"¥{potential/1e6:.1f}M",
                })
        if opps:
            opp_df = pd.DataFrame(opps).sort_values("対象台数", ascending=False).head(15)
            st.dataframe(opp_df, use_container_width=True, hide_index=True)
        else:
            st.info("クロスセル機会データがありません。")

    # 予約との連携
    if has_rsv and df_rsv is not None and "サービス種別" in df_rsv.columns:
        st.markdown('<div class="sec">サービス別 予約取得状況（クロスセル先の予約進捗）</div>', unsafe_allow_html=True)
        rsv_by_svc = df_rsv["サービス種別"].value_counts().rename("予約件数")
        svc_base = pd.DataFrame({
            "サービス": all_services,
            "取引済み台数": [df_master[f"取引_{s}"].sum() for s in all_services],
        })
        rsv_m = svc_base.merge(
            rsv_by_svc.reset_index().rename(columns={"index":"サービス","サービス種別":"サービス"}),
            on="サービス", how="left"
        ).fillna(0)
        rsv_m["予約件数"] = rsv_m["予約件数"].astype(int)
        rsv_m["予約転換率"] = (rsv_m["予約件数"] / rsv_m["取引済み台数"].replace(0, np.nan) * 100).round(1).astype(str) + "%"
        st.dataframe(rsv_m, use_container_width=True, hide_index=True)


# ── TAB4：店舗別達成状況 ──────────────────────
with tab4:
    st.markdown('<div class="sec">④ 店舗別 達成状況</div>', unsafe_allow_html=True)

    store_rows = []
    for st_name in stores:
        ds = df_master[df_master["入庫店舗ID"] == st_name]
        n = len(ds)
        s_n = (ds["ランク"] == "S").sum()
        rsv_n = (ds["予約件数"] > 0).sum()
        row = {
            "店舗": st_name, "管理台数": n,
            "S台数": s_n, "A台数": (ds["ランク"]=="A").sum(),
            "B台数": (ds["ランク"]=="B").sum(), "C台数": (ds["ランク"]=="C").sum(),
            "D台数": (ds["ランク"]=="D").sum(),
            "S率": f"{s_n/n*100:.1f}%",
            "平均取引数": round(ds["取引サービス数"].mean(), 2),
            "予約取得台数": rsv_n, "予約率": f"{rsv_n/n*100:.1f}%",
        }
        for svc in all_services:
            row[f"{svc}"] = ds[f"取引_{svc}"].sum()
        store_rows.append(row)

    df_store = pd.DataFrame(store_rows)

    # KPI
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.metric("店舗数", f"{len(stores)}店舗")
    with col_s2:
        if len(df_store) > 0:
            top_store = df_store.loc[df_store["S率"].str.rstrip("%").astype(float).idxmax(), "店舗"]
            st.metric("Sランク率 最高店舗", top_store)
    with col_s3:
        if len(df_store) > 0:
            st.metric("最高 平均取引数", f"{df_store['平均取引数'].max():.2f}")

    st.markdown("")

    st.markdown('<div class="sec">店舗別 ランク構成（台数）</div>', unsafe_allow_html=True)
    store_rank_chart = df_store.set_index("店舗")[["S台数","A台数","B台数","C台数","D台数"]]
    st.bar_chart(store_rank_chart, color=["#d97706","#2563eb","#059669","#94a3b8","#e2e8f0"])

    st.markdown('<div class="sec">店舗別 サービス取引台数</div>', unsafe_allow_html=True)
    svc_store_cols = [s for s in all_services if s in df_store.columns]
    if svc_store_cols:
        st.bar_chart(df_store.set_index("店舗")[svc_store_cols])

    st.markdown('<div class="sec">店舗別 詳細テーブル</div>', unsafe_allow_html=True)
    base_cols2 = ["店舗","管理台数","S台数","A台数","B台数","C台数","D台数","S率","平均取引数","予約取得台数","予約率"]
    show_cols2 = base_cols2 + svc_store_cols
    st.dataframe(df_store[[c for c in show_cols2 if c in df_store.columns]], use_container_width=True, hide_index=True)

    if len(df_store) > 0:
        st.download_button("📥 店舗別集計CSV", to_csv(df_store), "store_ltv.csv", "text/csv", use_container_width=True)

    if has_rsv and df_rsv is not None and "サービス種別" in df_rsv.columns and "登録番号" in df_rsv.columns:
        st.markdown('<div class="sec">店舗別 × サービス別 予約件数</div>', unsafe_allow_html=True)
        df_rsv_ext = df_rsv.merge(df_master[["登録番号","入庫店舗ID"]], on="登録番号", how="left")
        if "入庫店舗ID" in df_rsv_ext.columns:
            store_svc_rsv = df_rsv_ext.groupby(["入庫店舗ID","サービス種別"]).size().unstack(fill_value=0)
            st.dataframe(store_svc_rsv, use_container_width=True)


# ── TAB5：顧客ランク一覧 ──────────────────────
with tab5:
    st.markdown('<div class="sec">🏆 顧客ランク一覧・セグメント抽出</div>', unsafe_allow_html=True)

    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        rnk_sel = st.multiselect("ランク", ["S","A","B","C","D"], default=["S","A"], key="rnk_sel")
    with cf2:
        st_sel = st.multiselect("店舗", stores, default=stores, key="st_sel")
    with cf3:
        svc_sel = st.selectbox("取引ありサービス", ["指定なし"] + all_services, key="svc_sel")

    df_f = df_master[
        (df_master["ランク"].isin(rnk_sel)) & (df_master["入庫店舗ID"].isin(st_sel))
    ].copy()
    if svc_sel != "指定なし":
        df_f = df_f[df_f[f"取引_{svc_sel}"].astype(int) == 1]

    st.markdown(f"**抽出：{len(df_f):,}台**")

    base = ["顧客ID","車両ID","登録番号","入庫店舗ID","ランク","取引サービス数",
            "未取引サービス","予約件数","本予約数","予約サービス"]
    txn_flag = [f"取引_{s}" for s in all_services]
    cols_show = [c for c in base + txn_flag if c in df_f.columns]

    df_show = df_f[cols_show].sort_values("取引サービス数", ascending=False).copy()
    for s in all_services:
        c = f"取引_{s}"
        if c in df_show.columns:
            df_show[c] = df_show[c].apply(lambda x: "◯" if x == 1 else "")
    df_show.columns = [c.replace("取引_", "") for c in df_show.columns]
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    if len(df_f) > 0:
        st.download_button("📥 セグメントリストCSV", to_csv(df_f[cols_show]),
                           f"segment_{'_'.join(rnk_sel)}.csv", "text/csv", use_container_width=True)

    if has_rsv and df_rsv is not None:
        st.markdown('<div class="sec">予約一覧（ランク・店舗フィルタ連動）</div>', unsafe_allow_html=True)
        regs_f = set(df_f["登録番号"].astype(str))
        df_rsv_f = df_rsv[df_rsv["登録番号"].astype(str).isin(regs_f)].copy()
        if "登録番号" in df_rsv_f.columns and "登録番号" in df_master.columns:
            df_rsv_f = df_rsv_f.merge(
                df_master[["登録番号","入庫店舗ID","ランク","取引サービス数"]],
                on="登録番号", how="left"
            )
        sort_c = "予約日" if "予約日" in df_rsv_f.columns else df_rsv_f.columns[0]
        st.markdown(f"**{len(df_rsv_f):,}件**")
        st.dataframe(df_rsv_f.sort_values(sort_c), use_container_width=True, hide_index=True)
        if len(df_rsv_f) > 0:
            st.download_button("📥 予約リストCSV", to_csv(df_rsv_f),
                               "reservation_filtered.csv", "text/csv", use_container_width=True)


# ── TAB6：生涯顧客ストーリー ─────────────────
with tab6:
    st.markdown('<div class="sec">📈 生涯顧客化ストーリー ─ 数字で描くロードマップ</div>', unsafe_allow_html=True)

    s_cnt  = rank_counts.get("S", 0)
    a_cnt  = rank_counts.get("A", 0)
    b_cnt  = rank_counts.get("B", 0)
    cd_cnt = rank_counts.get("C", 0) + rank_counts.get("D", 0)

    p1, p2, p3 = st.columns(3)
    with p1:
        st.markdown(f"""<div class="story-panel">
<h3>🛡 Phase 1：Sランクを守る</h3>
<p><strong>{s_cnt:,}台</strong>（全体の{s_cnt/total*100:.1f}%）が生涯顧客</p>
<p>▶ 担当者固定・離脱ゼロを目標</p>
<p>▶ 車検前6か月から予約確保</p>
<p>▶ 乗り換え提案の先手アプローチ</p>
</div>""", unsafe_allow_html=True)
    with p2:
        st.markdown(f"""<div class="story-panel">
<h3>🚀 Phase 2：A・BをSへ育てる</h3>
<p><strong>{a_cnt+b_cnt:,}台</strong>（全体の{(a_cnt+b_cnt)/total*100:.1f}%）が昇格候補</p>
<p>▶ 未取引サービスの予約取得</p>
<p>▶ クロスセルタブで機会を特定</p>
<p>▶ 月別推移タブで進捗を確認</p>
</div>""", unsafe_allow_html=True)
    with p3:
        st.markdown(f"""<div class="story-panel">
<h3>🌱 Phase 3：C・Dを底上げ</h3>
<p><strong>{cd_cnt:,}台</strong>（全体の{cd_cnt/total*100:.1f}%）が未育成層</p>
<p>▶ オイル交換で接点頻度を上げる</p>
<p>▶ 点検案内で再来店を促す</p>
<p>▶ 予約が増えれば3か月後に結果が出る</p>
</div>""", unsafe_allow_html=True)

    st.markdown("")

    st.markdown('<div class="sec">💡 クロスセル売上シミュレーション</div>', unsafe_allow_html=True)
    sc1, sc2 = st.columns(2)
    with sc1:
        sim_svc = st.selectbox("提案サービス", all_services, key="sim_svc")
    with sc2:
        sim_rnk = st.multiselect("対象ランク", ["A","B","C"], default=["A","B"], key="sim_rnk")

    sim_tgt = df_master[(df_master["ランク"].isin(sim_rnk)) & (df_master[f"取引_{sim_svc}"].astype(int) == 0)]
    sim_rev = len(sim_tgt) * SERVICE_PRICE.get(sim_svc, 30000)

    st.markdown(f"""
<div style="background:#eff6ff;border-radius:10px;padding:1.1rem 1.5rem;border-left:4px solid #1a56db;margin:0.5rem 0;">
<strong>{'/'.join(sim_rnk)}ランク × {sim_svc} 未取引台数：{len(sim_tgt):,}台</strong><br>
全台数が1回取引した場合の追加売上推計：
<strong style="font-size:1.3rem;color:#1a56db;">¥{sim_rev:,.0f}円（¥{sim_rev/1e6:.1f}M）</strong>
</div>
""", unsafe_allow_html=True)

    st.markdown('<div class="sec">📌 サービス別 未取引ポテンシャル</div>', unsafe_allow_html=True)
    pot_rows = []
    for s in all_services:
        untreated = (df_master[f"取引_{s}"].astype(int) == 0).sum()
        pot = untreated * SERVICE_PRICE.get(s, 30000)
        pot_rows.append({
            "サービス": s,
            "取引済み台数": total - untreated,
            "未取引台数": untreated,
            "未取引率": f"{untreated/total*100:.1f}%",
            "潜在売上推計": f"¥{pot/1e6:.1f}M",
        })
    if pot_rows:
        st.dataframe(pd.DataFrame(pot_rows).sort_values("未取引台数", ascending=False),
                     use_container_width=True, hide_index=True)
    else:
        st.info("取引データが読み込まれていないため、ポテンシャルを計算できません。")

    total_pot = sum(
        (df_master[f"取引_{s}"] == 0).sum() * SERVICE_PRICE.get(s, 30000)
        for s in all_services
    )
    st.markdown(f"""<div class="story-panel">
<h3>🏆 経営アクション優先度まとめ</h3>
<p>① <strong>Sランク維持 {s_cnt:,}台</strong> ── 最優先。離脱1台 = LTV数十万〜数百万円の損失</p>
<p>② <strong>A+Bランク予約取得 {a_cnt+b_cnt:,}台</strong> ── 「予約台数の月次増加」が先行指標</p>
<p>③ <strong>C+Dのオイル交換接点 {cd_cnt:,}台</strong> ── 接触頻度を上げ車検・点検に繋げる</p>
<p>④ <strong>全体未取引ポテンシャル ¥{total_pot/1e6:.0f}M</strong> ── 取引深化余地の総量</p>
<p style="opacity:0.6;font-size:0.78rem;margin-top:0.8rem;">
「月別取引推移」と「予約取得台数の推移」を同時に見ることで、3か月後の売上を先読みできる。
</p>
</div>""", unsafe_allow_html=True)


# ── TAB KPI：営業活動KPIダッシュボード ──────────────────────
with tab_kpi:
    st.markdown('<div class="sec">🎯 営業活動KPIダッシュボード</div>', unsafe_allow_html=True)

    kpi_logs = load_activity_logs()

    if not kpi_logs:
        st.info("まだ活動ログがありません。「本日の接客指示」タブから活動記録を入力してください。")
    else:
        df_kpi = logs_to_df(kpi_logs)
        df_kpi["記録日"] = pd.to_datetime(df_kpi["記録日"], errors="coerce")
        df_kpi["フェーズ番号"] = df_kpi["フェーズ"].map({
            "─ 未記録":0,"提案できなかった":1,"提案した":2,
            "興味あり ⭐":3,"仮予約 📅":4,"本予約 ✅":5,"売れた ✅":4
        }).fillna(0).astype(int)

        today_dt = pd.Timestamp(datetime.today().date())
        this_month = today_dt.to_period("M")

        # 期間フィルタ
        kpi_period = st.radio("期間", ["本日", "今月", "全期間"], horizontal=True, key="kpi_period")
        if kpi_period == "本日":
            df_kpi_f = df_kpi[df_kpi["記録日"].dt.date == today_dt.date()]
        elif kpi_period == "今月":
            df_kpi_f = df_kpi[df_kpi["記録日"].dt.to_period("M") == this_month]
        else:
            df_kpi_f = df_kpi.copy()

        # 店舗・担当者フィルタ
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            stores_kpi = ["全店舗"] + sorted(df_kpi_f["店舗"].dropna().unique().tolist())
            sel_store_kpi = st.selectbox("店舗", stores_kpi, key="kpi_store")
        with col_f2:
            staffs_kpi = ["全担当者"] + sorted(df_kpi_f["担当者"].dropna().unique().tolist())
            sel_staff_kpi = st.selectbox("担当者", staffs_kpi, key="kpi_staff")

        if sel_store_kpi != "全店舗":
            df_kpi_f = df_kpi_f[df_kpi_f["店舗"] == sel_store_kpi]
        if sel_staff_kpi != "全担当者":
            df_kpi_f = df_kpi_f[df_kpi_f["担当者"] == sel_staff_kpi]

        # ── KPIサマリー ──
        st.markdown("### 📊 KPIサマリー")
        total_act   = len(df_kpi_f[df_kpi_f["フェーズ番号"] >= 2])  # 提案した以上
        total_kyomi = len(df_kpi_f[df_kpi_f["フェーズ番号"] >= 3])  # 興味あり以上
        total_kari  = len(df_kpi_f[df_kpi_f["フェーズ"].str.contains("仮予約", na=False)])
        total_hon   = len(df_kpi_f[df_kpi_f["フェーズ"].str.contains("本予約|売れた", na=False)])
        conv_rate   = total_kyomi / total_act * 100 if total_act > 0 else 0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("提案件数", f"{total_act:,}件")
        k2.metric("興味あり", f"{total_kyomi:,}件")
        k3.metric("仮予約", f"{total_kari:,}件")
        k4.metric("本予約・成約", f"{total_hon:,}件")
        k5.metric("転換率", f"{conv_rate:.1f}%", help="興味あり÷提案件数")

        st.markdown("---")

        # ── 提案項目別集計 ──
        st.markdown("### 🔍 提案項目別 結果")
        if len(df_kpi_f) > 0:
            item_summary = df_kpi_f.groupby("提案項目").agg(
                提案件数=("フェーズ番号", lambda x: (x >= 2).sum()),
                興味あり=("フェーズ番号", lambda x: (x >= 3).sum()),
                成約=("フェーズ番号", lambda x: (x >= 4).sum()),
            ).reset_index()
            item_summary["転換率"] = (
                item_summary["興味あり"] / item_summary["提案件数"].replace(0,1) * 100
            ).round(1).astype(str) + "%"
            item_summary = item_summary[item_summary["提案件数"] > 0].sort_values("提案件数", ascending=False)
            st.dataframe(item_summary, use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── 担当者別集計 ──
        st.markdown("### 👤 担当者別 実績")
        if len(df_kpi_f) > 0:
            staff_summary = df_kpi_f.groupby("担当者").agg(
                提案件数=("フェーズ番号", lambda x: (x >= 2).sum()),
                興味あり=("フェーズ番号", lambda x: (x >= 3).sum()),
                成約=("フェーズ番号", lambda x: (x >= 4).sum()),
            ).reset_index()
            staff_summary["転換率"] = (
                staff_summary["興味あり"] / staff_summary["提案件数"].replace(0,1) * 100
            ).round(1).astype(str) + "%"
            staff_summary = staff_summary[staff_summary["提案件数"] > 0].sort_values("提案件数", ascending=False)
            st.dataframe(staff_summary, use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── 担当者別詳細実績 ──
        st.markdown("### 👤 担当者別 詳細実績")
        if len(df_kpi_f) > 0:
            staff_detail = df_kpi_f.groupby(["担当者","提案項目"]).agg(
                提案=("フェーズ番号", lambda x: (x >= 2).sum()),
                興味あり=("フェーズ番号", lambda x: (x >= 3).sum()),
                成約=("フェーズ番号", lambda x: (x >= 4).sum()),
            ).reset_index()
            staff_detail = staff_detail[staff_detail["提案"] > 0]
            staff_detail["転換率"] = (
                staff_detail["興味あり"] / staff_detail["提案"].replace(0,1) * 100
            ).round(1).astype(str) + "%"

            # 担当者別に折り畳み表示
            for staff_name_kpi in staff_detail["担当者"].unique():
                df_s = staff_detail[staff_detail["担当者"]==staff_name_kpi]
                total_p  = df_s["提案"].sum()
                total_k  = df_s["興味あり"].sum()
                total_s  = df_s["成約"].sum()
                with st.expander(f"👤 {staff_name_kpi}　提案{total_p}件 / 興味あり{total_k}件 / 成約{total_s}件"):
                    st.dataframe(df_s[["提案項目","提案","興味あり","成約","転換率"]],
                                 use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── 車検フォローアップリスト ──
        st.markdown("### 📋 車検フォローアップリスト（興味あり・仮予約）")
        df_followup = df_kpi[
            (df_kpi["提案項目"] == "車検") &
            (df_kpi["フェーズ"].str.contains("興味あり|仮予約", na=False))
        ].copy()

        if len(df_followup) > 0:
            df_followup["車検満了日_dt"] = pd.to_datetime(df_followup["車検満了日"], errors="coerce")
            df_followup["満了まで"] = (df_followup["車検満了日_dt"] - today_dt).dt.days
            df_followup["アクション推奨"] = df_followup["満了まで"].apply(
                lambda d: "🔴 今すぐ連絡" if pd.notna(d) and d <= 90
                else ("⚠️ 3ヶ月以内に連絡" if pd.notna(d) and d <= 180 else "📅 継続フォロー")
            )
            show_cols = ["記録日","顧客ID","車両ID","担当者","フェーズ","車検満了日","満了まで","アクション推奨","備考"]
            show_cols = [c for c in show_cols if c in df_followup.columns]
            st.dataframe(df_followup[show_cols].sort_values("満了まで"), use_container_width=True, hide_index=True)

            # CSVエクスポート（営業リスト用）
            buf_fu = BytesIO()
            df_followup[show_cols].to_csv(buf_fu, index=False, encoding="utf-8-sig")
            st.download_button(
                "📥 フォローアップリストCSVダウンロード（予約システム取込用）",
                buf_fu.getvalue(),
                f"followup_shaken_{datetime.today().strftime('%Y%m%d')}.csv",
                "text/csv",
                use_container_width=True,
            )
        else:
            st.info("車検で「興味あり」または「仮予約」のログがまだありません")


# ──────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center;font-size:0.72rem;color:#94a3b8;padding:0.5rem 0;">
    生涯顧客LTV進捗管理システム v3.0 ─ オートウェーブ ─ セッション完結型・データ永続化なし
</div>
""", unsafe_allow_html=True)
