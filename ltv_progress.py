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
from pathlib import Path

# ──────────────────────────────────────────────
st.set_page_config(
    page_title="生涯顧客LTV進捗管理 | オートウェーブ",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
DEFAULT_SERVICES = ["車検", "タイヤ交換", "オイル交換", "12か月点検", "バッテリー交換", "コーティング", "自動車販売"]
SERVICE_PRICE = {
    "車検": 80000, "タイヤ交換": 40000, "オイル交換": 5000,
    "12か月点検": 15000, "バッテリー交換": 20000,
    "コーティング": 60000, "自動車販売": 2000000,
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
    for r, t in thr.items():
        if n >= t:
            return r
    return "D"


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
    if data_dir_exists and auto_master_path:
        st.markdown('''<div style="background:#ecfdf5;border:1px solid #6ee7b7;border-radius:8px;
        padding:0.5rem 0.8rem;font-size:0.78rem;color:#065f46;margin-bottom:0.5rem;">
        📁 <strong>自動読込モード</strong>：dataフォルダのCSVを検出しました
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
    st.markdown("### ⚙️ ランク設定（取引サービス種類数）")
    rank_s = st.slider("Sランク以上", 3, 7, 5, key="rs")
    rank_a = st.slider("Aランク以上", 2, 6, 4, key="ra")
    rank_b = st.slider("Bランク以上", 1, 5, 3, key="rb")
    rank_c = st.slider("Cランク以上", 1, 4, 2, key="rc")
    thresholds = {"S": rank_s, "A": rank_a, "B": rank_b, "C": rank_c, "D": 0}

    st.markdown("---")
    st.markdown("### 読込状況")

    # マスター
    if f_master:
        st.markdown('<div class="upload-status upload-ok">✅ マスター（手動アップロード）</div>', unsafe_allow_html=True)
    elif auto_master_path:
        st.markdown(f'<div class="upload-status upload-ok">📁 マスター：{auto_master_path.name}</div>', unsafe_allow_html=True)
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
        st.markdown('<div class="upload-status upload-wait">⏳ 取引CSV（未検出）</div>', unsafe_allow_html=True)

    # 予約
    if f_rsv:
        st.markdown('<div class="upload-status upload-ok">✅ 予約データ（手動アップロード）</div>', unsafe_allow_html=True)
    elif auto_rsv_path:
        st.markdown(f'<div class="upload-status upload-ok">📁 予約：{auto_rsv_path.name}</div>', unsafe_allow_html=True)
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
        st.caption("⚠️ dataフォルダが存在しません。作成してCSVを配置してください。")


# ──────────────────────────────────────────────
# サンプルCSV生成
# ──────────────────────────────────────────────
# ── マスターデータの決定（手動アップロード優先 > dataフォルダ自動読込）──
_has_master = f_master is not None or auto_master_path is not None

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

# ── マスターデータ読み込み（手動優先 > 自動）──
try:
    if f_master is not None:
        df_master = norm(read_csv(f_master, "マスターデータ"))
    elif auto_master_path is not None:
        df_master = norm(read_path(auto_master_path, "マスターデータ"))
    else:
        st.error("マスターデータが見つかりません。")
        st.stop()
except Exception as e:
    st.error(f"マスターデータ読み込みエラー: {e}")
    st.stop()

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

# 手動アップロード優先 > dataフォルダ自動読込
_txn_sources = []  # (サービス名, DataFrameまたはPath)
if f_txns:
    # 手動アップロードがある場合はそちらを使用
    _txn_sources = [("upload", f) for f in f_txns]
elif auto_txn_paths:
    # dataフォルダの取引CSVを自動読込
    _txn_sources = [("auto", p) for p in auto_txn_paths]

if _txn_sources:
    for mode, src in _txn_sources:
        if mode == "upload":
            f = src
            svc = f.name.replace(".csv", "").replace(".CSV", "").strip()
            try:
                df_t = norm(read_csv(f, svc))
            except Exception:
                st.warning(f"⚠️ {f.name} を読み込めませんでした")
                continue
        else:
            # auto mode: src is a Path
            p = src
            svc = p.name.replace(".csv", "").replace(".CSV", "").strip()
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
                st.warning(f"⚠️ {f.name}：「登録番号」列が見つかりません（自動車販売は登録番号キーが必要です）")
                continue
        else:
            # その他サービス：車両IDキー
            vc = find_col(df_t, ["車両ID", "VehicleID", "vehicle_id"])
            master_key = "車両ID"
            if not vc:
                st.warning(f"⚠️ {f.name}：「車両ID」列が見つかりません")
                continue

        dc = find_col(df_t, ["前回取引日", "取引日", "最終取引日", "日付", "date", "Date"])
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

# 取引サービス数をNumPy演算で計算（apply不使用）
flag_matrix = np.column_stack([df_master[c].values for c in txn_cols])
df_master["取引サービス数"] = flag_matrix.sum(axis=1).astype(int)
df_master["ランク"] = df_master["取引サービス数"].apply(lambda n: assign_rank(n, thresholds))

# 未取引サービス列をNumPy演算で生成（apply不使用）
untreated_list = []
for i in range(len(df_master)):
    row_flags = flag_matrix[i]
    untreated_list.append(
        "、".join([s for s, f in zip(all_services, row_flags) if int(f) == 0])
    )
df_master["未取引サービス"] = untreated_list

# ── 予約CSV突合（手動優先 > dataフォルダ自動読込）──
has_rsv = (f_rsv is not None) or (auto_rsv_path is not None)
df_rsv = None
if has_rsv:
    try:
        if f_rsv is not None:
            df_rsv = norm(read_csv(f_rsv, "予約データ"))
        else:
            df_rsv = norm(read_path(auto_rsv_path, "予約データ"))
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
tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 本日の接客指示",
    "📊 全体ダッシュボード",
    "📅 月別取引推移",
    "🔗 クロスセル進捗",
    "🏪 店舗別達成状況",
    "🏆 顧客ランク一覧",
    "📈 生涯顧客ストーリー",
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
                           "ランク", "取引サービス数", "未取引サービス", "車検満了日"] + \
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

                # Sランク専用：最優先対応
                if rank == "S":
                    if days_to_exp is not None and 0 <= days_to_exp <= 180:
                        actions.append(f"🔴【最優先】車検満期まで{days_to_exp}日。乗り換え相談＋車検予約を獲得する")
                    else:
                        actions.append("🌟【Sランク特別対応】担当者が直接挨拶。カーライフ状況をヒアリングし関係を深める")
                    if "自動車販売" in untreated_list and days_to_exp and days_to_exp <= 365:
                        actions.append("🚗 乗り換え提案：現在の車の年数・走行距離を確認し次の車を提案する")

                # A・Bランク：クロスセル提案
                elif rank in ["A", "B"]:
                    # 車検満期が近い
                    if days_to_exp is not None and 0 <= days_to_exp <= 90:
                        actions.append(f"⚡ 車検満期まで{days_to_exp}日！本日必ず車検予約を取る")
                    # 未取引サービスから最優先提案を1つ選ぶ
                    priority_svcs = ["自動車販売", "コーティング", "タイヤ交換", "バッテリー交換", "12か月点検", "オイル交換"]
                    for ps in priority_svcs:
                        if ps in untreated_list:
                            talk = {
                                "自動車販売": "「そろそろ乗り換えを考えていませんか？今なら〇〇がおすすめです」",
                                "コーティング": "「タイヤ（または車検）に合わせてボディもきれいにしませんか？」",
                                "タイヤ交換": "「タイヤの溝を確認しましたが、次の交換時期が近づいています」",
                                "バッテリー交換": "「バッテリーの状態を点検しましょうか？交換目安の時期です」",
                                "12か月点検": "「最後の12か月点検からしばらく経ちますね。今日ついでにどうですか？」",
                                "オイル交換": "「オイル交換の時期になっています。今日一緒にやりましょう」",
                            }.get(ps, f"「{ps}はいかがでしょうか？」")
                            actions.append(f"💡 提案：{ps} ─ {talk}")
                            break  # 1つだけ

                # C・Dランク：接点強化
                elif rank in ["C", "D"]:
                    if days_to_exp is not None and 0 <= days_to_exp <= 90:
                        actions.append(f"⚡ 車検満期まで{days_to_exp}日！車検予約を提案する")
                    else:
                        actions.append("📞 次回オイル交換の予約を今日取る（接触頻度アップが最優先）")
                    actions.append("👋 名前を呼んで前回の会話を思い出させる（「顔を覚えている店」を演出）")

                # 車検満期アラート（全ランク共通）
                if days_to_exp is not None:
                    if days_to_exp < 0:
                        actions.append(f"⚠️ 車検満期が{abs(days_to_exp)}日超過しています！早急に対応")
                    elif days_to_exp <= 30 and rank not in ["S"]:
                        actions.append(f"🔴 車検満期まで{days_to_exp}日！本日予約必須")

                return " ／ ".join(actions) if actions else "通常対応"

            def rank_badge(rank):
                colors = {"S": "#d97706", "A": "#2563eb", "B": "#059669", "C": "#64748b", "D": "#94a3b8"}
                bg = colors.get(rank, "#94a3b8")
                return f'<span style="background:{bg};color:white;padding:2px 10px;border-radius:10px;font-weight:900;font-size:0.82rem;">{rank}ランク</span>'

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
</div>
<div style="background:#fff8e1;border-radius:8px;padding:0.5rem 0.8rem;font-size:0.85rem;color:#92400e;font-weight:600;">
  📌 接客指示：{row.get('接客指示','通常対応')}
</div>
</div>
""", unsafe_allow_html=True)

            # ── 全来店予定一覧 ──
            st.markdown('<div class="sec">本日 全来店予定一覧（予約時刻順）</div>', unsafe_allow_html=True)

            # 時刻順にソート → 同時刻内はランク順
            rank_order_map = {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4}
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

            # ── CSV ダウンロード ──
            st.markdown("")
            dl_cols = ["予約時刻", "登録番号", "サービス種別", "予約ステータス", "入庫店舗ID",
                       "ランク", "取引サービス数", "未取引サービス", "車検残日数", "接客指示"]
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
                            "S": colors.HexColor("#fef3c7"),
                            "A": colors.HexColor("#dbeafe"),
                            "B": colors.HexColor("#dcfce7"),
                            "C": colors.HexColor("#f1f5f9"),
                            "D": colors.HexColor("#f8fafc"),
                        }
                        RANK_FG = {
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

                            # 時刻表示（あれば）
                            time_txt = f"[{rsv_time_pdf}] " if rsv_time_pdf else ""
                            name_car = f"{customer_name_pdf}　{car_model_pdf}".strip("　")

                            header_data = [[
                                Paragraph(f"{rank}ランク", ParagraphStyle("rk", fontName=FONT, fontSize=9, textColor=fg, leading=12)),
                                Paragraph(f"{time_txt}{reg_no}", ParagraphStyle("hd", fontName=FONT, fontSize=9, leading=12)),
                                Paragraph(name_car or store_name, ParagraphStyle("nm", fontName=FONT, fontSize=8.5, leading=12)),
                                Paragraph(f"車検満期：{exp_txt}", ParagraphStyle("ex", fontName=FONT, fontSize=8, leading=11)),
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

    # ── ランク定義の説明 ──
    st.markdown('<div class="sec">📖 ランク定義（判定基準）</div>', unsafe_allow_html=True)
    rank_def_rows = [
        {"ランク": "S", "意味": "生涯顧客", "判定条件": f"取引サービス {rank_s}種類以上",
         "対応方針": "最優先維持。担当者固定・乗り換え提案・離脱ゼロを目標"},
        {"ランク": "A", "意味": "優良顧客", "判定条件": f"取引サービス {rank_a}種類",
         "対応方針": "Sランク昇格候補。未取引サービスへのクロスセル提案"},
        {"ランク": "B", "意味": "定期顧客", "判定条件": f"取引サービス {rank_b}種類",
         "対応方針": "取引品目を1つ増やしてAランクへ育成"},
        {"ランク": "C", "意味": "利用顧客", "判定条件": f"取引サービス {rank_c}種類",
         "対応方針": "接触頻度を上げ、オイル交換を起点にBランクへ"},
        {"ランク": "D", "意味": "未育成顧客", "判定条件": f"取引サービス {rank_c-1}種類以下",
         "対応方針": "オイル交換の次回予約を取ることが最初のアクション"},
    ]
    df_rank_def = pd.DataFrame(rank_def_rows)
    st.caption(f"※ ランク判定基準は取引サービスの「種類数」で決まります（取引金額ではありません）。サイドバーのスライダーで変更可能。")
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
            rank_colors_pdf = {"S":"#d97706","A":"#2563eb","B":"#059669","C":"#64748b","D":"#94a3b8"}
            rank_hdr = [_p("ランク",8), _p("台数",8), _p("割合",8),
                        _p("意味",8), _p("判定条件",8), _p("平均取引数",8), _p("予約取得率",8)]
            rank_rows_pdf = [rank_hdr]
            for r in ["S","A","B","C","D"]:
                dr = df_master[df_master["ランク"]==r]
                if len(dr) == 0: continue
                fg = rl_colors.HexColor(rank_colors_pdf[r])
                meanings = {"S":"生涯顧客","A":"優良顧客","B":"定期顧客","C":"利用顧客","D":"未育成"}
                conds = {"S":f"{rank_s}種以上","A":f"{rank_a}種","B":f"{rank_b}種","C":f"{rank_c}種","D":f"{rank_c-1}種以下"}
                rank_rows_pdf.append([
                    _p(f"{r}ランク", 9, fg),
                    _p(f"{len(dr):,}台", 9),
                    _p(f"{len(dr)/total*100:.1f}%", 9),
                    _p(meanings[r], 8),
                    _p(conds[r], 8),
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


# ──────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center;font-size:0.72rem;color:#94a3b8;padding:0.5rem 0;">
    生涯顧客LTV進捗管理システム v3.0 ─ オートウェーブ ─ セッション完結型・データ永続化なし
</div>
""", unsafe_allow_html=True)
