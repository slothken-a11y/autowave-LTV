"""
予約CSV変換ツール
=================
オートウェーブ 予約システムCSV → 生涯顧客LTV管理システム用CSV変換ツール

【処理内容】
 ① C列（店舗名）   → マスターデータの店舗IDに部分一致で名寄せ
 ② N列（メニュー名）→ サービス名（ファイル名）に推論で名寄せ
 ③ Q列（登録番号） → 半角スペース除去して正規化

【セキュリティ】
 - 完全ローカル・インメモリ処理
 - データはサーバーに送信されません
 - ブラウザを閉じると全データ消去
"""

import streamlit as st
import pandas as pd
import re
from io import BytesIO
from datetime import datetime

# ──────────────────────────────────────────────
st.set_page_config(
    page_title="予約CSV変換ツール | オートウェーブ",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700;900&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans JP', 'Meiryo', sans-serif; }

.main-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    color: white; padding: 1.4rem 2rem; border-radius: 12px;
    margin-bottom: 1.2rem;
}
.main-header h1 { margin:0; font-size:1.5rem; font-weight:900; }
.main-header p  { margin:0.3rem 0 0; font-size:0.8rem; opacity:0.6; }

.sec { font-size:1rem; font-weight:800; color:#0f172a;
       border-left:4px solid #2563eb; padding-left:0.65rem;
       margin:1.4rem 0 0.8rem; }

.kpi-card {
    background:#fff; border-radius:10px; padding:0.9rem 1rem;
    box-shadow:0 2px 8px rgba(0,0,0,0.07); text-align:center;
    border-top:4px solid #2563eb;
}
.kpi-card.green { border-top-color:#059669; }
.kpi-card.red   { border-top-color:#dc2626; }
.kpi-card.gold  { border-top-color:#d97706; }
.kpi-card .lbl { font-size:0.72rem; color:#888; margin-bottom:0.25rem; font-weight:600; }
.kpi-card .val { font-size:1.7rem; font-weight:900; color:#0f172a; line-height:1.1; }

.match-ok   { background:#ecfdf5; color:#065f46; border-radius:6px; padding:2px 8px; font-size:0.8rem; font-weight:600; }
.match-warn { background:#fffbeb; color:#92400e; border-radius:6px; padding:2px 8px; font-size:0.8rem; font-weight:600; }
.match-err  { background:#fef2f2; color:#991b1b; border-radius:6px; padding:2px 8px; font-size:0.8rem; font-weight:600; }

.sec-banner {
    background:#ecfdf5; border:1px solid #6ee7b7; border-radius:8px;
    padding:0.45rem 1rem; font-size:0.77rem; color:#065f46;
    text-align:center; margin-bottom:1rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>🔄 予約CSV変換ツール</h1>
    <p>予約システムCSV → 生涯顧客LTV管理システム用CSV に自動変換します</p>
</div>
<div class="sec-banner">
    🔒 完全ローカル処理 ─ データはサーバーに送信されません。ブラウザを閉じると自動消去されます。
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# サービス名マッピング辞書（メニュー名 → CSVファイル名）
# ──────────────────────────────────────────────
SERVICE_MAPPING = {
    # 車検系
    "車検": ["車検", "検査", "継続検査", "新規検査", "ユーザー車検"],
    # オイル交換系
    "オイル交換": ["オイル", "oil", "エンジンオイル", "オイル交換", "オイルフィルター"],
    # タイヤ交換系
    "タイヤ交換": ["タイヤ", "tire", "ホイール", "スタッドレス", "夏タイヤ", "冬タイヤ", "タイヤ交換", "タイヤ保管"],
    # 12か月点検系
    "12か月点検": ["12ヶ月", "12か月", "12カ月", "一年点検", "年次点検", "法定点検",
                   "6ヶ月", "6か月", "6カ月", "定期点検", "点検", "整備", "不調", "診断"],
    # バッテリー交換系
    "バッテリー交換": ["バッテリー", "battery", "蓄電池"],
    # コーティング系
    "コーティング": ["コーティング", "洗車", "磨き", "ガラスコート", "ボディ"],
    # 自動車販売系
    "自動車販売": ["新車", "中古車", "販売", "購入", "乗り換え", "下取り", "査定"],
}

# 優先順位（後ろほど優先度低）
SERVICE_PRIORITY = [
    "車検", "自動車販売", "コーティング", "タイヤ交換",
    "バッテリー交換", "12か月点検", "オイル交換"
]


def infer_service(menu_name: str) -> tuple[str, float]:
    """
    メニュー名からサービス名を推論する。
    戻り値: (サービス名, 信頼度 0.0〜1.0)
    """
    if pd.isna(menu_name) or str(menu_name).strip() == "":
        return "不明", 0.0

    menu = str(menu_name).strip()

    # 完全一致チェック
    for svc in SERVICE_PRIORITY:
        if svc == menu:
            return svc, 1.0

    # キーワード部分一致（長いキーワード優先）
    matches = []
    for svc, keywords in SERVICE_MAPPING.items():
        for kw in sorted(keywords, key=len, reverse=True):
            if kw in menu:
                matches.append((svc, len(kw) / len(menu)))
                break

    if not matches:
        return "不明", 0.0

    # 最も長いキーワードでマッチしたものを採用
    # 信頼度はキーワード長に関わらず最低0.6を保証（部分一致は十分な根拠）
    best = max(matches, key=lambda x: x[1])
    return best[0], max(min(best[1] * 3, 0.95), 0.6)


def normalize_reg_number(reg: str) -> str:
    """登録番号の半角スペースを除去して正規化する"""
    if pd.isna(reg):
        return ""
    # 全スペース（半角・全角）を除去
    return re.sub(r'[\s\u3000]+', '', str(reg).strip())


def match_store(store_name: str, store_master: list[str]) -> tuple[str, float]:
    """
    店舗名を部分一致でマスターの店舗IDに名寄せする。
    戻り値: (マッチした店舗名, 信頼度)
    """
    if pd.isna(store_name) or not store_master:
        return store_name, 0.0

    name = str(store_name).strip()

    # 完全一致
    if name in store_master:
        return name, 1.0

    # 部分一致（マスター側が元データに含まれるか）
    for master in store_master:
        if master in name or name in master:
            return master, 0.85

    # 地名の抽出マッチ（「茂原店」→「茂原」など）
    for master in store_master:
        core = master.replace("店", "").replace("支店", "").replace("営業所", "")
        if core in name:
            return master, 0.75

    return name, 0.3  # マッチなし・元の値をそのまま返す


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    return buf.getvalue()


def read_csv_auto(f):
    for enc in ["utf-8-sig", "utf-8", "cp932", "shift_jis", "latin1"]:
        try:
            f.seek(0)
            return pd.read_csv(f, encoding=enc)
        except Exception:
            continue
    raise ValueError("CSVを読み込めませんでした")


# ──────────────────────────────────────────────
# サイドバー
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📂 ファイルアップロード")
    f_order = st.file_uploader(
        "予約CSV（システム出力）",
        type=["csv"], key="order",
        help="予約システムから出力したCSVをアップロード"
    )
    f_master = st.file_uploader(
        "マスターデータ.csv（店舗名名寄せ用・任意）",
        type=["csv"], key="master",
        help="店舗IDの名寄せ精度を上げるために使用"
    )

    st.markdown("---")
    st.markdown("### ⚙️ 変換設定")
    exclude_cancel = st.checkbox("キャンセル済みを除外", value=True)
    exclude_unknown = st.checkbox("サービス不明を除外", value=False)
    min_confidence = st.slider("名寄せ最低信頼度", 0.0, 1.0, 0.1, 0.05,
                                help="この値未満の名寄せ結果は「要確認」フラグが付きます")

    st.markdown("---")
    st.markdown("### 📋 サービスマッピング設定")
    st.caption("メニュー名のキーワードをカスタマイズできます")
    custom_keywords = {}
    with st.expander("マッピング確認・編集"):
        for svc, kws in SERVICE_MAPPING.items():
            val = st.text_input(svc, value="、".join(kws), key=f"kw_{svc}")
            custom_keywords[svc] = [k.strip() for k in val.split("、") if k.strip()]

    if custom_keywords:
        SERVICE_MAPPING.update(custom_keywords)


# ──────────────────────────────────────────────
# メイン処理
# ──────────────────────────────────────────────
if not f_order:
    st.info("サイドバーから予約CSVをアップロードしてください。")

    st.markdown('<div class="sec">入力CSVの列構成（予約システム出力）</div>', unsafe_allow_html=True)
    st.markdown("""
| 列 | 列名 | 変換内容 |
|----|------|---------|
| C列（3列目） | **店舗名** | マスターデータの店舗IDに部分一致で名寄せ |
| N列（14列目） | **メニュー名** | サービス名（車検・オイル交換等）に推論で名寄せ |
| Q列（17列目） | **登録番号** | 半角スペースを除去して正規化 |
| H列（8列目） | 予約日時 | 予約日・**予約時刻**として出力（時刻順でソート）|

**出力されるCSV：**
- `Reservation.csv` ─ 予約データ（LTV管理システム投入用）
- `変換レポート.csv` ─ 名寄せ結果の確認用
""")
    st.stop()

# ── CSVロード ──
try:
    df_order = read_csv_auto(f_order)
    df_order.columns = df_order.columns.str.strip().str.replace("　","",regex=False)
except Exception as e:
    st.error(f"予約CSV読み込みエラー: {e}")
    st.stop()

# ── アップロードされたCSVが予約CSVかどうか自動チェック ──
REQUIRED_RSV_COLS = ["予約日時", "メニュー名", "予約状況"]
MASTER_SIGNATURE  = ["車検満了日", "車両ID", "入庫店舗ID"]

looks_like_master = sum(1 for c in MASTER_SIGNATURE if c in df_order.columns) >= 2
has_rsv_cols      = any(c in df_order.columns for c in REQUIRED_RSV_COLS)

if looks_like_master and not has_rsv_cols:
    st.error(
        "【ファイル間違い】マスターデータが「予約CSV」欄にアップロードされています。\n\n"
        "正しいアップロード先:\n"
        "  ① 予約CSV欄 → 予約システムから出力した csv_order_XXXXXX.csv\n"
        "  ② マスターデータ欄 → Master_Data.csv（店舗名名寄せ用）\n\n"
        f"現在アップロードされたCSVの列: {list(df_order.columns)}"
    )
    st.stop()

if not has_rsv_cols:
    st.warning(
        "予約システムCSVの標準列（予約日時・メニュー名・予約状況）が見つかりません。\n"
        f"読み込んだCSVの列名: {list(df_order.columns[:10])}\n\n"
        "予約システムから出力したCSVをアップロードしているか確認してください。"
    )

# マスターデータから店舗名リストを取得
store_master_list = []
if f_master:
    try:
        df_m = read_csv_auto(f_master)
        df_m.columns = df_m.columns.str.strip()
        sc = next((c for c in ["入庫店舗ID", "店舗ID", "店舗名", "店舗"] if c in df_m.columns), None)
        if sc:
            store_master_list = df_m[sc].dropna().astype(str).unique().tolist()
    except Exception:
        pass

# ── キャンセル除外 ──
df_work = df_order.copy()
if exclude_cancel and "キャンセル日時" in df_work.columns:
    before = len(df_work)
    df_work = df_work[df_work["キャンセル日時"].isna()]
    cancelled = before - len(df_work)
else:
    cancelled = 0

# ── 変換処理（ベクトル演算で高速化）──
df_result = df_work.copy()

# 列名の正規化（全角スペース・前後空白・改行を除去）
df_result.columns = (
    df_result.columns
    .str.strip()
    .str.replace("　", "", regex=False)  # 全角スペース除去
    .str.replace(r'\s+', ' ', regex=True)   # 連続空白を1つに
    .str.strip()
)

# ── 列名確認（デバッグ用）──
with st.expander("📋 読み込んだCSVの列名確認（不具合時に確認）", expanded=False):
    st.write(list(df_result.columns))

# ① 登録番号の正規化（Q列）─ 全行一括
_reg_col = next((c for c in df_result.columns if "登録番号" in c), None)
if _reg_col:
    df_result["登録番号"] = df_result[_reg_col].fillna("").astype(str).str.replace(r'[\s\u3000]+', '', regex=True)
    # "nan"文字列を空文字に変換（予約システムで登録番号未入力の場合）
    df_result["登録番号"] = df_result["登録番号"].replace({"nan": "", "NaN": "", "None": "", "NULL": ""})
else:
    df_result["登録番号"] = ""

# ② 予約日時の一括変換（H列）
_dt_col = next((c for c in df_result.columns if "予約日時" in c or ("予約" in c and "日時" in c)), None)
_dt = pd.to_datetime(df_result[_dt_col] if _dt_col else pd.Series(dtype=str), errors="coerce")
df_result["予約日"]    = _dt.dt.strftime("%Y/%m/%d").fillna("")
df_result["予約時刻"]  = _dt.dt.strftime("%H:%M").fillna("")
df_result["_sort_dt"]  = _dt

# ③ 予約ステータスの一括変換（A列）
def _map_status(s):
    s = str(s).strip()
    if "来店前" in s or "受付" in s: return "本予約"
    if "仮" in s: return "仮予約"
    return "相談中"
_status_col = next((c for c in df_result.columns if "予約状況" in c), None)
df_result["予約ステータス"] = df_result[_status_col].apply(_map_status) if _status_col else "相談中" 

# ④ 店舗名の名寄せ（C列）─ ユニーク値のみ処理してキャッシュ
_store_cache = {}
def _match_store_cached(name):
    if name not in _store_cache:
        if store_master_list:
            matched, conf = match_store(name, store_master_list)
        else:
            matched, conf = name, 0.5
        _store_cache[name] = (matched, conf)
    return _store_cache[name]

_shop_col = next((c for c in df_result.columns if "店舗名" in c), None)
_store_results = (df_result[_shop_col].fillna("").astype(str) if _shop_col else pd.Series([""] * len(df_result), index=df_result.index)).apply(_match_store_cached)
df_result["入庫店舗ID"]     = _store_results.apply(lambda x: x[0])
df_result["_店舗信頼度_num"] = _store_results.apply(lambda x: x[1])

# ⑤ メニュー名からサービス名を推論（N列）─ ユニーク値のみ処理してキャッシュ
_svc_cache = {}
def _infer_svc_cached(menu):
    if menu not in _svc_cache:
        _svc_cache[menu] = infer_service(menu)
    return _svc_cache[menu]

# メニュー名列を優先順位つきで検索（「メニュー名」完全一致 > 「メニュー」部分一致の順）
_menu_col = next((c for c in df_result.columns if c == "メニュー名"), None) or next((c for c in df_result.columns if "メニュー名" in c), None) or next((c for c in df_result.columns if "メニュー" in c and "カテゴリー" not in c), None)

# メニュー名が取れているか確認（デバッグ用）
if _menu_col:
    _menu_sample = df_result[_menu_col].dropna().astype(str).head(3).tolist()
else:
    _menu_sample = []
    st.warning(f"⚠️ メニュー名列が見つかりませんでした。列名を確認してください。利用可能な列: {list(df_result.columns)}")

_svc_results = (df_result[_menu_col].fillna("").astype(str) if _menu_col else pd.Series([""] * len(df_result), index=df_result.index)).apply(_infer_svc_cached)
df_result["サービス種別"]    = _svc_results.apply(lambda x: x[0])
df_result["_サービス信頼度_num"] = _svc_results.apply(lambda x: x[1])

# ⑥ 元メニュー名の保持
df_result["元メニュー名"] = (df_result[_menu_col].fillna("").astype(str).str.strip() if _menu_col else "")

# ⑦ 顧客情報の一括取得
def _clean(col_keywords, df):
    """列名に指定キーワードが含まれる列を探して返す"""
    if isinstance(col_keywords, str):
        col_keywords = [col_keywords]
    matched = next((c for c in df.columns if any(kw in c for kw in col_keywords)), None)
    if matched:
        return df[matched].fillna("").astype(str).str.strip().replace("nan", "")
    return pd.Series([""] * len(df), index=df.index)

df_result["顧客名"]  = _clean(["お名前", "顧客名", "氏名"], df_result)
df_result["車種"]    = _clean(["車種"], df_result)
df_result["備考"]    = _clean(["備考"], df_result)
# アプリID列：「アプリ ID」（半角スペースあり）を優先して検索
_app_col = next((c for c in df_result.columns if c == "アプリ ID" or c == "アプリID"), None) or next((c for c in df_result.columns if "アプリ" in c and ("ID" in c or "id" in c)), None)
df_result["アプリID"] = (df_result[_app_col].fillna("").astype(str).str.strip().replace("nan","") if _app_col else pd.Series([""] * len(df_result), index=df_result.index))

# ⑧ 信頼度・要確認フラグ
df_result["_店舗信頼度"]    = df_result["_店舗信頼度_num"].apply(lambda x: f"{x:.0%}")
df_result["_サービス信頼度"] = df_result["_サービス信頼度_num"].apply(lambda x: f"{x:.0%}")
_overall = df_result[["_店舗信頼度_num","_サービス信頼度_num"]].min(axis=1)
df_result["_要確認"] = _overall.apply(lambda x: "⚠️ 要確認" if x < min_confidence else "✅ OK")

# レポート用列を整理
df_result["_元_店舗名"]      = (df_work[_shop_col].fillna("").astype(str) if _shop_col else "")
df_result["_名寄せ_店舗"]    = df_result["入庫店舗ID"]
df_result["_元_メニュー名"]  = df_result["元メニュー名"]
df_result["_推論_サービス"]  = df_result["サービス種別"]
df_result["_元_登録番号"]    = (df_work[_reg_col].fillna("").astype(str) if _reg_col else "")
df_result["_正規化_登録番号"] = df_result["登録番号"]

# 予約時刻順にソート（早い順）
if "_sort_dt" in df_result.columns:
    df_result = df_result.sort_values("_sort_dt", ascending=True, na_position="last")
    df_result = df_result.drop(columns=["_sort_dt"])
df_result = df_result.drop(columns=["_店舗信頼度_num", "_サービス信頼度_num"], errors="ignore")

# サービス不明除外
if exclude_unknown:
    df_result = df_result[df_result["サービス種別"] != "不明"]

# ── 統計計算 ──
total_in     = len(df_order)
total_out    = len(df_result)
needs_check_count = (df_result["_要確認"] == "⚠️ 要確認").sum()
unknown_count= (df_result["サービス種別"] == "不明").sum()

# ── 出力DataFrame ──
reservation_cols = ["予約時刻", "登録番号", "顧客名", "車種", "サービス種別", "元メニュー名", "予約ステータス", "予約日", "入庫店舗ID", "備考", "アプリID"]
df_reservation = df_result[reservation_cols].copy()
df_report = df_result.copy()

# ──────────────────────────────────────────────
# 画面表示
# ──────────────────────────────────────────────
st.markdown('<div class="sec">📊 変換サマリー</div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
for col, (lbl, val, cls) in zip([c1, c2, c3, c4], [
    ("入力件数",       f"{total_in:,}件",           ""),
    ("変換後件数",     f"{total_out:,}件",           "green"),
    ("要確認件数",     f"{needs_check_count:,}件",   "gold" if needs_check_count > 0 else "green"),
    ("サービス不明",   f"{unknown_count:,}件",       "red" if unknown_count > 0 else "green"),
]):
    with col:
        st.markdown(f"""<div class="kpi-card {cls}">
            <div class="lbl">{lbl}</div><div class="val">{val}</div>
        </div>""", unsafe_allow_html=True)

if cancelled > 0:
    st.info(f"ℹ️ キャンセル済み {cancelled}件 を除外しました。")

st.markdown("")

# ── タブ表示 ──
tab1, tab2, tab3 = st.tabs([
    "📄 変換結果（Reservation.csv）",
    "🔍 名寄せ確認レポート",
    "📊 サービス分布",
])

with tab1:
    st.markdown('<div class="sec">変換結果プレビュー</div>', unsafe_allow_html=True)
    st.caption("このデータをそのまま data/Reservation.csv として保存してください。")
    st.dataframe(df_reservation, use_container_width=True, hide_index=True)

    st.download_button(
        "📥 Reservation.csv をダウンロード",
        to_csv_bytes(df_reservation),
        "Reservation.csv",
        "text/csv",
        use_container_width=True,
        type="primary",
    )

with tab2:
    st.markdown('<div class="sec">名寄せ確認レポート</div>', unsafe_allow_html=True)
    st.caption("「⚠️ 要確認」の行は信頼度が低い名寄せ結果です。内容を確認して必要に応じて手修正してください。")

    # 要確認のみ表示オプション
    show_only_warn = st.checkbox("⚠️ 要確認のみ表示", value=False)

    report_cols = [
        "_要確認",
        "_元_店舗名", "_名寄せ_店舗", "_店舗信頼度",
        "_元_メニュー名", "_推論_サービス", "_サービス信頼度",
        "_元_登録番号", "_正規化_登録番号",
    ]

    df_show = df_report[report_cols].copy()
    df_show.columns = [c.replace("_", "").replace("元", "元：").replace("名寄せ", "名寄せ：")
                       .replace("推論", "推論：").replace("正規化", "正規化：") for c in report_cols]

    if show_only_warn:
        df_show = df_show[df_report["_要確認"] == "⚠️ 要確認"]

    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.download_button(
        "📥 名寄せ確認レポートCSVをダウンロード",
        to_csv_bytes(df_report),
        f"変換レポート_{datetime.today().strftime('%Y%m%d')}.csv",
        "text/csv",
        use_container_width=True,
    )

with tab3:
    st.markdown('<div class="sec">サービス別 変換件数</div>', unsafe_allow_html=True)
    svc_count = df_result["サービス種別"].value_counts().rename("件数")
    st.bar_chart(svc_count, color="#2563eb")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**サービス別 件数**")
        st.dataframe(svc_count.reset_index(), use_container_width=True, hide_index=True)
    with col_b:
        st.markdown("**予約ステータス別 件数**")
        status_count = df_result["予約ステータス"].value_counts().rename("件数")
        st.dataframe(status_count.reset_index(), use_container_width=True, hide_index=True)

    st.markdown("**店舗別 件数**")
    store_count = df_result["入庫店舗ID"].value_counts().rename("件数")
    st.bar_chart(store_count, color="#059669")


# ──────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center;font-size:0.72rem;color:#94a3b8;padding:0.5rem 0;">
    予約CSV変換ツール v1.0 ─ オートウェーブ ─ 完全ローカル処理・データ永続化なし
</div>
""", unsafe_allow_html=True)
