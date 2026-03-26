# app.py - 众链充电原始表合并系统（独立网页，无数据库、无登录）

import os
import importlib.util
from datetime import date
import streamlit as st
import pandas as pd
from io import BytesIO

# 按路径加载 handler，避免 import handlers 触发无关依赖（需求 4.3）
def _load_handler(module_name: str, file_name: str):
    app_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(app_dir, "handlers", file_name)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_pile_mod = _load_handler("table_merge_handler", "table_merge_handler.py")
_station_mod = _load_handler("station_merge_handler", "station_merge_handler.py")
_clean_mod = _load_handler("data_clean_handler", "data_clean_handler.py")
_energy_mod = _load_handler("energy_merge_handler", "energy_merge_handler.py")
_generic_mod = _load_handler("generic_merge_handler", "generic_merge_handler.py")
_csv_mod = _load_handler("csv_convert_handler", "csv_convert_handler.py")
_pivot_mod = _load_handler("pivot_handler", "pivot_handler.py")

# Excel 2007+ / openpyxl 单工作表最大行数；超过则无法写入 .xlsx（与列数无关）
OPENPYXL_MAX_SHEET_ROWS = 1_048_576


def _write_cleaned_csv_and_maybe_xlsx(df: pd.DataFrame, out_csv: str, out_xlsx: str) -> bool:
    """先写 CSV；仅当行数不超过 Excel 单表上限时写 xlsx。返回是否已写入 xlsx。"""
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    if len(df) > OPENPYXL_MAX_SHEET_ROWS:
        return False
    df.to_excel(out_xlsx, index=False, engine="openpyxl")
    return True


def pile_merge_files(files, engine="openpyxl"):
    if _pile_mod is None:
        return None, [], ["公共桩合并模块加载失败"], []
    return _pile_mod.merge_files(files, engine=engine)

def station_merge_files(files, engine="openpyxl"):
    if _station_mod is None:
        return None, [], ["充电站合并规则配置中，暂不可用"], []
    return _station_mod.merge_files(files, engine=engine)

def pile_merge_files_to_csv(files, engine="openpyxl"):
    if _pile_mod is None or not hasattr(_pile_mod, "merge_files_to_csv"):
        return None, [], ["公共桩合并模块加载失败"], []
    return _pile_mod.merge_files_to_csv(files, engine=engine)

def station_merge_files_to_csv(files, engine="openpyxl"):
    if _station_mod is None or not hasattr(_station_mod, "merge_files_to_csv"):
        return None, [], ["充电站合并模块加载失败"], []
    return _station_mod.merge_files_to_csv(files, engine=engine)

def energy_merge_only(files, engine="openpyxl"):
    if _energy_mod is None or not hasattr(_energy_mod, "merge_only"):
        return None, [], ["电量表合并模块加载失败"], []
    return _energy_mod.merge_only(files, engine=engine)

def energy_merge_aggregate(files, engine="openpyxl"):
    if _energy_mod is None or not hasattr(_energy_mod, "merge_aggregate"):
        return None, [], ["电量表合并模块加载失败"], []
    return _energy_mod.merge_aggregate(files, engine=engine)

def generic_get_columns(files, engine="openpyxl"):
    if _generic_mod is None or not hasattr(_generic_mod, "get_columns_from_files"):
        return [], [], ["其他类型表格合并模块加载失败"]
    return _generic_mod.get_columns_from_files(files, engine=engine)

def generic_merge_vertical(files, selected_columns, engine="openpyxl"):
    if _generic_mod is None or not hasattr(_generic_mod, "merge_vertical"):
        return None, [], ["其他类型表格合并模块加载失败"]
    return _generic_mod.merge_vertical(files, selected_columns, engine=engine)

def generic_merge_horizontal(files, align_col, merge_columns, column_name_mode, with_aggregate, engine="openpyxl"):
    if _generic_mod is None or not hasattr(_generic_mod, "merge_horizontal"):
        return None, [], ["其他类型表格合并模块加载失败"]
    return _generic_mod.merge_horizontal(files, align_col, merge_columns, column_name_mode, with_aggregate, engine=engine)

def generic_run_validation(df, rules):
    if _generic_mod is None or not hasattr(_generic_mod, "run_validation"):
        return []
    return _generic_mod.run_validation(df, rules)

def csv_convert_excel(file_bytes: bytes, filename: str):
    """多 Sheet Excel 合并为单表，返回 (df, error, stats)。"""
    if _csv_mod is None or not hasattr(_csv_mod, "excel_sheets_to_csv"):
        return None, "CSV格式转换模块加载失败", None
    return _csv_mod.excel_sheets_to_csv(file_bytes, filename)


def csv_convert_by_path(input_path: str):
    """按本地路径读取 Excel 并合并为 CSV 保存到同目录，返回 (output_path, error, stats)。"""
    if _csv_mod is None or not hasattr(_csv_mod, "excel_path_to_csv"):
        return None, "CSV格式转换模块加载失败", None
    return _csv_mod.excel_path_to_csv(input_path)


def csv_convert_parse_paths(multiline_text: str):
    """多行路径字符串解析为路径列表。"""
    if _csv_mod is None or not hasattr(_csv_mod, "parse_paths_from_multiline"):
        return []
    return _csv_mod.parse_paths_from_multiline(multiline_text or "")


def csv_convert_paths_to_single(paths: list):
    """多表合并为一个 CSV，返回 (output_path, error, stats)。"""
    if _csv_mod is None or not hasattr(_csv_mod, "excel_paths_to_single_csv"):
        return None, "CSV格式转换模块加载失败", None
    return _csv_mod.excel_paths_to_single_csv(paths)


def csv_convert_paths_to_separate(paths: list):
    """多表分别转化为 CSV，返回 List[(output_path, error, stats)]。"""
    if _csv_mod is None or not hasattr(_csv_mod, "excel_paths_to_separate_csvs"):
        return []
    return _csv_mod.excel_paths_to_separate_csvs(paths)


def pivot_get_numeric_columns(df):
    if _pivot_mod is None or not hasattr(_pivot_mod, "get_numeric_columns"):
        return []
    return _pivot_mod.get_numeric_columns(df)


def pivot_build_table(df, index_cols, columns_cols, values_aggs):
    """values_aggs: List[(value_col, agg_name)]。返回 (result_df, error)。"""
    if _pivot_mod is None or not hasattr(_pivot_mod, "build_pivot_table"):
        return None, "数据透视表模块加载失败"
    return _pivot_mod.build_pivot_table(df, index_cols, columns_cols, values_aggs)


def pivot_filter_dataframe(df, filter_col=None, selected_values=None, where_expr=None):
    if _pivot_mod is None or not hasattr(_pivot_mod, "filter_dataframe"):
        return None, "数据透视表模块加载失败"
    return _pivot_mod.filter_dataframe(df, filter_col, selected_values, where_expr)


def pivot_get_distinct_values(df, column, limit=10000):
    if _pivot_mod is None or not hasattr(_pivot_mod, "get_distinct_values"):
        return None, "数据透视表模块加载失败"
    return _pivot_mod.get_distinct_values(df, column, limit)


def pivot_get_db_columns(db_type, host, port, user, password, database, table, schema=None):
    if _pivot_mod is None or not hasattr(_pivot_mod, "get_db_columns"):
        return None, "数据透视表模块加载失败"
    return _pivot_mod.get_db_columns(db_type, host, port, user, password, database, table, schema)


def pivot_get_db_distinct(db_type, host, port, user, password, database, table, column, limit=10000, schema=None):
    if _pivot_mod is None or not hasattr(_pivot_mod, "get_db_distinct_values"):
        return None, "数据透视表模块加载失败"
    return _pivot_mod.get_db_distinct_values(
        db_type, host, port, user, password, database, table, column, limit, schema
    )


def pivot_build_from_db(db_type, host, port, user, password, database, table, index_cols, columns_cols, values_aggs, where_clause=None, schema=None):
    if _pivot_mod is None or not hasattr(_pivot_mod, "build_pivot_from_db"):
        return None, "数据透视表模块未支持数据库"
    return _pivot_mod.build_pivot_from_db(
        db_type, host, port, user, password, database, table,
        index_cols, columns_cols, values_aggs, where_clause, schema
    )


def pivot_load_table_from_db(db_type, host, port, user, password, database, table, schema=None, limit=1_000_000):
    """从数据库表加载数据为 DataFrame，供数据清洗等使用。"""
    if _pivot_mod is None or not hasattr(_pivot_mod, "load_table_from_db"):
        return None, "数据透视表模块未支持"
    return _pivot_mod.load_table_from_db(
        db_type, host, port, user, password, database, table, schema, limit
    )


# 预览行数（需求：仅展示前 10 条）
PREVIEW_ROWS = 10


def _parse_error_list(error_list):
    """将 error_list（「文件名」原因）解析为 [{文件名, 未合并原因}, ...]。"""
    rows = []
    for e in error_list:
        e = str(e).strip()
        if e.startswith("「") and "」" in e:
            idx = e.index("」")
            rows.append({"文件名": e[1:idx].strip(), "未合并原因": e[idx + 1 :].strip()})
        else:
            rows.append({"文件名": "", "未合并原因": e})
    return rows


def _show_error_table(error_list):
    """在 expander 中以表格展示未合并文件及原因。"""
    if not error_list:
        return
    rows = _parse_error_list(error_list)
    with st.expander("⚠️ 未合并的文件及原因", expanded=True):
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# 标题区背景图（数据流/连接示意），作为标题内容背景、适应内容大小
APP_DIR = os.path.dirname(os.path.abspath(__file__))
HEADER_BANNER_IMAGE = os.path.join(APP_DIR, "assets", "header_banner.png")

def _header_banner_bg_css() -> str:
    if not os.path.exists(HEADER_BANNER_IMAGE):
        return ""
    import base64
    with open(HEADER_BANNER_IMAGE, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    return f"background-image: linear-gradient(to bottom, rgba(255,255,255,0.72) 0%, rgba(255,255,255,0.58) 100%), linear-gradient(to bottom, rgba(0,0,0,0.32) 0%, rgba(0,0,0,0.18) 100%), url(data:image/png;base64,{b64}); background-size: cover; background-position: center;"

st.set_page_config(
    page_title="众链充电原始表合并系统",
    page_icon="📎",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 侧栏与主区标题通用样式（功能栏图标卡片 + 标题区背景图）
SIDEBAR_OPTIONS_DISPLAY = ["🔌 公共桩多表合并", "⚡ 充电站多表合并", "📊 电量表多表合并", "📑 合并汇总其他类型表格", "📄 CSV格式转换", "📊 数据透视表", "🧹 数据清洗"]
SIDEBAR_OPTIONS_VALUE = ["公共桩多表合并", "充电站多表合并", "电量表多表合并", "合并汇总其他类型表格", "CSV格式转换", "数据透视表", "数据清洗"]

def _sidebar_display_to_value(display: str) -> str:
    for i, d in enumerate(SIDEBAR_OPTIONS_DISPLAY):
        if d == display or SIDEBAR_OPTIONS_VALUE[i] in (display or ""):
            return SIDEBAR_OPTIONS_VALUE[i]
    return SIDEBAR_OPTIONS_VALUE[0]

st.markdown("""
<style>
/* ===== 侧栏：功能选择、合并类型、选项卡片与图标 ===== */
[data-testid="stSidebar"] .stMarkdown h3 { font-size: 1.5rem; font-weight: 700; color: #0e1117; margin-bottom: 0.5rem; }
[data-testid="stSidebar"] .stRadio > label:first-child { font-size: 1.1rem; font-weight: 600; color: #31333F; }
[data-testid="stSidebar"] .stRadio label[data-testid="stWidgetLabel"] { font-size: 1.05rem; line-height: 1.5; font-weight: 500; color: #0e1117; }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] { gap: 0.5rem; }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] > label { min-height: 2.8rem; width: 100%; padding: 0.6rem 0.75rem; border-radius: 0.5rem; background: #f8f9fa; border: 1px solid #e9ecef; box-shadow: 0 1px 2px rgba(0,0,0,0.04); display: flex; align-items: center; gap: 0.5rem; transition: background 0.2s, border-color 0.2s; }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) { background: #e8f4fc; border-left: 3px solid #0d6efd; border-color: #cce5ff; box-shadow: 0 1px 3px rgba(13,110,253,0.12); font-weight: 600; }
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: 0.6rem; }
[data-testid="stSidebar"] { padding: 1.25rem 1rem 2rem 1rem; }
/* ===== 主区标题处：内容为标题+说明，背景为图（适应内容大小） ===== */
.header-banner { min-height: auto; border-radius: 12px; padding: 1.35rem 1.75rem; margin: -0.5rem 0 1.25rem 0; border: 1px solid #e8e0d4; box-shadow: 0 2px 10px rgba(0,0,0,0.06); }
.header-banner .header-inner { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.4rem; }
.header-banner .header-icon { font-size: 2rem; line-height: 1; }
.header-banner .header-title { font-size: 1.85rem; font-weight: 700; color: #0e1117; margin: 0; letter-spacing: 0.02em; text-shadow: 0 0 20px rgba(255,255,255,0.9); }
.header-banner .header-caption { font-size: 0.92rem; color: #374151; margin: 0; line-height: 1.45; text-shadow: 0 0 16px rgba(255,255,255,0.85); }
</style>
""", unsafe_allow_html=True)

# 若存在标题背景图，注入为 .header-banner 背景（适应内容大小）
_bg_css = _header_banner_bg_css()
if _bg_css:
    st.markdown(f"<style>.header-banner {{ {_bg_css} }}</style>", unsafe_allow_html=True)

# 侧栏：三选一（公共桩 / 充电站 / 数据清洗）
if "merge_mode" not in st.session_state:
    st.session_state.merge_mode = "公共桩多表合并"
if "main_view" not in st.session_state:
    st.session_state.main_view = "merge"  # merge | clean_upload | clean_after_merge
with st.sidebar:
    st.markdown("### 功能选择")
    _idx = SIDEBAR_OPTIONS_VALUE.index(st.session_state.merge_mode) if st.session_state.merge_mode in SIDEBAR_OPTIONS_VALUE else 0
    # 仅用 index 控制选中项，不通过 Session State 设 key，避免 “default value and Session State” 冲突
    _mode_display = st.radio(
        "合并类型",
        options=SIDEBAR_OPTIONS_DISPLAY,
        index=_idx,
        key="sidebar_merge_mode",
    )
    mode = _sidebar_display_to_value(_mode_display)

# 根据侧栏选择更新模式与主视图（从合并页切到数据清洗时设为 clean_upload，不覆盖已进入的 clean_after_merge）
if mode == "数据清洗":
    st.session_state.merge_mode = "数据清洗"
    if st.session_state.main_view == "merge":
        st.session_state.main_view = "clean_upload"
else:
    st.session_state.merge_mode = mode
    # 避免从「数据清洗」按钮跳转过来时被侧栏 radio 的旧值覆盖回 merge
    if st.session_state.main_view != "clean_after_merge":
        st.session_state.main_view = "merge"

is_pile = st.session_state.merge_mode == "公共桩多表合并"
is_energy = st.session_state.merge_mode == "电量表多表合并"
is_generic = st.session_state.merge_mode == "合并汇总其他类型表格"
is_csv_convert = st.session_state.merge_mode == "CSV格式转换"
is_pivot = st.session_state.merge_mode == "数据透视表"
is_clean_view = st.session_state.main_view in ("clean_upload", "clean_after_merge")

# ---------- 数据清洗页（双入口共用同一套 UI） ----------
if is_clean_view:
    st.markdown("""
    <div class="header-banner">
      <div class="header-inner">
        <span class="header-icon">🧹</span>
        <h1 class="header-title">数据清洗</h1>
      </div>
      <p class="header-caption">对合并结果或自备表进行清洗，规则可在此处配置与执行。</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    df_for_clean = st.session_state.get("df_for_clean") if st.session_state.main_view == "clean_after_merge" else None
    clean_source_from_path = st.session_state.get("clean_source_from_path", False)
    clean_source_paths = list(st.session_state.get("clean_source_paths") or [])
    if not clean_source_paths and st.session_state.get("clean_source_path"):
        clean_source_paths = [st.session_state.get("clean_source_path")]
    has_path_source = clean_source_from_path and len(clean_source_paths) > 0
    if df_for_clean is not None or has_path_source:
        st.caption("当前使用合并结果表、上传表或文件链接进行清洗。" if clean_source_from_path else "当前使用合并结果表或上传表进行清洗。")
        # 数据类型下拉：充电站数据 / 充电桩数据（仅在有 df 时自动检测）
        if "clean_table_type" not in st.session_state:
            if df_for_clean is not None and _clean_mod:
                _detected = _clean_mod._detect_table_type(df_for_clean)
                st.session_state.clean_table_type = _detected
            else:
                st.session_state.clean_table_type = "station"
        if "panel_custom_clean_open" not in st.session_state:
            st.session_state.panel_custom_clean_open = False
        st.caption("请先选择数据类型（充电站表 或 充电桩表），再执行一键清洗或自定义清洗。")
        st.markdown("**步骤 1：选择规则类型**")
        _type_options = ["充电站数据", "充电桩数据"]
        _type_values = ["station", "pile"]
        _current_idx = _type_values.index(st.session_state.clean_table_type) if st.session_state.clean_table_type in _type_values else 0
        _sel_display = st.selectbox(
            "数据类型",
            options=_type_options,
            index=_current_idx,
            key="clean_table_type_select",
            help="选择当前表格为充电站或充电桩数据，后续将按该类型适用规则清洗。",
        )
        _new_type = _type_values[_type_options.index(_sel_display)]
        if _new_type != st.session_state.clean_table_type:
            st.session_state.clean_table_type = _new_type
            st.session_state.pop("df_cleaned", None)
            st.session_state.pop("clean_report", None)
            st.session_state.pop("panel_custom_clean_open", None)
        clean_table_type = st.session_state.clean_table_type

        if df_for_clean is not None:
            st.markdown("#### 待清洗数据预览")
            st.dataframe(df_for_clean.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
            st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(df_for_clean):,} 行。")
        elif has_path_source:
            st.markdown("#### 数据来源")
            n_paths = len(clean_source_paths)
            st.caption(f"已配置 **{n_paths}** 个文件链接（换行分隔），将依次按当前规则清洗，每个结果保存到该文件所在文件夹。")
            if n_paths > 0:
                with st.expander("查看路径列表", expanded=False):
                    for i, p in enumerate(clean_source_paths[:10], 1):
                        st.text(p)
                    if n_paths > 10:
                        st.caption(f"… 共 {n_paths} 个")
        _path_done = st.session_state.pop("clean_path_done_msg", None)
        _batch_result = st.session_state.get("clean_path_batch_result")
        if _path_done:
            if _batch_result and len(_batch_result.get("fail", [])) > 0 and len(_batch_result.get("success", [])) == 0:
                st.warning(_path_done)
            else:
                st.success(_path_done)
        if _batch_result:
            _s, _f = _batch_result.get("success", []), _batch_result.get("fail", [])
            if len(_f) > 0:
                with st.expander("⚠️ 失败文件列表", expanded=True):
                    for x in _f:
                        st.text(f"{x.get('path', '')} — {x.get('err', '')}")
            if len(_s) > 0 and len(_s) <= 20:
                with st.expander("✅ 成功保存路径", expanded=False):
                    for x in _s:
                        _ox = x.get("out_xlsx")
                        _xlsx_note = _ox if _ox else f"未生成（>{OPENPYXL_MAX_SHEET_ROWS:,} 行）"
                        st.text(f"CSV: {x.get('out_csv', '')}  |  XLSX: {_xlsx_note}")
            elif len(_s) > 20:
                with st.expander("✅ 成功保存路径（前 20 个）", expanded=False):
                    for x in _s[:20]:
                        _ox = x.get("out_xlsx")
                        _xlsx_note = _ox if _ox else f"未生成（>{OPENPYXL_MAX_SHEET_ROWS:,} 行）"
                        st.text(f"CSV: {x.get('out_csv', '')}  |  XLSX: {_xlsx_note}")
                    st.caption(f"… 共 {len(_s)} 个")
        if _clean_mod is None:
            st.error("清洗模块加载失败，请检查 handlers/data_clean_handler.py 是否存在。")
        else:
            rules_for_type = _clean_mod.get_rules_for_table_type(clean_table_type)
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                do_clean = st.button("▶ 一键清洗", type="primary", key="do_data_clean", use_container_width=True)
            with col_btn2:
                show_custom = st.button("自定义清洗", key="btn_show_custom_clean", use_container_width=True)
            if do_clean:
                if has_path_source and df_for_clean is None:
                    _success, _fail = [], []
                    _last_df, _last_report = None, None
                    _n = len(clean_source_paths)
                    with st.spinner(f"正在依次清洗（共 {_n} 个文件）…" if _n > 1 else "正在清洗中..."):
                        for _idx, _path in enumerate(clean_source_paths):
                            try:
                                _low = _path.lower()
                                if _low.endswith(".csv"):
                                    try:
                                        _work_df = pd.read_csv(_path, encoding="utf-8-sig")
                                    except Exception:
                                        _work_df = pd.read_csv(_path, encoding="gbk")
                                elif _low.endswith(".xlsx") or _low.endswith(".xls"):
                                    _work_df = pd.read_excel(_path, sheet_name=0, engine="openpyxl" if _low.endswith(".xlsx") else "xlrd")
                                else:
                                    _work_df = None
                                if _work_df is None or _work_df.empty:
                                    _fail.append({"path": _path, "err": "表格为空或无法解析"})
                                    continue
                                cleaned_df, report = _clean_mod.clean_dataframe(_work_df, table_type=clean_table_type)
                                _dir = os.path.dirname(_path)
                                _base = os.path.splitext(os.path.basename(_path))[0]
                                out_csv = os.path.join(_dir, f"{_base}_已清洗.csv")
                                out_xlsx = os.path.join(_dir, f"{_base}_已清洗.xlsx")
                                _wrote_xlsx = _write_cleaned_csv_and_maybe_xlsx(cleaned_df, out_csv, out_xlsx)
                                _success.append(
                                    {
                                        "path": _path,
                                        "out_csv": out_csv,
                                        "out_xlsx": out_xlsx if _wrote_xlsx else None,
                                    }
                                )
                                _last_df, _last_report = cleaned_df, report
                            except Exception as e:
                                _fail.append({"path": _path, "err": str(e)})
                    st.session_state.df_cleaned = _last_df
                    st.session_state.clean_report = _last_report
                    st.session_state.clean_path_batch_result = {"success": _success, "fail": _fail}
                    if _n == 1 and _success:
                        _s0 = _success[0]
                        if _s0.get("out_xlsx"):
                            st.session_state.clean_path_done_msg = (
                                f"结果已保存到：{_s0['out_csv']}，{_s0['out_xlsx']}"
                            )
                        else:
                            st.session_state.clean_path_done_msg = (
                                f"结果已保存 CSV：{_s0['out_csv']}（行数超过 Excel 单表上限 {OPENPYXL_MAX_SHEET_ROWS:,}，未生成 xlsx）"
                            )
                    elif _n == 1 and _fail:
                        st.session_state.clean_path_done_msg = "该文件清洗失败，请查看下方失败列表。"
                    else:
                        st.session_state.clean_path_done_msg = f"共 {_n} 个文件，成功 {len(_success)} 个，失败 {len(_fail)} 个。"
                    st.rerun()
                elif df_for_clean is not None:
                    with st.spinner("正在清洗中..."):
                        try:
                            cleaned_df, report = _clean_mod.clean_dataframe(df_for_clean, table_type=clean_table_type)
                            st.session_state.df_cleaned = cleaned_df
                            st.session_state.clean_report = report
                            st.rerun()
                        except Exception as e:
                            st.error(f"清洗失败：{e}")
                            import traceback
                            with st.expander("错误详情"):
                                st.code(traceback.format_exc())
            if show_custom:
                st.session_state.panel_custom_clean_open = True
            if st.session_state.get("panel_custom_clean_open", False):
                st.markdown("##### 选择要执行的清洗规则")
                rule_ids_selected = []
                _RULE_SEQ = getattr(_clean_mod, "RULE_SEQUENCE", "sequence")
                for rid, label in rules_for_type:
                    if rid == _RULE_SEQ and clean_table_type == "pile":
                        _c1, _c2 = st.columns([0.58, 0.42])
                        with _c1:
                            if st.checkbox(label, key=f"clean_rule_{rid}", value=True):
                                rule_ids_selected.append(rid)
                        with _c2:
                            st.number_input(
                                "序号起始",
                                min_value=1,
                                value=1,
                                step=1,
                                key="clean_sequence_start",
                                help="勾选「序号列」时从该值递增（如 10→10,11,12…）；仅勾选 uid 且表内无数号时，uid 也会用该值自动补序号。",
                            )
                    else:
                        if st.checkbox(label, key=f"clean_rule_{rid}", value=True):
                            rule_ids_selected.append(rid)
                do_custom = st.button("执行自定义清洗", type="primary", key="do_custom_clean", use_container_width=False)
                if do_custom:
                    _pile_seq_start = (
                        int(st.session_state.get("clean_sequence_start", 1))
                        if clean_table_type == "pile"
                        else 1
                    )
                    if not rule_ids_selected:
                        st.warning("请至少勾选一项清洗规则。")
                    elif has_path_source and df_for_clean is None:
                        _success, _fail = [], []
                        _last_df, _last_report = None, None
                        _n = len(clean_source_paths)
                        with st.spinner("正在依次清洗…" if _n > 1 else "正在清洗中..."):
                            for _idx, _path in enumerate(clean_source_paths):
                                try:
                                    _low = _path.lower()
                                    if _low.endswith(".csv"):
                                        try:
                                            _work_df = pd.read_csv(_path, encoding="utf-8-sig")
                                        except Exception:
                                            _work_df = pd.read_csv(_path, encoding="gbk")
                                    elif _low.endswith(".xlsx") or _low.endswith(".xls"):
                                        _work_df = pd.read_excel(_path, sheet_name=0, engine="openpyxl" if _low.endswith(".xlsx") else "xlrd")
                                    else:
                                        _work_df = None
                                    if _work_df is None or _work_df.empty:
                                        _fail.append({"path": _path, "err": "表格为空或无法解析"})
                                        continue
                                    cleaned_df, report = _clean_mod.clean_dataframe(
                                        _work_df,
                                        table_type=clean_table_type,
                                        rules_to_apply=rule_ids_selected,
                                        sequence_start=_pile_seq_start,
                                    )
                                    _dir = os.path.dirname(_path)
                                    _base = os.path.splitext(os.path.basename(_path))[0]
                                    out_csv = os.path.join(_dir, f"{_base}_已清洗.csv")
                                    out_xlsx = os.path.join(_dir, f"{_base}_已清洗.xlsx")
                                    _wrote_xlsx = _write_cleaned_csv_and_maybe_xlsx(cleaned_df, out_csv, out_xlsx)
                                    _success.append(
                                        {
                                            "path": _path,
                                            "out_csv": out_csv,
                                            "out_xlsx": out_xlsx if _wrote_xlsx else None,
                                        }
                                    )
                                    _last_df, _last_report = cleaned_df, report
                                except Exception as e:
                                    _fail.append({"path": _path, "err": str(e)})
                        st.session_state.df_cleaned = _last_df
                        st.session_state.clean_report = _last_report
                        st.session_state.clean_path_batch_result = {"success": _success, "fail": _fail}
                        if _n == 1 and _success:
                            _s0 = _success[0]
                            if _s0.get("out_xlsx"):
                                st.session_state.clean_path_done_msg = (
                                    f"结果已保存到：{_s0['out_csv']}，{_s0['out_xlsx']}"
                                )
                            else:
                                st.session_state.clean_path_done_msg = (
                                    f"结果已保存 CSV：{_s0['out_csv']}（行数超过 Excel 单表上限 {OPENPYXL_MAX_SHEET_ROWS:,}，未生成 xlsx）"
                                )
                        elif _n == 1 and _fail:
                            st.session_state.clean_path_done_msg = "该文件清洗失败，请查看下方失败列表。"
                        else:
                            st.session_state.clean_path_done_msg = f"共 {_n} 个文件，成功 {len(_success)} 个，失败 {len(_fail)} 个。"
                        st.session_state.panel_custom_clean_open = False
                        st.rerun()
                    else:
                        with st.spinner("正在清洗中..."):
                            try:
                                cleaned_df, report = _clean_mod.clean_dataframe(
                                    df_for_clean,
                                    table_type=clean_table_type,
                                    rules_to_apply=rule_ids_selected,
                                    sequence_start=_pile_seq_start,
                                )
                                st.session_state.df_cleaned = cleaned_df
                                st.session_state.clean_report = report
                                st.session_state.panel_custom_clean_open = False
                                st.rerun()
                            except Exception as e:
                                st.error(f"清洗失败：{e}")
                                import traceback
                                with st.expander("错误详情"):
                                    st.code(traceback.format_exc())
        df_cleaned = st.session_state.get("df_cleaned")
        clean_report = st.session_state.get("clean_report")
        if df_cleaned is not None and clean_report is not None:
            st.markdown("---")
            st.markdown("#### 清洗结果")
            applied_rules = clean_report.get("applied_rules", [])
            if applied_rules:
                for i, (_rid, label) in enumerate(applied_rules, 1):
                    st.markdown(f"{i}、{label} 清洗完成 ✅")
            st.success("清洗完成")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("日期清洗成功", clean_report.get("date_clean_success", 0))
            with c2:
                st.metric("日期清洗失败", clean_report.get("date_clean_fail", 0))
            with c3:
                st.metric("W→kW 换算条数", clean_report.get("power_w_to_kw_count", 0))
            with c4:
                missing = clean_report.get("station_inner_id_missing_rows", [])
                st.metric("充电站内部编号缺失", len(missing))
            c5, c6, c7 = st.columns(3)
            with c5:
                st.metric("日期00日→01修正", clean_report.get("date_zero_day_fixed_count", 0))
            with c6:
                st.metric("电表号截断单元格", clean_report.get("meter_truncated_count", 0))
            with c7:
                st.metric("Excel序列号→日期", clean_report.get("date_excel_serial_converted_count", 0))
            unknown_fmts = clean_report.get("date_unknown_formats", [])
            if unknown_fmts:
                with st.expander("⚠️ 无法识别的日期格式（样例）", expanded=False):
                    for fmt in unknown_fmts[:20]:
                        st.code(fmt, language=None)
            if missing:
                with st.expander("⚠️ 充电站内部编号缺失记录", expanded=False):
                    st.dataframe(pd.DataFrame(missing), use_container_width=True, hide_index=True)
            anomaly = clean_report.get("pile_open_time_anomaly_rows", [])
            if anomaly:
                with st.expander("⚠️ 设备开通时间异常（晚于当前时间）", expanded=False):
                    st.dataframe(pd.DataFrame(anomaly), use_container_width=True, hide_index=True)
            psql_added = clean_report.get("psql_added_columns", [])
            if psql_added:
                with st.expander("根据 psql 新增字段", expanded=False):
                    st.caption("本次新增列（取值为空）：")
                    st.write(", ".join(psql_added))
            latlon_invalid = clean_report.get("latlon_invalid_rows", [])
            latlon_count = clean_report.get("latlon_invalid_count", 0)
            if latlon_count > 0:
                with st.expander("⚠️ 经纬度不合理（已标记为NULL）", expanded=False):
                    st.caption(f"共 {latlon_count} 处异常；修正方式：标记为NULL。")
                    st.dataframe(pd.DataFrame(latlon_invalid), use_container_width=True, hide_index=True)
            phone_cleaned = clean_report.get("phone_cleaned_count", 0)
            phone_examples = clean_report.get("phone_abnormal_examples", [])
            if phone_cleaned > 0:
                with st.expander("📞 联系电话过长/异常审查", expanded=False):
                    st.caption(f"清洗行数：{phone_cleaned}；修正方式：仅保留数字与逗号、截断至50字或置空。")
                    if phone_examples:
                        st.caption("异常值示例：")
                        st.write(", ".join(phone_examples[:15]))
                        if len(phone_examples) > 15:
                            st.caption(f"… 共 {len(phone_examples)} 条")
            st.markdown("#### 清洗后数据预览")
            st.dataframe(df_cleaned.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
            st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(df_cleaned):,} 行。")
            _export_date = date.today().strftime("%Y%m%d")
            _default_name = f"已清洗_{_export_date}"
            _export_name = st.text_input(
                "导出文件名（可修改，不含扩展名）",
                value=_default_name,
                key="clean_export_filename",
                help="修改后点击下方「下载 Excel」或「下载 CSV」即可导出。",
            )
            _base = (_export_name.strip() or _default_name).replace("\\", "_").replace("/", "_").replace(":", "_")
            _name_xlsx = f"{_base}.xlsx"
            _name_csv = f"{_base}.csv"
            d1, d2 = st.columns(2)
            with d1:
                if len(df_cleaned) <= OPENPYXL_MAX_SHEET_ROWS:
                    buf = BytesIO()
                    df_cleaned.to_excel(buf, index=False, engine="openpyxl")
                    buf.seek(0)
                    st.download_button(
                        "下载清洗后 Excel",
                        data=buf.getvalue(),
                        file_name=_name_xlsx,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_cleaned_xlsx",
                    )
                else:
                    st.caption(
                        f"当前 {len(df_cleaned):,} 行，超过 Excel 单表上限 {OPENPYXL_MAX_SHEET_ROWS:,} 行，无法生成 xlsx，请用右侧 CSV 下载。"
                    )
            with d2:
                buf_csv = BytesIO()
                df_cleaned.to_csv(buf_csv, index=False, encoding="utf-8-sig")
                buf_csv.seek(0)
                st.download_button("下载清洗后 CSV", data=buf_csv.getvalue(), file_name=_name_csv, mime="text/csv", key="download_cleaned_csv")
            clean_save_dir = st.session_state.get("clean_save_dir")
            clean_source_basename = st.session_state.get("clean_source_basename")
            if clean_save_dir and clean_source_basename:
                st.markdown("#### 保存到链接所在文件夹")
                st.caption("数据由「输入文件链接」加载时，可将清洗结果直接写入该路径所在文件夹。")
                _saved_msg = st.session_state.get("clean_saved_to_path_msg")
                if _saved_msg:
                    st.success(_saved_msg)
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("保存 CSV 到链接文件夹", key="clean_save_csv_path"):
                        out_csv = os.path.join(clean_save_dir, f"{clean_source_basename}_已清洗.csv")
                        try:
                            df_cleaned.to_csv(out_csv, index=False, encoding="utf-8-sig")
                            st.session_state.clean_saved_to_path_msg = f"已保存：{out_csv}"
                            st.rerun()
                        except Exception as e:
                            st.error(f"保存失败：{e}")
                with c2:
                    _xlsx_too_big = len(df_cleaned) > OPENPYXL_MAX_SHEET_ROWS
                    if st.button(
                        "保存 Excel 到链接文件夹",
                        key="clean_save_xlsx_path",
                        disabled=_xlsx_too_big,
                        help=(
                            f"超过 {OPENPYXL_MAX_SHEET_ROWS:,} 行时 Excel 无法容纳整张表"
                            if _xlsx_too_big
                            else None
                        ),
                    ):
                        out_xlsx = os.path.join(clean_save_dir, f"{clean_source_basename}_已清洗.xlsx")
                        try:
                            df_cleaned.to_excel(out_xlsx, index=False, engine="openpyxl")
                            st.session_state.clean_saved_to_path_msg = f"已保存：{out_xlsx}"
                            st.rerun()
                        except Exception as e:
                            st.error(f"保存失败：{e}")
                    if _xlsx_too_big:
                        st.caption(
                            f"Excel 单表最多 {OPENPYXL_MAX_SHEET_ROWS:,} 行，请使用「保存 CSV 到链接文件夹」。"
                        )
        with st.expander("📋 清洗规则说明"):
            st.markdown("详见项目内《数据清洗规则》文档（`数据清洗规则.md`）。主要包含：通用空值/序号/位置截断、日期统一为 yyyy/mm/dd 与清洗结果标记、功率→kW/电压→V/电流→A、充电站内部编号缺失校验、充电桩设备类型标准化与设备开通时间校验。")
    else:
        # clean_upload：选择数据导入方式后再加载
        st.caption("请选择数据导入方式：直接导入（小量级）、连接数据库、或输入文件链接（清洗结果可保存到链接所在文件夹）。")
        clean_import_mode = st.radio(
            "数据导入方式",
            options=["直接导入", "连接数据库", "输入文件链接"],
            key="clean_import_mode",
            horizontal=True,
            help="直接导入：网页上传文件，适合小量数据。连接数据库：与数据透视表相同的连接方式。输入文件链接：填写本机路径，清洗后可直接保存到该路径所在文件夹。",
        )
        df_loaded = None
        upload_clean = None
        if clean_import_mode == "直接导入":
            st.caption("适合小量级数据，上传后在此进行清洗。")
            upload_clean = st.file_uploader(
                "上传待清洗的表格",
                type=["xlsx", "xls", "csv"],
                accept_multiple_files=False,
                key="clean_upload_file",
                help="支持 .xlsx / .xls / .csv",
            )
            if upload_clean is not None:
                try:
                    if upload_clean.name.lower().endswith(".csv"):
                        try:
                            df_loaded = pd.read_csv(BytesIO(upload_clean.getvalue()), encoding="utf-8-sig")
                        except Exception:
                            df_loaded = pd.read_csv(BytesIO(upload_clean.getvalue()), encoding="gbk")
                    else:
                        df_loaded = pd.read_excel(BytesIO(upload_clean.getvalue()), engine="openpyxl" if upload_clean.name.lower().endswith(".xlsx") else "xlrd")
                    if df_loaded is not None and not df_loaded.empty:
                        st.session_state.pop("clean_save_dir", None)
                        st.session_state.pop("clean_source_basename", None)
                        st.session_state.pop("clean_source_path", None)
                        st.session_state.pop("clean_source_paths", None)
                        st.session_state.pop("clean_source_from_path", None)
                        st.session_state.df_for_clean = df_loaded
                        st.session_state.pop("df_cleaned", None)
                        st.session_state.pop("clean_report", None)
                        st.session_state.main_view = "clean_after_merge"
                        st.rerun()
                    else:
                        df_loaded = None
                        st.warning("表格为空或无法解析。")
                except Exception as e:
                    st.error(f"读取文件失败：{e}")
            else:
                st.info("👆 请上传一个 Excel 或 CSV 文件。")
        elif clean_import_mode == "连接数据库":
            st.caption("与数据透视表相同的连接设置；加载后在此进行清洗。大表将按「最大行数」限制拉取，避免超时。")
            with st.expander("数据库连接", expanded=True):
                _db_type_label = st.radio("数据库类型", ["MySQL", "PostgreSQL (psql)"], key="clean_db_type_radio", horizontal=True)
                _db_type = "psql" if "PostgreSQL" in _db_type_label else "mysql"
                _suffix = "_psql" if _db_type == "psql" else "_mysql"
                _default_port = 5432 if _db_type == "psql" else 3306
                if _db_type == "psql":
                    _def_host, _def_user, _def_pass = "localhost", "postgres", "Admin2026"
                    _def_db, _def_schema, _def_table = "evdata", "rowdata", "evdata_2512_row"
                else:
                    _def_host = _def_user = _def_pass = _def_db = _def_schema = _def_table = ""
                _port_key = "clean_db_port_psql" if _db_type == "psql" else "clean_db_port_mysql"
                _host = st.text_input("主机", value=st.session_state.get("clean_db_host" + _suffix, _def_host), key="clean_db_host" + _suffix)
                _port = st.number_input("端口", value=int(st.session_state.get(_port_key, _default_port)), min_value=1, max_value=65535, key=_port_key)
                _user = st.text_input("用户名", value=st.session_state.get("clean_db_user" + _suffix, _def_user), key="clean_db_user" + _suffix)
                _pass = st.text_input("密码", type="password", value=st.session_state.get("clean_db_pass" + _suffix, _def_pass), key="clean_db_pass" + _suffix)
                _db_name = st.text_input("数据库名", value=st.session_state.get("clean_db_name" + _suffix, _def_db), key="clean_db_name" + _suffix)
                if _db_type == "psql":
                    _schema = st.text_input("Schema", value=st.session_state.get("clean_db_schema", _def_schema), key="clean_db_schema")
                else:
                    _schema = None
                _table = st.text_input("表名", value=st.session_state.get("clean_db_table" + _suffix, _def_table), key="clean_db_table" + _suffix)
                _limit = st.number_input("最大行数", value=int(st.session_state.get("clean_db_limit", 1_000_000)), min_value=1, max_value=10_000_000, key="clean_db_limit", help="为避免大表一次性拉取导致超时或内存不足，可限制行数。")
            if st.button("加载数据", type="primary", key="clean_db_load"):
                if not _host or not _user or not _db_name or not _table:
                    st.warning("请填写主机、用户名、数据库名和表名。")
                else:
                    with st.spinner("正在从数据库加载..."):
                        df_loaded, err = pivot_load_table_from_db(_db_type, _host, _port, _user, _pass, _db_name, _table, _schema, _limit)
                        if err:
                            st.error(err)
                        elif df_loaded is not None and not df_loaded.empty:
                            st.session_state.pop("clean_save_dir", None)
                            st.session_state.pop("clean_source_basename", None)
                            st.session_state.pop("clean_source_path", None)
                            st.session_state.pop("clean_source_paths", None)
                            st.session_state.pop("clean_source_from_path", None)
                            st.session_state.df_for_clean = df_loaded
                            st.session_state.pop("df_cleaned", None)
                            st.session_state.pop("clean_report", None)
                            st.session_state.main_view = "clean_after_merge"
                            st.rerun()
                        else:
                            st.warning("未读取到数据或表为空。")
        else:
            # 输入文件链接：多行，每行一个路径，换行分隔；全部校验通过后进入配置，依次清洗并保存到各文件所在文件夹
            st.caption("每行一个本机 Excel/CSV 路径，将依次按相同规则清洗，每个结果保存到该文件所在文件夹。")
            _path_input = st.text_area(
                "文件路径（每行一个）",
                value=st.session_state.get("clean_path_input", ""),
                key="clean_path_input",
                placeholder="每行一个文件路径，例如：\nC:\\data\\ev1.csv\nC:\\data\\ev2.xlsx\nD:\\export\\station.xlsx",
                height=120,
            )
            if st.button("开始配置清洗", type="primary", key="clean_path_confirm"):
                lines = [ln.strip().strip('"').strip("'") for ln in (_path_input or "").splitlines() if ln.strip()]
                if not lines:
                    st.warning("请输入至少一个文件路径（每行一个）。")
                else:
                    paths = [os.path.abspath(p) for p in lines]
                    errors = []
                    for i, p in enumerate(paths):
                        if not os.path.isfile(p):
                            errors.append(f"第 {i + 1} 行：文件不存在")
                        else:
                            low = p.lower()
                            if not (low.endswith(".csv") or low.endswith(".xlsx") or (low.endswith(".xls") and not low.endswith(".xlsx"))):
                                errors.append(f"第 {i + 1} 行：仅支持 .csv / .xlsx / .xls")
                    if errors:
                        for e in errors:
                            st.error(e)
                    else:
                        st.session_state.clean_source_paths = paths
                        st.session_state.clean_source_from_path = True
                        st.session_state.pop("clean_source_path", None)
                        st.session_state.pop("clean_save_dir", None)
                        st.session_state.pop("clean_source_basename", None)
                        st.session_state.pop("df_for_clean", None)
                        st.session_state.pop("df_cleaned", None)
                        st.session_state.pop("clean_report", None)
                        st.session_state.pop("clean_path_batch_result", None)
                        st.session_state.main_view = "clean_after_merge"
                        st.rerun()
        if clean_import_mode == "直接导入" and upload_clean is None:
            with st.expander("📋 清洗规则说明"):
                st.markdown("详见《数据清洗规则》文档（`数据清洗规则.md`）。")
        elif clean_import_mode == "连接数据库":
            with st.expander("📋 清洗规则说明"):
                st.markdown("详见《数据清洗规则》文档（`数据清洗规则.md`）。")
        elif clean_import_mode == "输入文件链接":
            with st.expander("📋 清洗规则说明"):
                st.markdown("详见《数据清洗规则》文档（`数据清洗规则.md`）。")
    st.stop()

# ---------- 电量表多表合并页 ----------
if is_energy:
    st.markdown("""
    <div class="header-banner">
      <div class="header-inner">
        <span class="header-icon">📊</span>
        <h1 class="header-title">电量表多表合并</h1>
      </div>
      <p class="header-caption">上传多个电量表 Excel/CSV，支持仅合并（纵向拼接）或按省级行政区域合并汇总。</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    energy_upload = st.file_uploader(
        "选择要合并的 Excel 或 CSV 文件（可多选）",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="energy_table_merge_upload",
        help="支持 .xlsx / .xls / .csv；表头将自动识别「省级行政区域名称」或「月度充电电量」。",
    )
    if energy_upload:
        st.markdown("#### 📁 已选文件")
        file_list = []
        for i, f in enumerate(energy_upload, 1):
            size_mb = f.size / (1024 * 1024)
            file_list.append({"序号": i, "文件名": f.name, "大小 (MB)": f"{size_mb:.2f}"})
        st.dataframe(pd.DataFrame(file_list), use_container_width=True, hide_index=True)
        col_only, col_agg = st.columns(2)
        with col_only:
            do_merge_only = st.button("仅合并", type="primary", key="energy_do_merge_only", use_container_width=True)
        with col_agg:
            do_merge_agg = st.button("合并汇总", type="primary", key="energy_do_merge_aggregate", use_container_width=True)
        if do_merge_only:
            files = [(f.name, f.getvalue()) for f in energy_upload]
            with st.spinner("正在仅合并..."):
                try:
                    merged_df, success_list, error_list, row_counts = energy_merge_only(files)
                    if merged_df is not None:
                        st.session_state.merge_result_df = merged_df
                        st.session_state.merge_result_success_list = success_list
                        st.session_state.merge_result_error_list = error_list or []
                        st.session_state.merge_result_row_counts = row_counts or []
                        st.session_state.merge_result_mode = "energy"
                        st.session_state.merge_result_energy_type = "only"
                        st.rerun()
                    else:
                        st.error("没有可合并的数据。")
                        if error_list:
                            _show_error_table(error_list)
                except Exception as e:
                    st.error(f"合并失败：{e}")
                    import traceback
                    with st.expander("错误详情", expanded=False):
                        st.code(traceback.format_exc())
        if do_merge_agg:
            files = [(f.name, f.getvalue()) for f in energy_upload]
            with st.spinner("正在合并汇总..."):
                try:
                    merged_df, success_list, error_list, row_counts = energy_merge_aggregate(files)
                    if merged_df is not None:
                        st.session_state.merge_result_df = merged_df
                        st.session_state.merge_result_success_list = success_list
                        st.session_state.merge_result_error_list = error_list or []
                        st.session_state.merge_result_row_counts = row_counts or []
                        st.session_state.merge_result_mode = "energy"
                        st.session_state.merge_result_energy_type = "aggregate"
                        st.rerun()
                    else:
                        st.error("没有可合并的数据。")
                        if error_list:
                            _show_error_table(error_list)
                except Exception as e:
                    st.error(f"合并汇总失败：{e}")
                    import traceback
                    with st.expander("错误详情", expanded=False):
                        st.code(traceback.format_exc())

    # 电量表结果区
    has_energy_result = (
        st.session_state.get("merge_result_mode") == "energy"
        and "merge_result_df" in st.session_state
        and st.session_state.merge_result_df is not None
    )
    if has_energy_result:
        merged_df = st.session_state.merge_result_df
        success_list = st.session_state.get("merge_result_success_list", [])
        error_list = st.session_state.get("merge_result_error_list", [])
        row_counts = st.session_state.get("merge_result_row_counts", [])
        energy_type = st.session_state.get("merge_result_energy_type", "only")
        st.success("合并完成（仅合并）" if energy_type == "only" else "合并汇总完成")
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("合并表行数", f"{len(merged_df):,}")
        with m2:
            st.metric("合并成功数", len(success_list))
        with m3:
            st.metric("未合并文件数", len(error_list), delta=None if not error_list else "见下方")
        if row_counts:
            st.markdown("**各表行数**")
            st.dataframe(
                pd.DataFrame(row_counts, columns=["文件名", "行数"]),
                use_container_width=True,
                hide_index=True,
            )
        st.markdown("#### 📥 导出")
        _export_base = "电量表仅合并结果" if energy_type == "only" else "电量表合并汇总结果"
        _export_date = date.today().strftime("%Y%m%d")
        _default_name = f"{_export_base}_{_export_date}"
        _export_name = st.text_input(
            "导出文件名（可修改，不含扩展名）",
            value=_default_name,
            key="energy_export_filename",
            help="修改后点击下方「下载 Excel」或「下载 CSV」即可导出。",
        )
        _base = (_export_name.strip() or _default_name).replace("\\", "_").replace("/", "_").replace(":", "_")
        _name_xlsx = f"{_base}.xlsx"
        _name_csv = f"{_base}.csv"
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            merged_df.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button(
                "下载 Excel",
                data=buf.getvalue(),
                file_name=_name_xlsx,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="energy_download_xlsx",
            )
        with c2:
            buf_csv = BytesIO()
            merged_df.to_csv(buf_csv, index=False, encoding="utf-8-sig")
            buf_csv.seek(0)
            st.download_button(
                "下载 CSV",
                data=buf_csv.getvalue(),
                file_name=_name_csv,
                mime="text/csv",
                key="energy_download_csv",
            )
        if error_list:
            st.markdown("---")
            _show_error_table(error_list)
        st.markdown("#### 📊 结果预览")
        st.dataframe(merged_df.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
        st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(merged_df):,} 行。")
        with st.expander("📋 合并规则说明"):
            st.markdown("""
- **表头**：前 3 行中先出现「省级行政区域名称」或「月度充电电量」的行作为表头。
- **Sheet**：仅一个有内容则用该 Sheet；多个则 sheet1/sheet2 形式用 sheet1，日期命名用最新日期，否则用第一个有内容的 Sheet。
- **仅合并**：每表增加「文件名称」「运营商名称」及七个电量字段后纵向拼接。
- **合并汇总**：同上增加字段后按省级行政区域名称加总（月度充电电量及七项电量字段求和）。
- **运营商名称**：由文件名按运营商映射表识别，无匹配填「未识别」。
            """)
    elif energy_upload and not has_energy_result:
        st.info("请点击「仅合并」或「合并汇总」执行合并。")
    else:
        st.info("👆 请选择至少一个 Excel 或 CSV 文件，然后点击「仅合并」或「合并汇总」。")
        with st.expander("📋 合并规则说明"):
            st.markdown("详见《电量表合并规则》文档（`电量表合并规则.md`）。")
    st.stop()

# ---------- 合并汇总其他类型表格页 ----------
if is_generic:
    st.markdown("""
    <div class="header-banner">
      <div class="header-inner">
        <span class="header-icon">📑</span>
        <h1 class="header-title">合并汇总其他类型表格</h1>
      </div>
      <p class="header-caption">上传多张数据表后，通过下拉框配置合并方向、字段与对齐方式，支持纵向拼接或横向按键对齐合并。</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    generic_upload = st.file_uploader(
        "选择要合并的 Excel 或 CSV 文件（可多选）",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="generic_table_merge_upload",
        help="支持 .xlsx / .xls / .csv，首行为表头。",
    )
    if generic_upload:
        files_tuples = [(f.name, f.getvalue()) for f in generic_upload]
        st.markdown("#### 📁 已选文件")
        file_list = [{"序号": i, "文件名": f.name, "大小 (MB)": f"{f.size / (1024*1024):.2f}"} for i, f in enumerate(generic_upload, 1)]
        st.dataframe(pd.DataFrame(file_list), use_container_width=True, hide_index=True)
        cols_list, _success, _errs = generic_get_columns(files_tuples)
        if not cols_list and _errs:
            st.warning("无法从已选文件中解析表头，请检查文件格式。")
            for e in _errs[:5]:
                st.caption(e)
        else:
            merge_direction = st.radio("合并方向", options=["纵向", "横向"], index=0, key="generic_merge_direction", horizontal=True)
            if merge_direction == "纵向":
                vertical_fields = st.multiselect("纵向合并字段", options=cols_list or [], default=cols_list[:3] if cols_list else [], key="generic_vertical_fields", help="选择需要纵向合并的列")
                run_vertical = st.button("执行合并", type="primary", key="generic_run_vertical")
                if run_vertical:
                    if not vertical_fields:
                        st.error("请至少选择一列纵向合并字段。")
                    else:
                        with st.spinner("正在纵向合并..."):
                            try:
                                out_df, succ, errs = generic_merge_vertical(files_tuples, vertical_fields)
                                if out_df is not None:
                                    st.session_state.merge_result_df = out_df
                                    st.session_state.merge_result_success_list = succ
                                    st.session_state.merge_result_error_list = errs or []
                                    st.session_state.merge_result_mode = "generic"
                                    st.rerun()
                                else:
                                    st.error("合并失败或无数据。")
                                    if errs:
                                        _show_error_table(errs)
                            except Exception as e:
                                st.error(f"合并失败：{e}")
                                import traceback
                                with st.expander("错误详情", expanded=False):
                                    st.code(traceback.format_exc())
            else:
                col_name_mode = st.selectbox("新增列名称", options=["表名称", "表名称去重"], index=0, key="generic_col_name_mode", help="横向合并时每表对应新列的名称来源")
                align_col = st.selectbox("横向对齐字段", options=cols_list or [], index=0 if cols_list else 0, key="generic_align_col", help="按此列取值对齐不同表的行")
                horizontal_fields = st.multiselect("横向合并字段", options=cols_list or [], default=cols_list[:2] if cols_list else [], key="generic_horizontal_fields", help="这些列将按对齐结果以新列形式追加")
                merge_mode_h = st.radio("合并方式", options=["仅合并", "合并+汇总"], index=0, key="generic_merge_mode_h", horizontal=True)
                run_horizontal = st.button("执行合并", type="primary", key="generic_run_horizontal")
                if run_horizontal:
                    if not align_col or not horizontal_fields:
                        st.error("请选择横向对齐字段和至少一列横向合并字段。")
                    else:
                        with st.spinner("正在横向合并..."):
                            try:
                                out_df, succ, errs = generic_merge_horizontal(
                                    files_tuples, align_col, horizontal_fields, col_name_mode, merge_mode_h == "合并+汇总"
                                )
                                if out_df is not None:
                                    st.session_state.merge_result_df = out_df
                                    st.session_state.merge_result_success_list = succ
                                    st.session_state.merge_result_error_list = errs or []
                                    st.session_state.merge_result_mode = "generic"
                                    st.rerun()
                                else:
                                    st.error("合并失败或无数据。")
                                    if errs:
                                        _show_error_table(errs)
                            except Exception as e:
                                st.error(f"合并失败：{e}")
                                import traceback
                                with st.expander("错误详情", expanded=False):
                                    st.code(traceback.format_exc())

    has_generic_result = (
        st.session_state.get("merge_result_mode") == "generic"
        and "merge_result_df" in st.session_state
        and st.session_state.merge_result_df is not None
    )
    if has_generic_result:
        merged_df = st.session_state.merge_result_df
        success_list = st.session_state.get("merge_result_success_list", [])
        error_list = st.session_state.get("merge_result_error_list", [])
        st.success("合并完成")
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("合并表行数", f"{len(merged_df):,}")
        with m2:
            st.metric("合并成功数", len(success_list))
        with m3:
            st.metric("未合并文件数", len(error_list), delta=None if not error_list else "见下方")
        if "generic_test_rules" not in st.session_state:
            st.session_state.generic_test_rules = []
        _prev_shape = getattr(st.session_state, "generic_test_result_df_shape", None)
        if _prev_shape != merged_df.shape:
            st.session_state.pop("generic_test_result", None)
            st.session_state.generic_test_result_df_shape = merged_df.shape
        st.markdown("#### 🔍 一键测试")
        cols = list(merged_df.columns.astype(str))
        for ri, rule in enumerate(st.session_state.generic_test_rules):
            with st.container():
                left_f = rule.get("left_field", "")
                left_op = rule.get("left_op") or "（单字段）"
                rel = rule.get("relation", "=")
                right_f = rule.get("right_field") if rule.get("right_field") is not None else str(rule.get("right_constant", ""))
                st.caption(f"规则{ri+1}: {left_f} {left_op} {rel} {right_f}")
                if st.button("删除", key=f"generic_del_rule_{ri}"):
                    st.session_state.generic_test_rules.pop(ri)
                    st.rerun()
        with st.expander("添加一条测试规则", expanded=False):
            left_field = st.selectbox("左侧字段", options=cols, key="generic_left_field")
            left_op = st.selectbox("左侧运算符（单字段选「无」）", options=["无", "+", "-", "*", "/"], key="generic_left_op")
            left_right_type = st.radio("左侧第二项", options=["字段", "常数"], key="generic_left_right_type", horizontal=True)
            if left_right_type == "字段":
                left_right_field = st.selectbox("左侧第二项字段", options=cols, key="generic_left_right_field")
                left_right_constant = None
            else:
                left_right_constant = st.number_input("左侧第二项常数", value=0.0, key="generic_left_right_constant")
                left_right_field = None
            relation = st.selectbox("关系", options=["=", ">", ">=", "<", "<=", "!="], key="generic_relation")
            right_type = st.radio("右侧为", options=["字段", "常数"], key="generic_right_type", horizontal=True)
            if right_type == "字段":
                right_field = st.selectbox("右侧字段", options=cols, key="generic_right_field")
                right_constant = None
            else:
                right_constant = st.number_input("右侧常数", value=0.0, key="generic_right_constant")
                right_field = None
            tolerance = st.number_input("等于时的容差（可选）", value=1e-6, format="%e", key="generic_tolerance") if relation == "=" else None
            if st.button("添加规则", key="generic_add_rule"):
                op_map = {"无": None, "+": "+", "-": "-", "*": "*", "/": "/"}
                st.session_state.generic_test_rules.append({
                    "left_field": left_field, "left_op": op_map[left_op],
                    "left_right_type": "field" if left_right_type == "字段" else "constant",
                    "left_right_field": left_right_field if left_right_type == "字段" else None,
                    "left_right_constant": left_right_constant if left_right_type == "常数" else None,
                    "relation": relation, "right_field": right_field if right_type == "字段" else None,
                    "right_constant": right_constant if right_type == "常数" else None,
                    "tolerance": tolerance,
                })
                st.rerun()
        run_test = st.button("一键测试", type="primary", key="generic_run_test")
        if run_test and st.session_state.generic_test_rules:
            with st.spinner("正在校验..."):
                st.session_state.generic_test_result = generic_run_validation(merged_df, st.session_state.generic_test_rules)
        if st.session_state.get("generic_test_result"):
            tr = st.session_state.generic_test_result
            passed = sum(1 for x in tr if x["passed"])
            failed = len(tr) - passed
            total_v = sum(x["violation_count"] for x in tr)
            st.markdown(f"**测试结果**：共 {len(tr)} 条规则，通过 {passed} 条，不通过 {failed} 条；违规行合计 {total_v} 行。")
            all_violation_dfs = []
            for x in tr:
                with st.expander(f"{x['label']} — {'通过' if x['passed'] else '不通过'}（违规 {x['violation_count']} 行）", expanded=not x["passed"]):
                    if not x["passed"] and x["violation_df"] is not None and len(x["violation_df"]) > 0:
                        st.dataframe(x["violation_df"].head(50), use_container_width=True, hide_index=True)
                        all_violation_dfs.append(x["violation_df"])
            if all_violation_dfs:
                combined_v = pd.concat(all_violation_dfs, ignore_index=True).drop_duplicates()
                _vb = BytesIO()
                combined_v.to_excel(_vb, index=False, engine="openpyxl")
                _vb.seek(0)
                st.download_button("导出违规行 Excel", data=_vb.getvalue(), file_name=f"一键测试违规行_{date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="generic_export_violations")
        st.markdown("#### 📥 导出")
        _export_date = date.today().strftime("%Y%m%d")
        _default_name = f"其他类型表格合并结果_{_export_date}"
        _export_name = st.text_input("导出文件名（可修改，不含扩展名）", value=_default_name, key="generic_export_filename", help="修改后点击下方按钮导出。")
        _base = (_export_name.strip() or _default_name).replace("\\", "_").replace("/", "_").replace(":", "_")
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            merged_df.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button("下载 Excel", data=buf.getvalue(), file_name=f"{_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="generic_download_xlsx")
        with c2:
            buf_csv = BytesIO()
            merged_df.to_csv(buf_csv, index=False, encoding="utf-8-sig")
            buf_csv.seek(0)
            st.download_button("下载 CSV", data=buf_csv.getvalue(), file_name=f"{_base}.csv", mime="text/csv", key="generic_download_csv")
        if error_list:
            st.markdown("---")
            _show_error_table(error_list)
        st.markdown("#### 📊 结果预览")
        st.dataframe(merged_df.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
        st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(merged_df):,} 行。")
        with st.expander("📋 合并规则说明"):
            st.markdown("详见《其他类型表格合并规则》文档（`其他类型表格合并规则.md`）。")
    elif generic_upload and not has_generic_result:
        st.info("请在上方选择合并方向与字段后点击「执行合并」。")
    else:
        st.info("👆 请选择至少一个 Excel 或 CSV 文件，再配置合并方向与字段。")
        with st.expander("📋 合并规则说明"):
            st.markdown("详见《其他类型表格合并规则》文档（`其他类型表格合并规则.md`）。")
    st.stop()

# ---------- CSV格式转换页 ----------
if is_csv_convert:
    st.markdown("""
    <div class="header-banner">
      <div class="header-inner">
        <span class="header-icon">📄</span>
        <h1 class="header-title">CSV格式转换</h1>
      </div>
      <p class="header-caption">上传一个多 Sheet 的 Excel，或输入本机路径；各 Sheet 首行表头一致，合并为一个 CSV 文件（UTF-8 编码）。</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    csv_convert_mode = st.radio(
        "选择方式",
        options=["上传文件", "输入本地路径"],
        index=0,
        key="csv_convert_mode",
        horizontal=True,
    )
    if csv_convert_mode == "输入本地路径":
        st.caption("仅在本机运行时可使用；结果将保存到与源文件相同目录，文件名：原文件名_合并.csv")
        csv_convert_scope = st.radio(
            "转换范围",
            options=["单表转换", "多表同时转换"],
            index=0,
            key="csv_convert_scope",
            horizontal=True,
        )
        if csv_convert_scope == "单表转换":
            csv_convert_path = st.text_input(
                "Excel 文件路径",
                value="",
                placeholder=r"例如 C:\Users\...\xxx.xlsx",
                key="csv_convert_path",
            )
            do_convert_path = st.button("一键转化", type="primary", key="csv_convert_do_path")
            if do_convert_path and csv_convert_path.strip():
                with st.spinner("正在合并各 Sheet，请稍候..."):
                    try:
                        output_path, err, stats = csv_convert_by_path(csv_convert_path.strip())
                        if err:
                            st.error(err)
                        else:
                            st.success(f"已保存到：{output_path}")
                            if stats:
                                m1, m2 = st.columns(2)
                                with m1:
                                    st.metric("合并后总行数", f"{stats.get('total_rows', 0):,}")
                                with m2:
                                    st.metric("合并的 Sheet 数", stats.get("sheet_count", 0))
                                if stats.get("sheet_names") and stats.get("row_counts"):
                                    with st.expander("各 Sheet 行数", expanded=False):
                                        tbl = pd.DataFrame({"Sheet 名称": stats["sheet_names"], "行数": stats["row_counts"]})
                                        st.dataframe(tbl, use_container_width=True, hide_index=True)
                    except Exception as e:
                        st.error(f"转换失败：{e}")
                        import traceback
                        with st.expander("错误详情", expanded=False):
                            st.code(traceback.format_exc())
            elif do_convert_path and not csv_convert_path.strip():
                st.warning("请输入 Excel 文件路径。")
        else:
            csv_convert_paths_text = st.text_area(
                "Excel 文件路径（每行一个）",
                value="",
                placeholder=r"C:\path\a.xlsx" + "\n" + r"C:\path\b.xlsx",
                key="csv_convert_paths",
                height=120,
            )
            csv_multi_mode = st.radio(
                "输出方式",
                options=["多表转换为一个 CSV", "多表分别转化为 CSV"],
                index=0,
                key="csv_multi_mode",
                horizontal=True,
            )
            do_convert_multi = st.button("一键转化", type="primary", key="csv_convert_do_multi")
            if do_convert_multi:
                paths = csv_convert_parse_paths(csv_convert_paths_text)
                if not paths:
                    st.warning("请至少输入一行有效的 .xlsx 或 .xls 文件路径。")
                elif csv_multi_mode == "多表转换为一个 CSV":
                    with st.spinner("正在合并多表为单个 CSV，请稍候..."):
                        try:
                            output_path, err, stats = csv_convert_paths_to_single(paths)
                            if err:
                                st.error(err)
                            else:
                                st.success(f"已保存到：{output_path}")
                                if stats:
                                    m1, m2 = st.columns(2)
                                    with m1:
                                        st.metric("合并后总行数", f"{stats.get('total_rows', 0):,}")
                                    with m2:
                                        st.metric("参与文件数", stats.get("file_count", 0))
                        except Exception as e:
                            st.error(f"转换失败：{e}")
                            import traceback
                            with st.expander("错误详情", expanded=False):
                                st.code(traceback.format_exc())
                else:
                    with st.spinner("正在分别转换各表..."):
                        try:
                            results = csv_convert_paths_to_separate(paths)
                            success = sum(1 for r in results if r[0] and not r[1])
                            fail = len(results) - success
                            st.metric("成功", success)
                            if fail:
                                st.metric("失败", fail)
                            for i, (out_path, err, stats) in enumerate(results):
                                path_display = paths[i] if i < len(paths) else ""
                                if err:
                                    st.error(f"【{path_display}】{err}")
                                else:
                                    st.success(f"【{path_display}】已保存到：{out_path}")
                        except Exception as e:
                            st.error(f"转换失败：{e}")
                            import traceback
                            with st.expander("错误详情", expanded=False):
                                st.code(traceback.format_exc())
    else:
        csv_convert_upload = st.file_uploader(
            "选择要转换的 Excel 文件（单文件）",
            type=["xlsx", "xls"],
            accept_multiple_files=False,
            key="csv_convert_upload",
            help="支持 .xlsx / .xls；每个 Sheet 第一行为标题行，且各 Sheet 表头完全一致。",
        )
        if csv_convert_upload:
            f = csv_convert_upload
            st.markdown("#### 📁 已选文件")
            st.caption(f"**{f.name}**（{f.size / 1024:.1f} KB）")
            do_convert = st.button("转换为 CSV", type="primary", key="csv_convert_do_convert")
            if do_convert:
                with st.spinner("正在合并各 Sheet..."):
                    try:
                        df, err, stats = csv_convert_excel(f.getvalue(), f.name)
                        if err:
                            st.error(err)
                        elif df is not None:
                            base_name = f.name.rsplit(".", 1)[0] if "." in f.name else f.name
                            st.session_state.csv_convert_df = df
                            st.session_state.csv_convert_stats = stats or {}
                            st.session_state.csv_convert_filename = base_name
                            st.rerun()
                    except Exception as e:
                        st.error(f"转换失败：{e}")
                        import traceback
                        with st.expander("错误详情", expanded=False):
                            st.code(traceback.format_exc())

        has_csv_result = (
            "csv_convert_df" in st.session_state
            and st.session_state.csv_convert_df is not None
        )
        if has_csv_result:
            merged_df = st.session_state.csv_convert_df
            stats = st.session_state.get("csv_convert_stats") or {}
            base_name = st.session_state.get("csv_convert_filename", "合并结果")
            st.success("转换完成")
            m1, m2 = st.columns(2)
            with m1:
                st.metric("合并后总行数", f"{len(merged_df):,}")
            with m2:
                st.metric("合并的 Sheet 数", stats.get("sheet_count", 0))
            if stats.get("sheet_names") and stats.get("row_counts"):
                with st.expander("各 Sheet 行数", expanded=False):
                    tbl = pd.DataFrame({"Sheet 名称": stats["sheet_names"], "行数": stats["row_counts"]})
                    st.dataframe(tbl, use_container_width=True, hide_index=True)
            st.markdown("#### 📥 下载 CSV（UTF-8）")
            buf_csv = BytesIO()
            merged_df.to_csv(buf_csv, index=False, encoding="utf-8")
            buf_csv.seek(0)
            st.download_button(
                "下载 CSV",
                data=buf_csv.getvalue(),
                file_name=f"{base_name}.csv",
                mime="text/csv; charset=utf-8",
                key="csv_convert_download",
            )
            st.markdown("#### 📊 结果预览")
            st.dataframe(merged_df.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
            st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(merged_df):,} 行。")
        elif csv_convert_upload and not has_csv_result:
            st.info("请点击「转换为 CSV」生成合并结果。")
        else:
            st.info("👆 请上传一个多 Sheet 的 Excel 文件（.xlsx 或 .xls）。")
    st.stop()

# ---------- 数据透视表页 ----------
if is_pivot:
    st.markdown("""
    <div class="header-banner">
      <div class="header-inner">
        <span class="header-icon">📊</span>
        <h1 class="header-title">数据透视表</h1>
      </div>
      <p class="header-caption">导入数据或连接数据库后自动识别字段，配置筛选、行/列/值及聚合，一次性设置完成后点击生成，仿 Excel 数据透视表。</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    pivot_source = st.radio("数据来源", ["导入文件", "连接数据库"], key="pivot_source", horizontal=True)
    cols = None
    pivot_df = None
    pivot_use_db = pivot_source == "连接数据库"

    if pivot_use_db:
        st.caption("先选择数据库类型，再填写连接信息，点击「获取表字段」后即可配置透视（不拉取全量数据，设置完成后点击「生成透视表」再执行）。")
        with st.expander("数据库连接", expanded=True):
            db_type_label = st.radio("数据库类型", ["MySQL", "PostgreSQL (psql)"], key="pivot_db_type_radio", horizontal=True)
            db_type = "psql" if "PostgreSQL" in db_type_label else "mysql"
            suffix = "_psql" if db_type == "psql" else "_mysql"
            default_port = 5432 if db_type == "psql" else 3306
            if db_type == "psql":
                default_host, default_user, default_pass = "localhost", "postgres", "Admin2026"
                default_dbname, default_schema, default_table = "evdata", "rowdata", "evdata_2512_row"
            else:
                default_host = default_user = default_pass = default_dbname = default_table = ""
                default_schema = "public"
            st.caption("MySQL 默认端口 3306，PostgreSQL 默认端口 5432。")
            db_host = st.text_input("主机", value=st.session_state.get("pivot_db_host" + suffix, default_host), key="pivot_db_host" + suffix, placeholder="127.0.0.1 或 localhost")
            db_port = st.number_input("端口", value=int(st.session_state.get("pivot_db_port" + suffix, default_port)), min_value=1, max_value=65535, key="pivot_db_port" + suffix)
            db_user = st.text_input(
                "用户名",
                value=st.session_state.get("pivot_db_user" + suffix, default_user),
                key="pivot_db_user" + suffix,
                help="MySQL：安装时或创建用户时设定，常见为 root。PostgreSQL：安装时设定，本机一般为 postgres；云数据库或公司环境请向管理员索取或查看连接说明。",
            )
            db_pass = st.text_input("密码", type="password", value=st.session_state.get("pivot_db_pass" + suffix, default_pass), key="pivot_db_pass" + suffix)
            db_name = st.text_input("数据库名", value=st.session_state.get("pivot_db_name" + suffix, default_dbname), key="pivot_db_name" + suffix)
            if db_type == "psql":
                db_schema = st.text_input("Schema", value=st.session_state.get("pivot_db_schema", default_schema), key="pivot_db_schema", help="PostgreSQL  schema，一般为 public")
            else:
                db_schema = None
            db_table = st.text_input("表名", value=st.session_state.get("pivot_db_table" + suffix, default_table), key="pivot_db_table" + suffix)
        if st.button("获取表字段", key="pivot_db_fetch_cols"):
            if not db_host or not db_user or not db_name or not db_table:
                st.warning("请填写主机、用户名、数据库名和表名。")
            else:
                with st.spinner("连接并读取表结构..."):
                    col_list, err = pivot_get_db_columns(db_type, db_host, db_port, db_user, db_pass, db_name, db_table, db_schema)
                    if err:
                        st.error(err)
                    else:
                        st.session_state.pivot_db_columns = col_list
                        st.session_state.pivot_db_config = {
                            "db_type": db_type, "host": db_host, "port": db_port, "user": db_user, "password": db_pass,
                            "database": db_name, "table": db_table, "schema": db_schema,
                        }
                        st.rerun()
        if st.session_state.get("pivot_db_columns"):
            cols = st.session_state.pivot_db_columns
            st.success("已获取字段：%s" % ", ".join(str(c) for c in cols[:20]) + (" ..." if len(cols) > 20 else ""))
    else:
        st.caption("数据来源：导入文件（Excel/CSV）。大表建议行数在 100 万以内。")
        pivot_upload = st.file_uploader(
            "选择要透视的数据文件",
            type=["xlsx", "xls", "csv"],
            accept_multiple_files=False,
            key="pivot_upload",
            help="支持 .xlsx / .xls / .csv，首行为表头。",
        )
        if pivot_upload:
            f = pivot_upload
            if "pivot_source_df" not in st.session_state or st.session_state.get("pivot_source_filename") != f.name:
                with st.spinner("正在读取数据..."):
                    try:
                        if f.name.lower().endswith(".csv"):
                            try:
                                df_up = pd.read_csv(BytesIO(f.getvalue()), encoding="utf-8-sig", nrows=1_000_000)
                            except Exception:
                                df_up = pd.read_csv(BytesIO(f.getvalue()), encoding="gbk", nrows=1_000_000)
                        else:
                            try:
                                df_up = pd.read_excel(BytesIO(f.getvalue()), sheet_name=0, engine="openpyxl" if f.name.lower().endswith(".xlsx") else "xlrd", nrows=1_000_000)
                            except TypeError:
                                df_up = pd.read_excel(BytesIO(f.getvalue()), sheet_name=0, engine="openpyxl" if f.name.lower().endswith(".xlsx") else "xlrd")
                                if len(df_up) > 1_000_000:
                                    df_up = df_up.iloc[:1_000_000]
                        if len(df_up) >= 1_000_000:
                            st.warning("数据已截断为前 100 万行，超出部分未参与透视。")
                        st.session_state.pivot_source_df = df_up
                        st.session_state.pivot_source_filename = f.name
                    except Exception as e:
                        st.error(f"读取文件失败：{e}")
                        import traceback
                        with st.expander("错误详情", expanded=False):
                            st.code(traceback.format_exc())
            if st.session_state.get("pivot_source_df") is not None:
                pivot_df = st.session_state.pivot_source_df
                cols = list(pivot_df.columns.astype(str))
                st.metric("已加载行数", f"{len(pivot_df):,}")
                with st.expander("预览前 10 行", expanded=False):
                    st.dataframe(pivot_df.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)

    if cols:
        st.markdown("#### 字段列表（自动识别）")
        st.caption("下方配置筛选、行/列/值，设置完成后点击「生成透视表」一次性执行。")
        st.markdown("#### 筛选（可选）")
        filter_mode = st.radio("筛选方式", ["不筛选", "按取值选择", "输入条件"], key="pivot_filter_mode", horizontal=True)
        filter_col = None
        filter_selected = None
        filter_where = None
        if filter_mode == "按取值选择":
            filter_col = st.selectbox("筛选字段", options=cols, key="pivot_filter_col")
            if st.button("获取该字段可选值", key="pivot_fetch_distinct"):
                if pivot_use_db:
                    cfg = st.session_state.get("pivot_db_config")
                    if cfg:
                        with st.spinner("查询去重值..."):
                            vals, err = pivot_get_db_distinct(
                                cfg.get("db_type", "mysql"), cfg["host"], cfg["port"], cfg["user"], cfg["password"],
                                cfg["database"], cfg["table"], filter_col, schema=cfg.get("schema"),
                            )
                            if err:
                                st.error(err)
                            else:
                                st.session_state.pivot_filter_options = vals
                                st.rerun()
                else:
                    with st.spinner("获取去重值..."):
                        vals, err = pivot_get_distinct_values(pivot_df, filter_col)
                        if err:
                            st.error(err)
                        else:
                            st.session_state.pivot_filter_options = vals
                            st.rerun()
            if st.session_state.get("pivot_filter_options") is not None:
                opts = st.session_state.pivot_filter_options
                filter_selected = st.multiselect("选择要保留的值", options=opts, default=[], key="pivot_filter_multiselect")
        elif filter_mode == "输入条件":
            st.caption("数据库：按 MySQL WHERE 子句规则填写（不含 WHERE 关键字）。文件：按 Python 表达式填写，如 age > 18 and city == '北京'")
            filter_where = st.text_area("条件", key="pivot_filter_where", placeholder="例如：age > 18 AND city = '北京'", height=80)

        st.markdown("#### 行 / 列 / 值")
        c1, c2 = st.columns(2)
        with c1:
            row_fields = st.multiselect("行字段", options=cols, default=[], key="pivot_row_fields")
        with c2:
            col_fields = st.multiselect("列字段", options=cols, default=[], key="pivot_col_fields")
        st.markdown("**值字段与聚合**：选择一个值字段，勾选需要的聚合方式。")
        value_col = st.selectbox("值字段", options=cols, key="pivot_value_col")
        agg_checks = st.columns(5)
        with agg_checks[0]:
            agg_sum = st.checkbox("求和", key="pivot_agg_sum")
        with agg_checks[1]:
            agg_count = st.checkbox("计数", key="pivot_agg_count")
        with agg_checks[2]:
            agg_mean = st.checkbox("平均值", key="pivot_agg_mean")
        with agg_checks[3]:
            agg_max = st.checkbox("最大值", key="pivot_agg_max")
        with agg_checks[4]:
            agg_min = st.checkbox("最小值", key="pivot_agg_min")
        values_aggs = []
        if agg_sum:
            values_aggs.append((value_col, "加总"))
        if agg_count:
            values_aggs.append((value_col, "计数"))
        if agg_mean:
            values_aggs.append((value_col, "平均值"))
        if agg_max:
            values_aggs.append((value_col, "最大值"))
        if agg_min:
            values_aggs.append((value_col, "最小值"))

        do_pivot = st.button("生成透视表", type="primary", key="pivot_do")
        if do_pivot:
            if not row_fields and not col_fields:
                st.warning("请至少选择行字段或列字段。")
            elif not values_aggs:
                st.warning("请至少勾选一种值字段的聚合方式（求和/计数/平均值/最大值/最小值）。")
            else:
                with st.spinner("正在生成透视表..."):
                    try:
                        if pivot_use_db:
                            cfg = st.session_state.get("pivot_db_config")
                            if not cfg:
                                st.error("请先点击「获取表字段」完成数据库配置。")
                            else:
                                where_clause = None
                                if filter_mode == "输入条件" and filter_where and filter_where.strip():
                                    where_clause = filter_where.strip()
                                elif filter_mode == "按取值选择" and filter_col and filter_selected:
                                    db_t = cfg.get("db_type", "mysql")
                                    if db_t == "psql":
                                        col_esc = '"' + str(filter_col).replace('"', '""') + '"'
                                    else:
                                        col_esc = "`" + str(filter_col).replace("`", "``") + "`"
                                    def _sql_val(x):
                                        if x is None:
                                            return "NULL"
                                        if isinstance(x, (int, float)) and not isinstance(x, bool):
                                            return str(x)
                                        s = str(x).replace("\\", "\\\\").replace("'", "''")
                                        return "'%s'" % s
                                    in_vals = ",".join(_sql_val(v) for v in filter_selected)
                                    where_clause = "%s IN (%s)" % (col_esc, in_vals)
                                result_df, err = pivot_build_from_db(
                                    cfg.get("db_type", "mysql"), cfg["host"], cfg["port"], cfg["user"], cfg["password"],
                                    cfg["database"], cfg["table"],
                                    row_fields, col_fields, values_aggs, where_clause, cfg.get("schema"),
                                )
                                if err:
                                    st.error(err)
                                else:
                                    st.session_state.pivot_result_df = result_df
                                    st.rerun()
                        else:
                            work_df = pivot_df
                            if filter_mode == "按取值选择" and filter_col and filter_selected:
                                work_df, err = pivot_filter_dataframe(pivot_df, filter_col=filter_col, selected_values=filter_selected)
                                if err:
                                    st.error(err)
                                    work_df = None
                            elif filter_mode == "输入条件" and filter_where and filter_where.strip():
                                work_df, err = pivot_filter_dataframe(pivot_df, where_expr=filter_where)
                                if err:
                                    st.error(err)
                                    work_df = None
                            if work_df is not None:
                                result_df, err = pivot_build_table(work_df, row_fields, col_fields, values_aggs)
                                if err:
                                    st.error(err)
                                else:
                                    st.session_state.pivot_result_df = result_df
                                    st.rerun()
                    except Exception as e:
                        st.error(f"生成失败：{e}")
                        import traceback
                        with st.expander("错误详情", expanded=False):
                            st.code(traceback.format_exc())
        if st.session_state.get("pivot_result_df") is not None:
            res = st.session_state.pivot_result_df
            st.success("透视表已生成")
            st.caption("若列较多，可左右滑动表格区域查看完整内容；导出文件为完整结果。")
            # 把行索引变为普通列，避免最左侧一列在预览时被隐藏，并避免双滚动条
            has_index = getattr(res.index, "name", None) is not None or isinstance(getattr(res, "index", None), pd.MultiIndex)
            display_df = res.reset_index() if has_index else res
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.markdown("#### 导出")
            b1, b2 = st.columns(2)
            _include_index = getattr(res.index, "name", None) is not None or (hasattr(res, "index") and isinstance(res.index, pd.MultiIndex))
            with b1:
                buf_x = BytesIO()
                res.to_excel(buf_x, index=_include_index, engine="openpyxl")
                buf_x.seek(0)
                st.download_button("下载 Excel", data=buf_x.getvalue(), file_name="数据透视表结果.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="pivot_dl_xlsx")
            with b2:
                buf_c = BytesIO()
                res.to_csv(buf_c, index=_include_index, encoding="utf-8-sig")
                buf_c.seek(0)
                st.download_button("下载 CSV", data=buf_c.getvalue(), file_name="数据透视表结果.csv", mime="text/csv", key="pivot_dl_csv")
    else:
        if not pivot_use_db:
            st.info("请上传一个 Excel 或 CSV 文件以开始配置透视表。")
        else:
            st.info("请填写上方数据库连接信息并点击「获取表字段」。若已点击但列表仍未出现，请查看本页上方是否有红色报错（如曾提示缺少驱动，请在本机终端运行：pip install psycopg2-binary 后重试）。")
    st.stop()

# ---------- 合并页（公共桩 / 充电站） ----------
merge_fn = pile_merge_files if is_pile else station_merge_files
merge_csv_fn = pile_merge_files_to_csv if is_pile else station_merge_files_to_csv
key_prefix = "pile_" if is_pile else "station_"
mode_caption = "上传多个运营商 Excel/CSV，按统一表头自动识别并纵向合并，最左侧填充「上报机构」。" if is_pile else "上传多个充电站 Excel/CSV，按统一表头自动识别并纵向合并，最左侧填充「上报机构」。"

st.markdown(f"""
<div class="header-banner">
  <div class="header-inner">
    <span class="header-icon">🔗⚡</span>
    <h1 class="header-title">众链充电原始表合并系统</h1>
  </div>
  <p class="header-caption">{mode_caption}</p>
</div>
""", unsafe_allow_html=True)
st.markdown("---")

merge_large_mode = st.radio(
    "合并模式",
    options=["小文件", "大文件"],
    index=0,
    key=f"{key_prefix}merge_mode_radio",
    horizontal=True,
    help="小文件：合并后在页面预览并导出 Excel/CSV；大文件：直接合并为 CSV，仅提供下载，不展示预览。",
)
is_large_file_mode = merge_large_mode == "大文件"

merge_upload = st.file_uploader(
    "选择要合并的 Excel 或 CSV 文件（可多选）",
    type=["xlsx", "xls", "csv"],
    accept_multiple_files=True,
    key=f"{key_prefix}table_merge_upload",
    help="支持 .xlsx / .xls / .csv；表头将按方案自动判定。",
)

if merge_upload:
    st.markdown("#### 📁 已选文件")
    file_list = []
    for i, f in enumerate(merge_upload, 1):
        size_mb = f.size / (1024 * 1024)
        file_list.append({"序号": i, "文件名": f.name, "大小 (MB)": f"{size_mb:.2f}"})
    st.dataframe(pd.DataFrame(file_list), use_container_width=True, hide_index=True)

    do_merge = st.button("▶ 开始合并", type="primary", key=f"{key_prefix}do_table_merge", use_container_width=False)

    if do_merge:
        files = [(f.name, f.getvalue()) for f in merge_upload]
        with st.spinner("正在解析并合并..." if not is_large_file_mode else "正在合并为 CSV..."):
            try:
                if is_large_file_mode:
                    csv_bytes, success_list, error_list, row_counts = merge_csv_fn(files)
                    if csv_bytes is not None:
                        st.session_state.merge_result_csv_bytes = csv_bytes
                        st.session_state.merge_result_success_list = success_list
                        st.session_state.merge_result_error_list = error_list or []
                        st.session_state.merge_result_row_counts = row_counts or []
                        st.session_state.merge_result_mode = "pile" if is_pile else "station"
                        st.session_state.merge_result_is_large = True
                        st.session_state.pop("merge_result_df", None)
                        st.rerun()
                    else:
                        st.error("没有可合并的数据。")
                        if error_list:
                            _show_error_table(error_list)
                        with st.expander("📋 合并规则说明"):
                            st.caption("大文件模式：直接合并为 CSV 并下载，不展示预览。")
                else:
                    merged_df, success_list, error_list, row_counts = merge_fn(files)
                    if merged_df is not None:
                        st.session_state.merge_result_df = merged_df
                        st.session_state.merge_result_success_list = success_list
                        st.session_state.merge_result_error_list = error_list or []
                        st.session_state.merge_result_row_counts = row_counts or []
                        st.session_state.merge_result_mode = "pile" if is_pile else "station"
                        st.session_state.merge_result_is_large = False
                        st.session_state.pop("merge_result_csv_bytes", None)
                        st.rerun()
                    else:
                        st.error("没有可合并的数据。")
                        if error_list:
                            _show_error_table(error_list)
                    with st.expander("📋 合并规则说明"):
                        if is_pile:
                            st.markdown("公共桩：表头含「充电桩编号」或「充电桩编码」；多 Sheet 以 1.1 为主表；1.3 补全厂商信息。")
                        else:
                            st.markdown("充电站：表头含「所属充电站编号」或「充电站编码」；多 Sheet 为 1.1 → 含「充电站」→ 否则报错。")
            except Exception as e:
                st.error(f"合并失败：{e}")
                import traceback
                st.markdown("---")
                with st.expander("错误详情", expanded=False):
                    st.code(traceback.format_exc())

    # 结果区：有当前模式的合并结果时显示（小文件：预览+双格式导出；大文件：仅 CSV 下载）
    current_result_mode = st.session_state.get("merge_result_mode")
    is_large_result = st.session_state.get("merge_result_is_large", False)
    has_small_result = (
        current_result_mode == ("pile" if is_pile else "station")
        and not is_large_result
        and "merge_result_df" in st.session_state
        and st.session_state.merge_result_df is not None
    )
    has_large_result = (
        current_result_mode == ("pile" if is_pile else "station")
        and is_large_result
        and "merge_result_csv_bytes" in st.session_state
        and st.session_state.merge_result_csv_bytes is not None
    )
    if has_large_result:
        success_list = st.session_state.get("merge_result_success_list", [])
        error_list = st.session_state.get("merge_result_error_list", [])
        row_counts = st.session_state.get("merge_result_row_counts", [])
        csv_bytes = st.session_state.merge_result_csv_bytes
        total_rows = sum(n for _, n in row_counts)
        st.success("合并完成（大文件模式）")
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("合并表行数", f"{total_rows:,}")
        with m2:
            st.metric("合并成功数", len(success_list))
        with m3:
            st.metric("未合并文件数", len(error_list), delta=None if not error_list else "见下方")
        if row_counts:
            st.markdown("**各表行数**")
            st.dataframe(
                pd.DataFrame(row_counts, columns=["文件名", "行数"]),
                use_container_width=True,
                hide_index=True,
            )
        st.markdown("#### 📥 导出")
        _export_base = "充电桩合并结果" if is_pile else "充电站合并结果"
        _export_date = date.today().strftime("%Y%m%d")
        _default_name = f"{_export_base}_{_export_date}.csv"
        _export_name = st.text_input(
            "导出文件名（可修改）",
            value=_default_name,
            key="merge_large_export_filename",
            help="修改后点击「下载合并结果 CSV」即可导出。",
        )
        _name_csv = (_export_name.strip() or _default_name).replace("\\", "_").replace("/", "_").replace(":", "_")
        if not _name_csv.lower().endswith(".csv"):
            _name_csv = _name_csv + ".csv"
        st.download_button(
            "下载合并结果 CSV",
            data=csv_bytes,
            file_name=_name_csv,
            mime="text/csv",
            key=f"{key_prefix}download_merged_csv_large",
        )
        if error_list:
            st.markdown("---")
            _show_error_table(error_list)
        st.caption("大文件模式不展示合并结果预览，请直接下载 CSV。")
    elif has_small_result:
        merged_df = st.session_state.merge_result_df
        success_list = st.session_state.get("merge_result_success_list", [])
        error_list = st.session_state.get("merge_result_error_list", [])
        row_counts = st.session_state.get("merge_result_row_counts", [])
        st.success("合并完成")
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("合并表行数", f"{len(merged_df):,}")
        with m2:
            st.metric("合并成功数", len(success_list))
        with m3:
            st.metric("未合并文件数", len(error_list), delta=None if not error_list else "见下方")
        if row_counts:
            st.markdown("**各表行数**")
            st.dataframe(
                pd.DataFrame(row_counts, columns=["文件名", "行数"]),
                use_container_width=True,
                hide_index=True,
            )
        st.markdown("#### 📥 导出")
        _export_base = "充电桩合并结果" if is_pile else "充电站合并结果"
        _export_date = date.today().strftime("%Y%m%d")
        _default_name = f"{_export_base}_{_export_date}"
        _export_name = st.text_input(
            "导出文件名（可修改，不含扩展名）",
            value=_default_name,
            key=f"{key_prefix}merge_small_export_filename",
            help="修改后点击下方「下载 Excel」或「下载 CSV」即可导出。",
        )
        _base = (_export_name.strip() or _default_name).replace("\\", "_").replace("/", "_").replace(":", "_")
        _name_xlsx = f"{_base}.xlsx"
        _name_csv = f"{_base}.csv"
        c1, c2 = st.columns(2)
        with c1:
            buf = BytesIO()
            merged_df.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            st.download_button(
                "下载 Excel",
                data=buf.getvalue(),
                file_name=_name_xlsx,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}download_merged_xlsx",
            )
        with c2:
            buf_csv = BytesIO()
            merged_df.to_csv(buf_csv, index=False, encoding="utf-8-sig")
            buf_csv.seek(0)
            st.download_button(
                "下载 CSV",
                data=buf_csv.getvalue(),
                file_name=_name_csv,
                mime="text/csv",
                key=f"{key_prefix}download_merged_csv",
            )
        if st.button("数据清洗", key=f"{key_prefix}goto_clean", type="secondary"):
            st.session_state.df_for_clean = merged_df.copy()
            st.session_state.pop("df_cleaned", None)
            st.session_state.pop("clean_report", None)
            st.session_state.main_view = "clean_after_merge"
            st.session_state.merge_mode = "数据清洗"
            st.rerun()
        if error_list:
            st.markdown("---")
            _show_error_table(error_list)
        st.markdown("#### 📊 合并结果预览")
        st.dataframe(merged_df.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
        st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(merged_df):,} 行。")
        with st.expander("📋 合并规则说明"):
            if is_pile:
                st.markdown("""
- **表头**：首行含「单位」「参考」「编码方法」之一时，取第二行含「充电桩编号」或「充电桩编码」为表头；否则取首行或前 3 行内第一个含「充电桩编号」/「充电桩编码」的行。表头阶段编号与编码等同。
- **多 Sheet**：若存在名称含「1.1」的 Sheet，则以其为主表；否则取第一个有内容的 Sheet。
- **1.3**：名称含「1.3」的 Sheet 表头行为含「充电桩生产厂商名称」的行，用于补全主表厂商名称与类型（1.2 运营商补全已取消）。
- **上报机构**：最左侧一列，由文件名清洗（去掉 `202512_公共桩_`、`_公共桩`、`附件一：`及其后内容）。
                """)
            else:
                st.markdown("""
- **表头**：前 3 行内第一个包含「所属充电站编号」或「充电站编码」的行作为表头。
- **多 Sheet**：1.1 优先 → 名称含「充电站」的 Sheet → 否则报错「多sheet表无法确定主表」。（1.2 运营商补全已取消。）
- **上报机构**：与公共桩同一套清洗规则。
                """)
else:
    st.info("👆 请在上方选择至少一个 Excel 或 CSV 文件后点击「开始合并」。")
    with st.expander("📋 合并规则说明"):
        if is_pile:
            st.markdown("""
- **表头**：首行含「单位」「参考」「编码方法」之一时，取第二行含「充电桩编号」或「充电桩编码」为表头；否则取首行或前 3 行内第一个含「充电桩编号」/「充电桩编码」的行。表头阶段编号与编码等同。
- **多 Sheet**：若存在名称含「1.1」的 Sheet，则以其为主表；否则取第一个有内容的 Sheet。
- **1.2 / 1.3**：1.2 表头行为含「运营商名称」的行，1.3 为含「充电桩生产厂商名称」的行；用于补全主表运营商/厂商名称与类型。
- **上报机构**：最左侧一列，由文件名清洗（去掉 `202512_公共桩_`、`_公共桩`、`附件一：`及其后内容）。
            """)
        else:
            st.markdown("""
- **表头**：前 3 行内第一个包含「所属充电站编号」或「充电站编码」的行作为表头。
- **多 Sheet**：1.1 优先 → 名称含「充电站」的 Sheet → 否则报错「多sheet表无法确定主表」。
- **1.2 / 1.3**：1.2 为名称含「1.2」或「运营商」的 Sheet，表头行含「运营商名称」；1.3 为含「1.3」或「厂商」的 Sheet，表头行含「充电桩生产厂商名称」。补全主表运营商/厂商名称与类型。
- **上报机构**：与公共桩同一套清洗规则。
            """)
