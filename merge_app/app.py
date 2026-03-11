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
SIDEBAR_OPTIONS_DISPLAY = ["🔌 公共桩多表合并", "⚡ 充电站多表合并", "📊 电量表多表合并", "🧹 数据清洗"]
SIDEBAR_OPTIONS_VALUE = ["公共桩多表合并", "充电站多表合并", "电量表多表合并", "数据清洗"]

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
    df_for_clean = None
    if st.session_state.main_view == "clean_after_merge":
        df_for_clean = st.session_state.get("df_for_clean")
    if df_for_clean is not None:
        st.caption("当前使用合并结果表或上传表进行清洗。")
        # 数据类型下拉：充电站数据 / 充电桩数据
        if "clean_table_type" not in st.session_state:
            _detected = _clean_mod._detect_table_type(df_for_clean) if _clean_mod else "station"
            st.session_state.clean_table_type = _detected
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

        st.markdown("#### 待清洗数据预览")
        st.dataframe(df_for_clean.head(PREVIEW_ROWS), use_container_width=True, hide_index=True)
        st.caption(f"仅展示前 {PREVIEW_ROWS} 行，共 {len(df_for_clean):,} 行。")
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
                with st.spinner("正在按规范 V2.0 一键清洗..."):
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
                for rid, label in rules_for_type:
                    if st.checkbox(label, key=f"clean_rule_{rid}", value=True):
                        rule_ids_selected.append(rid)
                do_custom = st.button("执行自定义清洗", type="primary", key="do_custom_clean", use_container_width=False)
                if do_custom:
                    if not rule_ids_selected:
                        st.warning("请至少勾选一项清洗规则。")
                    else:
                        with st.spinner("正在按所选规则清洗..."):
                            try:
                                cleaned_df, report = _clean_mod.clean_dataframe(
                                    df_for_clean, table_type=clean_table_type, rules_to_apply=rule_ids_selected
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
                buf = BytesIO()
                df_cleaned.to_excel(buf, index=False, engine="openpyxl")
                buf.seek(0)
                st.download_button("下载清洗后 Excel", data=buf.getvalue(), file_name=_name_xlsx, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="download_cleaned_xlsx")
            with d2:
                buf_csv = BytesIO()
                df_cleaned.to_csv(buf_csv, index=False, encoding="utf-8-sig")
                buf_csv.seek(0)
                st.download_button("下载清洗后 CSV", data=buf_csv.getvalue(), file_name=_name_csv, mime="text/csv", key="download_cleaned_csv")
        with st.expander("📋 清洗规则说明"):
            st.markdown("详见项目内《数据清洗规则》文档（`数据清洗规则.md`）。主要包含：通用空值/序号/位置截断、日期统一为 yyyy/mm/dd 与清洗结果标记、功率→kW/电压→V/电流→A、充电站内部编号缺失校验、充电桩设备类型标准化与设备开通时间校验。")
    else:
        # clean_upload：先上传再清洗
        st.caption("请上传 Excel 或 CSV，上传后将在此进行清洗。")
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
                    df_upload = pd.read_csv(upload_clean, encoding="utf-8-sig")
                else:
                    df_upload = pd.read_excel(upload_clean, engine="openpyxl")
                if df_upload is not None and not df_upload.empty:
                    st.session_state.df_for_clean = df_upload
                    st.session_state.pop("df_cleaned", None)
                    st.session_state.pop("clean_report", None)
                    st.session_state.main_view = "clean_after_merge"
                    st.rerun()
                else:
                    st.warning("表格为空或无法解析。")
            except Exception as e:
                st.error(f"读取文件失败：{e}")
        else:
            st.info("👆 请上传一个 Excel 或 CSV 文件。")
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
