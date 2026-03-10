# data_manager.py - EXCEL数据导入和管理界面

import sys
import os
import warnings

# 过滤 Streamlit 缓存相关的 RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*expire_cache.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*was never awaited.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*")
# 过滤所有来自 streamlit 模块的 RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, module="streamlit.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="streamlit.*")

# 过滤 Altair 类型推断警告（当数据为空时）
warnings.filterwarnings("ignore", message=".*don't know how to infer vegalite type from 'empty'.*")

import streamlit as st
import pandas as pd
from datetime import date, datetime
from io import BytesIO
import plotly.express as px
import time

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 确保日志系统已初始化（在导入其他模块之前）
from data.error_handler import logger as _logger_init

from utils.db_utils import create_db_engine
from handlers import (
    FileUploadHandler,
    DataImportHandler,
    DataPreviewHandler,
    DataAnalysisHandler,
    DataCompareHandler,
    DataQualityHandler,
    DataCleaningHandler,
    PDFReportHandler,
    TextReportHandler,
    RegionCleaningHandler,
)
from handlers.table_merge_handler import merge_files

# 导入审计服务（用于记录操作日志）
from services.audit_service import get_audit_service, audit_log
from auth.session_manager import SessionManager
from auth.permission_checker import PermissionChecker
from data.error_handler import ErrorHandler
from data.region_dictionary import RegionDictionary


# Streamlit页面配置
st.set_page_config(
    page_title="📊 充电数据管理系统",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式 - 现代化绿色和蓝色主题
st.markdown("""
<style>
    /* 全局样式 */
    .main > div {
        padding-top: 2rem;
    }
    
    /* 标题样式 */
    h1 {
        color: #00695C;
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
        background: linear-gradient(135deg, #00695C 0%, #00B4D8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* 副标题样式 */
    .stMarkdown {
        color: #4A90A4;
    }
    
    /* 侧边栏样式 */
    .css-1d391kg {
        background: linear-gradient(180deg, #E8F5E9 0%, #C8E6C9 100%);
    }
    
    /* 按钮样式 */
    .stButton > button {
        background: linear-gradient(135deg, #00B4D8 0%, #0096C7 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(0, 180, 216, 0.3);
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #0096C7 0%, #0077B6 100%);
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0, 180, 216, 0.4);
    }
    
    /* 主要按钮样式 */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #4CAF50 0%, #45A049 100%);
        box-shadow: 0 4px 6px rgba(76, 175, 80, 0.3);
    }
    
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #45A049 0%, #388E3C 100%);
        box-shadow: 0 6px 12px rgba(76, 175, 80, 0.4);
    }
    
    /* 次要按钮样式 */
    .stButton > button[kind="secondary"] {
        background: linear-gradient(135deg, #00B4D8 0%, #0096C7 100%);
        box-shadow: 0 4px 6px rgba(0, 180, 216, 0.3);
    }
    
    /* 卡片样式 */
    .element-container {
        background: white;
        border-radius: 12px;
        padding: 1rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    
    /* Metric卡片样式 */
    [data-testid="stMetricValue"] {
        color: #00695C;
        font-weight: 700;
    }
    
    [data-testid="stMetricLabel"] {
        color: #4A90A4;
        font-weight: 600;
    }
    
    /* 表格样式 */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
    
    /* 输入框样式 */
    .stTextInput > div > div > input {
        border: 2px solid #E0E0E0;
        border-radius: 8px;
        transition: border-color 0.3s ease;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #00B4D8;
        box-shadow: 0 0 0 3px rgba(0, 180, 216, 0.1);
    }
    
    /* 选择框样式 */
    .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid #E0E0E0;
    }
    
    /* 标签页样式：强制横向滚动，避免右侧 Tab 被裁切 */
    .stTabs, .stTabs > div, [data-testid="stVerticalBlock"] > .stTabs {
        overflow-x: auto !important;
        overflow-y: hidden !important;
        max-width: 100% !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        overflow-x: auto !important;
        overflow-y: hidden !important;
        flex-wrap: nowrap !important;
        padding-bottom: 6px;
        max-width: 100% !important;
        -webkit-overflow-scrolling: touch;
        display: flex !important;
    }
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
        height: 8px;
    }
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
        background: #2DB7B0;
        border-radius: 4px;
    }
    .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
        background: #E5F2F1;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: #E8F5E9;
        border-radius: 8px 8px 0 0;
        color: #00695C;
        font-weight: 600;
        padding: 0.75rem 1.5rem;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #00B4D8 0%, #0096C7 100%);
        color: white;
    }
    
    /* 成功消息样式 */
    .stSuccess {
        background: linear-gradient(135deg, #4CAF50 0%, #45A049 100%);
        color: white;
        border-radius: 8px;
        padding: 1rem;
        border-left: 4px solid #2E7D32;
    }
    
    /* 错误消息样式 */
    .stError {
        background: linear-gradient(135deg, #F44336 0%, #E53935 100%);
        color: white;
        border-radius: 8px;
        padding: 1rem;
        border-left: 4px solid #C62828;
    }
    
    /* 信息消息样式 */
    .stInfo {
        background: linear-gradient(135deg, #00B4D8 0%, #0096C7 100%);
        color: white;
        border-radius: 8px;
        padding: 1rem;
        border-left: 4px solid #0077B6;
    }
    
    /* 警告消息样式 */
    .stWarning {
        background: linear-gradient(135deg, #FF9800 0%, #F57C00 100%);
        color: white;
        border-radius: 8px;
        padding: 1rem;
        border-left: 4px solid #E65100;
    }
    
    /* 分隔线样式 */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent 0%, #00B4D8 50%, transparent 100%);
        margin: 2rem 0;
    }
    
    /* 代码块样式 */
    .stCodeBlock {
        border-radius: 8px;
        border: 1px solid #E0E0E0;
    }
    
    /* 进度条样式 */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #4CAF50 0%, #00B4D8 100%);
    }
    
    /* 图表容器样式 */
    [data-testid="stPlotlyChart"] {
        border-radius: 12px;
        padding: 1rem;
        background: white;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
</style>
""", unsafe_allow_html=True)

# 标题区域
st.title("⚡ 充电数据管理系统")
st.markdown("""
<div style='text-align: center; color: #4A90A4; font-size: 1.2rem; margin-bottom: 2rem;'>
    🚀 支持EXCEL数据自动读取、清洗、入库和比对分析
</div>
""", unsafe_allow_html=True)

# 侧边栏 - 诊断工具
with st.sidebar:
    st.divider()
    if st.button("🔧 运行系统诊断", width="stretch"):
        import subprocess
        import sys
        
        with st.spinner("正在运行诊断..."):
            try:
                result = subprocess.run(
                    [sys.executable, "test_data_system.py"],
                    capture_output=True,
                    text=True,
                    encoding='utf-8'
                )
                st.code(result.stdout)
                if result.stderr:
                    st.error("诊断过程中出现错误:")
                    st.code(result.stderr)
            except Exception as e:
                st.error(f"运行诊断失败: {str(e)}")


# 获取数据库表列表
@st.cache_resource
def get_database_tables():
    """获取数据库中的所有表名"""
    try:
        from sqlalchemy import inspect
        engine = create_db_engine(echo=False)  # 使用统一工具函数
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return sorted(tables)
    except Exception as e:
        st.error(f"获取表列表失败: {str(e)}")
        return ['table2509ev']  # 返回默认表名作为备选

# 初始化session_state
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = 'evdata'  # 默认值设为evdata

if 'chart_data' not in st.session_state:
    st.session_state.chart_data = None
if 'operator_list' not in st.session_state:
    st.session_state.operator_list = []

# 侧边栏配置
with st.sidebar:
    st.header("⚙️ 配置选项")
    
    # 表名选择
    try:
        tables = get_database_tables()
        if tables:
            # 确定默认索引：优先evdata，否则使用session_state中的值，再否则使用0
            default_index = 0
            if 'evdata' in tables:
                default_index = tables.index('evdata')
            elif st.session_state.selected_table in tables:
                default_index = tables.index(st.session_state.selected_table)
            
            selected_table = st.selectbox(
                "选择数据表",
                options=tables,
                index=default_index,
                help="选择要操作的数据表"
            )
            st.session_state.selected_table = selected_table
        else:
            selected_table = st.text_input(
                "输入表名",
                value=st.session_state.selected_table,
                help="输入要操作的数据表名称"
            )
            st.session_state.selected_table = selected_table
    except Exception as e:
        selected_table = st.text_input(
            "输入表名",
            value=st.session_state.selected_table,
            help="输入要操作的数据表名称"
        )
        st.session_state.selected_table = selected_table
    
    st.info(f"📋 当前表: **{st.session_state.selected_table}**")
    
    # 数据导入模式
    import_mode = st.radio(
        "导入模式",
        ["追加模式 (append)", "替换模式 (replace)", "更新/插入模式 (upsert)"],
        help="追加：直接添加新数据；替换：清空表后导入；更新/插入：存在则更新，不存在则插入"
    )
    
    # 唯一键设置（用于upsert）
    unique_key = st.text_input(
        "唯一键字段",
        value="充电桩编号",
        help="用于识别重复记录的字段名（仅在更新/插入模式使用）"
    )
    
    # ========== 数据缓存管理 ==========
    st.divider()
    st.subheader("🚀 数据缓存")
    
    # 获取Pandas数据服务
    try:
        from services.pandas_data_service import PandasDataService
        pandas_service = PandasDataService.get_instance()
        
        cache_info = pandas_service.get_cache_info()
        
        if cache_info['is_loaded']:
            st.success(f"✅ 缓存已加载")
            st.caption(f"📊 {cache_info['record_count']:,} 条数据")
            st.caption(f"💾 {cache_info['memory_mb']:.1f} MB")
            cache_age = int(cache_info['cache_age']) if cache_info['cache_age'] else 0
            st.caption(f"⏱️ {cache_age}秒前加载")
            
            if st.button("🗑️ 清除缓存", width="stretch"):
                pandas_service.clear_cache()
                st.success("缓存已清除")
                st.rerun()
        else:
            st.info("💤 缓存未加载")
            st.caption("首次查询会自动加载")
            
            if st.button("📥 预加载数据", width="stretch"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(current, total, message):
                    progress_bar.progress(current / total if total > 0 else 0)
                    status_text.text(message)
                
                with st.spinner("正在加载数据..."):
                    pandas_service.get_dataframe(progress_callback=update_progress)
                
                st.success("✅ 数据加载完成！")
                st.rerun()
    except Exception as e:
        st.warning(f"缓存服务不可用: {str(e)}")

# 初始化Handler类
# 注意：不使用缓存，每次表名改变时都重新初始化，确保使用正确的表名
def init_handlers(table_name: str):
    """初始化所有Handler类"""
    return {
        'upload': FileUploadHandler(),
        'import': DataImportHandler(table_name),
        'preview': DataPreviewHandler(table_name),
        'analysis': DataAnalysisHandler(table_name),
        'compare': DataCompareHandler(table_name),
        'quality': DataQualityHandler(table_name),
        'cleaning': DataCleaningHandler(table_name),
        'pdf': PDFReportHandler(),
        'text_report': TextReportHandler()
    }

# 每次页面运行时都重新初始化handlers，确保使用当前选择的表名
handlers = init_handlers(st.session_state.selected_table)

# 创建标签页
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📤 数据导入",
    "👁️ 数据预览",
    "📊 数据分析统计",
    "🔄 数据比对",
    "📋 数据质量报告",
    "🧹 数据清洗",
    "🗺️ 区域编码清洗",
    "📎 多表合并",
])

# ========== 标签页1: 数据导入 ==========
with tab1:
    st.header("📤 数据导入")
    
    # 权限检查：只有管理员可以导入数据
    if not PermissionChecker.is_admin():
        st.warning("⚠️ 数据导入功能仅限管理员使用")
        st.info("如需导入数据，请联系管理员操作。")
        uploaded_files = []  # 非管理员无法上传
    else:
        uploaded_files = st.file_uploader(
            "上传EXCEL或CSV文件（支持批量上传）",
            type=['xlsx', 'xls', 'csv'],
            accept_multiple_files=True,
            help="支持.xlsx, .xls, .csv格式，可同时选择多个文件，最大单文件500MB"
        )
    
    if uploaded_files:
        # 显示文件队列
        st.subheader(f"📁 文件队列 ({len(uploaded_files)} 个文件)")
        
        # 计算总大小
        total_size_mb = sum(f.size / 1024 / 1024 for f in uploaded_files)
        has_large_file = any(f.size > 50 * 1024 * 1024 for f in uploaded_files)
        
        # 显示文件列表
        file_list_data = []
        for i, f in enumerate(uploaded_files, 1):
            size_mb = f.size / 1024 / 1024
            file_list_data.append({
                '序号': i,
                '文件名': f.name,
                '大小': f"{size_mb:.2f} MB",
                '类型': '大文件' if size_mb > 50 else '普通'
            })
        
        st.dataframe(pd.DataFrame(file_list_data), width="stretch", hide_index=True)
        
        # 汇总信息
        col_sum1, col_sum2, col_sum3 = st.columns(3)
        with col_sum1:
            st.metric("文件数量", len(uploaded_files))
        with col_sum2:
            st.metric("总大小", f"{total_size_mb:.2f} MB")
        with col_sum3:
            st.metric("大文件数", sum(1 for f in uploaded_files if f.size > 50 * 1024 * 1024))
        
        # 处理选项
        st.subheader("处理选项")
        col_option1, col_option2 = st.columns(2)
        with col_option1:
            sheet_name = st.text_input("工作表名称（Excel）", value="", help="留空则读取第一个工作表，批量导入时所有文件使用相同设置")
            sheet_name = None if not sheet_name else sheet_name
        with col_option2:
            if has_large_file:
                st.info("📦 检测到大文件，将自动使用分块处理模式")
        
        # 批次大小选择
        chunk_size = 5000  # 默认值
        if has_large_file:
            st.subheader("批次设置")
            chunk_size = st.slider(
                "每批处理行数",
                min_value=1000,
                max_value=20000,
                value=5000,
                step=1000,
                help="建议值：1000-20000，较大的值可以提高处理速度但需要更多内存"
            )
        
        # 批量导入说明
        if len(uploaded_files) > 1:
            st.info("💡 **批量导入说明**：第一个文件使用所选导入模式，后续文件自动使用追加模式")
        
        # 确定导入模式
        if_exists = 'append'
        use_upsert = False
        if "追加" in import_mode:
            if_exists = 'append'
        elif "替换" in import_mode:
            if_exists = 'replace'
        elif "更新/插入" in import_mode:
            if_exists = 'append'
            use_upsert = True
        
        # 处理按钮
        if st.button("🚀 开始批量处理", type="primary", width="stretch"):
            # 保存所有文件
            saved_files = handlers['upload'].save_multiple_files(uploaded_files)
            valid_paths = [path for path in saved_files.values() if path is not None]
            
            if not valid_paths:
                st.error("❌ 文件保存失败，请重试")
            else:
                # 进度显示
                total_progress = st.progress(0)
                file_status = st.empty()
                detail_container = st.container()
                
                # 实时状态跟踪
                file_statuses = {f.name: '⏳ 等待中' for f in uploaded_files}
                
                def update_file_status_display():
                    """更新文件状态显示"""
                    status_text = " | ".join([f"{name}: {status}" for name, status in file_statuses.items()])
                    file_status.text(status_text[:200] + "..." if len(status_text) > 200 else status_text)
                
                def batch_progress_callback(file_idx, total_files, file_name, status):
                    """批量导入进度回调"""
                    if status == 'processing':
                        file_statuses[file_name] = '🔄 处理中'
                    elif status == 'success':
                        file_statuses[file_name] = '✅ 完成'
                    elif status == 'failed':
                        file_statuses[file_name] = '❌ 失败'
                    
                    progress = (file_idx + 1) / total_files if status != 'processing' else file_idx / total_files
                    total_progress.progress(progress)
                    update_file_status_display()
                
                # 执行批量导入
                with st.spinner(f"正在处理 {len(valid_paths)} 个文件..."):
                    batch_result = handlers['import'].import_files_batch(
                        file_paths=valid_paths,
                        if_exists=if_exists,
                        use_upsert=use_upsert,
                        unique_key=unique_key,
                        chunk_size=chunk_size,
                        progress_callback=batch_progress_callback
                    )
                
                total_progress.progress(1.0)
                
                # 显示结果汇总
                st.divider()
                st.subheader("📊 批量导入结果")
                
                # 总体统计
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("成功文件", f"{batch_result['success_files']}/{batch_result['total_files']}")
                with col2:
                    st.metric("失败文件", batch_result['failed_files'])
                with col3:
                    st.metric("总读取行数", f"{batch_result['total_rows_read']:,}")
                with col4:
                    st.metric("总入库行数", f"{batch_result['total_rows_loaded']:,}")
                
                # 状态消息
                if batch_result['status'] == 'success':
                    st.success(f"✅ 批量导入完成！全部 {batch_result['total_files']} 个文件处理成功")
                elif batch_result['status'] == 'partial_success':
                    st.warning(f"⚠️ 部分完成：{batch_result['success_files']} 个成功，{batch_result['failed_files']} 个失败")
                else:
                    st.error(f"❌ 批量导入失败：全部 {batch_result['total_files']} 个文件处理失败")
                
                # 详细结果表格
                with st.expander("📋 文件处理详情", expanded=True):
                    result_data = []
                    for fr in batch_result['file_results']:
                        status_icon = '✅' if fr['status'] in ['success', 'partial_success'] else '❌'
                        result_data.append({
                            '状态': status_icon,
                            '文件名': fr['file'],
                            '大小': f"{fr['size_mb']:.2f} MB",
                            '读取行数': fr['rows_read'],
                            '入库行数': fr['rows_loaded'],
                            '错误': fr.get('error', '-') or '-'
                        })
                    
                    st.dataframe(pd.DataFrame(result_data), width="stretch", hide_index=True)
                
                # 记录审计日志
                current_username = SessionManager.get_username() or 'anonymous'
                audit_log(
                    username=current_username,
                    action_type='batch_import',
                    action_desc=f'批量导入: {batch_result["success_files"]}/{batch_result["total_files"]} 成功',
                    module='data_import',
                    target_table='evdata',
                    affected_rows=batch_result['total_rows_loaded'],
                    result_status=batch_result['status'],
                    request_params={
                        'total_files': batch_result['total_files'],
                        'success_files': batch_result['success_files'],
                        'failed_files': batch_result['failed_files'],
                        'total_rows_read': batch_result['total_rows_read'],
                        'total_rows_loaded': batch_result['total_rows_loaded'],
                        'if_exists': if_exists,
                        'file_names': [f.name for f in uploaded_files]
                    }
                )

# ========== 标签页2: 数据预览 ==========
with tab2:
    st.header("👁️ 数据预览")
    
    # 预览选项
    col_preview1, col_preview2 = st.columns(2)
    with col_preview1:
        preview_limit = st.number_input("预览行数", min_value=1, max_value=10000, value=100)
    with col_preview2:
        if st.button("🔄 刷新预览", width="stretch"):
            st.rerun()
    
    # 使用DataPreviewHandler处理预览
    preview_result = handlers['preview'].preview_data(limit=preview_limit)
    
    if preview_result['success']:
        st.dataframe(preview_result['data'], width="stretch")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("总记录数", preview_result['row_count'])
        with col2:
            st.metric("字段数", preview_result['column_count'])
    else:
        st.error(f"❌ 预览失败: {preview_result.get('error', '未知错误')}")
        
        with st.expander("🔍 查看详细错误信息"):
            st.code(ErrorHandler.format_error_report(preview_result.get('error_details', {})))

# ========== 标签页3: 数据分析统计 ==========
with tab3:
    st.header("📊 数据分析统计")
    
    # 分析设置
    col_setting1, col_setting2 = st.columns(2)
    with col_setting1:
        analysis_type = st.radio(
            "分析类型",
            ["总体统计", "按运营商统计", "按区域统计", "按类型统计", "按充电站统计"],
            horizontal=True
        )
    with col_setting2:
        # 记录数限制设置（默认查全量数据）
        record_limit = st.number_input(
            "统计记录数",
            min_value=0,
            value=None,
            placeholder="全部",
            help="留空或输入0表示统计全部记录，可设置具体数值限制查询范围"
        )
        limit_value = None if (record_limit is None or record_limit == 0) else int(record_limit)
    
    # 运营商筛选（手动加载运营商列表）
    operator_filter = None  # 初始化为 None
    
    # 运营商列表管理区域
    col_op1, col_op2 = st.columns([3, 1])
    with col_op1:
        st.write("**运营商筛选（可选）**")
    with col_op2:
        if st.button("🔄 更新运营商列表", width="stretch", help="从数据库重新加载运营商列表"):
            try:
                with st.spinner("正在加载运营商列表..."):
                    operator_list = handlers['analysis'].get_operator_list()
                    if operator_list:
                        st.session_state.operator_list = operator_list
                        st.success(f"✅ 已加载 {len(operator_list)} 个运营商")
                    else:
                        st.session_state.operator_list = []
                        st.info("ℹ️ 暂无运营商数据")
            except Exception as e:
                st.error(f"❌ 获取运营商列表失败: {str(e)}")
                st.session_state.operator_list = []
    
    # 使用缓存的运营商列表
    operator_list = st.session_state.operator_list
    if operator_list:
        selected_operators = st.multiselect(
            "选择要统计的运营商",
            options=operator_list,
            default=[],
            help="选择要统计的运营商，留空表示统计全部运营商",
            key="operator_multiselect_filter"
        )
        operator_filter = selected_operators if selected_operators else None
    else:
        if len(st.session_state.operator_list) == 0:
            st.info("💡 点击上方「更新运营商列表」按钮加载运营商列表")
        operator_filter = None
    
    # 时间维度筛选
    col_date1, col_date2, col_date3 = st.columns(3)
    with col_date1:
        date_dimension = st.selectbox(
            "时间维度",
            ["无", "充电站投入使用时间", "充电桩生产日期"],
            help="选择基于哪个时间字段进行筛选"
        )
        date_field_value = None if date_dimension == "无" else date_dimension
    with col_date2:
        try:
            start_date_value = st.date_input(
                "开始日期",
                value=None,
                help="留空表示不限制开始日期"
            )
        except Exception as e:
            st.error(f"❌ 开始日期控件错误: {str(e)}")
            start_date_value = None
    with col_date3:
        try:
            end_date_value = st.date_input(
                "结束日期",
                value=None,
                help="留空表示不限制结束日期"
            )
        except Exception as e:
            st.error(f"❌ 结束日期控件错误: {str(e)}")
            end_date_value = None
    
    # 验证日期范围
    if start_date_value and end_date_value and start_date_value > end_date_value:
        st.error("❌ 开始日期不能晚于结束日期！")
        st.stop()
    
    # 区域筛选（三级联动）
    st.subheader("📍 区域筛选（可选）")
    
    # 初始化区域字典
    if 'region_dict' not in st.session_state:
        st.session_state.region_dict = RegionDictionary(table_name=handlers['analysis'].table_name)
        st.session_state.region_data = None  # 延迟加载
    
    region_dict = st.session_state.region_dict
    
    # 区域列表管理区域
    col_region1, col_region2 = st.columns([3, 1])
    with col_region1:
        st.write("**三级联动区域筛选**")
    with col_region2:
        if st.button("🔄 更新区域列表", width="stretch", help="从数据库重新加载区域列表"):
            try:
                with st.spinner("正在加载区域数据..."):
                    st.session_state.region_data = region_dict.load_from_database(force_refresh=True)
                    stats_info = region_dict.get_statistics()
                    st.success(f"✅ 已加载 {stats_info['total_provinces']} 个省份，{stats_info['total_cities']} 个城市，{stats_info['total_districts']} 个区县")
            except Exception as e:
                st.error(f"❌ 获取区域列表失败: {str(e)}")
                st.session_state.region_data = None
    
    # 首次加载区域数据（如果未加载）
    if st.session_state.region_data is None:
        with st.spinner("正在加载区域数据..."):
            try:
                st.session_state.region_data = region_dict.load_from_database()
                stats_info = region_dict.get_statistics()
                st.info(f"ℹ️ 已加载 {stats_info['total_provinces']} 个省份，{stats_info['total_cities']} 个城市，{stats_info['total_districts']} 个区县")
            except Exception as e:
                st.error(f"❌ 加载区域数据失败: {str(e)}")
                st.session_state.region_data = {
                    'provinces': [],
                    'province_cities': {},
                    'province_city_districts': {}
                }
    
    # 三级联动菜单
    region_data = st.session_state.region_data
    region_filter = None
    
    if region_data and region_data.get('provinces'):
        col_province, col_city, col_district = st.columns(3)
        
        with col_province:
            provinces = ['全部'] + region_data['provinces']
            selected_province = st.selectbox(
                "省份",
                options=provinces,
                index=0,
                help="选择要筛选的省份",
                key="region_province_select"
            )
        
        # 根据省份筛选城市
        selected_city = None
        selected_district = None
        
        # 直辖市列表
        direct_cities = ['北京市', '上海市', '天津市', '重庆市']
        
        if selected_province and selected_province != '全部':
            # 直辖市特殊处理：城市=省份，直接显示区县
            if selected_province in direct_cities:
                selected_city = selected_province  # 直辖市的城市就是省份本身
                
                with col_city:
                    st.selectbox(
                        "城市",
                        options=[selected_province],
                        index=0,
                        help="直辖市",
                        key="region_city_select",
                        disabled=True
                    )
                
                # 直接显示区县
                districts = region_dict.get_districts(selected_province, selected_city)
                if districts:
                    with col_district:
                        district_options = ['全部'] + districts
                        selected_district = st.selectbox(
                            "区县",
                            options=district_options,
                            index=0,
                            help="选择要筛选的区县",
                            key="region_district_select"
                        )
                else:
                    with col_district:
                        st.selectbox(
                            "区县",
                            options=['全部'],
                            index=0,
                            help="该直辖市暂无区县数据",
                            key="region_district_select",
                            disabled=True
                        )
            else:
                # 普通省份：先选城市再选区县
                cities = region_dict.get_cities(selected_province)
                if cities:
                    with col_city:
                        city_options = ['全部'] + cities
                        selected_city = st.selectbox(
                            "城市",
                            options=city_options,
                            index=0,
                            help="选择要筛选的城市",
                            key="region_city_select"
                        )
                    
                    # 根据城市筛选区县
                    if selected_city and selected_city != '全部':
                        districts = region_dict.get_districts(selected_province, selected_city)
                        if districts:
                            with col_district:
                                district_options = ['全部'] + districts
                                selected_district = st.selectbox(
                                    "区县",
                                    options=district_options,
                                    index=0,
                                    help="选择要筛选的区县",
                                    key="region_district_select"
                                )
                        else:
                            with col_district:
                                st.selectbox(
                                    "区县",
                                    options=['全部'],
                                    index=0,
                                    help="该城市暂无区县数据",
                                    key="region_district_select",
                                    disabled=True
                                )
                    else:
                        with col_district:
                            st.selectbox(
                                "区县",
                                options=['全部'],
                                index=0,
                                help="请先选择城市",
                                key="region_district_select",
                                disabled=True
                            )
                else:
                    with col_city:
                        st.selectbox(
                            "城市",
                            options=['全部'],
                            index=0,
                            help="该省份暂无城市数据",
                            key="region_city_select",
                            disabled=True
                        )
                    with col_district:
                        st.selectbox(
                            "区县",
                            options=['全部'],
                            index=0,
                            help="请先选择城市",
                            key="region_district_select",
                            disabled=True
                        )
        else:
            with col_city:
                st.selectbox(
                    "城市",
                    options=['全部'],
                    index=0,
                    help="请先选择省份",
                    key="region_city_select",
                    disabled=True
                )
            with col_district:
                st.selectbox(
                    "区县",
                    options=['全部'],
                    index=0,
                    help="请先选择城市",
                    key="region_district_select",
                    disabled=True
                )
        
        # 构建区域筛选条件
        if selected_province and selected_province != '全部':
            region_filter = {}
            if selected_province != '全部':
                region_filter['province'] = selected_province
            if selected_city and selected_city != '全部':
                region_filter['city'] = selected_city
            if selected_district and selected_district != '全部':
                region_filter['district'] = selected_district
            
            # 如果region_filter为空，设置为None
            if not region_filter:
                region_filter = None
            else:
                # 显示当前筛选条件
                filter_text = []
                if region_filter.get('province'):
                    filter_text.append(f"省份: {region_filter['province']}")
                if region_filter.get('city'):
                    filter_text.append(f"城市: {region_filter['city']}")
                if region_filter.get('district'):
                    filter_text.append(f"区县: {region_filter['district']}")
                if filter_text:
                    st.info("📍 当前筛选条件: " + " | ".join(filter_text))
    else:
        st.info("💡 点击上方「更新区域列表」按钮加载区域数据")
    
    # 功率筛选（新增）
    st.subheader("⚡ 功率筛选（可选）")
    power_ranges = [
        '≤7kW（慢充）',
        '7-30kW（小功率）',
        '30-60kW（中功率）',
        '60-120kW（大功率）',
        '>120kW（超快充）'
    ]
    selected_power_ranges = st.multiselect(
        "选择功率区间",
        options=power_ranges,
        default=[],
        help="选择要统计的功率区间，留空表示统计全部功率范围",
        key="power_range_multiselect"
    )
    power_filter = selected_power_ranges if selected_power_ranges else None

    # 功率比较：优先五种单值（> < = ≥ ≤）+ 介于区间（最小值～最大值），默认为空
    power_op = st.selectbox(
        "比较方式",
        options=["无", "大于", "小于", "等于", "大于等于", "小于等于", "介于"],
        index=0,
        help="单值：与右侧数值比较；介于：在最小值与最大值之间；选「无」表示不按此条件筛选",
        key="power_op_select"
    )
    power_value = None
    power_value_min = None
    power_value_max = None
    if power_op == "介于":
        col_min, col_max = st.columns(2)
        with col_min:
            power_value_min_raw = st.text_input(
                "功率最小值 (kW)",
                value="",
                placeholder="如 60",
                help="介于时必填",
                key="power_value_min_input"
            )
        with col_max:
            power_value_max_raw = st.text_input(
                "功率最大值 (kW)",
                value="",
                placeholder="如 660",
                help="介于时必填",
                key="power_value_max_input"
            )
        if power_value_min_raw and power_value_min_raw.strip():
            try:
                power_value_min = float(power_value_min_raw.strip())
            except ValueError:
                pass
        if power_value_max_raw and power_value_max_raw.strip():
            try:
                power_value_max = float(power_value_max_raw.strip())
            except ValueError:
                pass
    elif power_op and power_op != "无":
        power_value_raw = st.text_input(
            "功率数值 (kW)",
            value="",
            placeholder="如 60 或 660，留空表示不按此条件筛选",
            help="单值比较时填写",
            key="power_value_input"
        )
        if power_value_raw and power_value_raw.strip():
            try:
                power_value = float(power_value_raw.strip())
            except ValueError:
                pass

    # 直流/交流筛选（新增）
    st.subheader("🔌 充电类型筛选（可选）")
    charge_types = ['直流', '交流']
    selected_charge_types = st.multiselect(
        "选择充电类型",
        options=charge_types,
        default=[],
        help="选择要统计的充电类型，留空表示统计全部类型",
        key="charge_type_multiselect"
    )
    charge_type_filter = selected_charge_types if selected_charge_types else None
    
    # 生成统计按钮
    if st.button("📊 生成统计报告", type="primary", width="stretch"):
        import time
        start_time = time.time()
        
        try:
            # 使用DataAnalysisHandler处理分析
            with st.spinner("正在生成统计报告..."):
                analysis_result = handlers['analysis'].get_statistics(
                    analysis_type=analysis_type,
                    limit=limit_value,
                    date_field=date_field_value,
                    start_date=start_date_value,
                    end_date=end_date_value,
                    operator_filter=operator_filter,
                    power_filter=power_filter,
                    power_op=power_op,
                    power_value=power_value,
                    power_value_min=power_value_min,
                    power_value_max=power_value_max,
                    region_filter=region_filter,
                    charge_type_filter=charge_type_filter
                )
        except Exception as e:
            st.error(f"❌ 生成统计报告时发生错误: {str(e)}")
            import traceback
            with st.expander("🔍 查看详细错误信息"):
                st.code(traceback.format_exc())
            st.stop()
        
        # 计算用时
        elapsed_time = time.time() - start_time
        
        if analysis_result.get('success'):
            stats = analysis_result['stats']
            
            # 显示统计信息
            info_messages = []
            if limit_value is None or limit_value > 0:
                actual_limit = limit_value if limit_value else "全部"
                info_messages.append(f"记录数: {actual_limit}")
            if date_field_value and (start_date_value or end_date_value):
                date_range_str = f"{start_date_value or '开始'} 至 {end_date_value or '结束'}"
                info_messages.append(f"时间范围 ({date_field_value}): {date_range_str}")
            
            if info_messages:
                st.info("ℹ️  " + " | ".join(info_messages))
            
            st.success(f"✅ 统计报告生成成功！本次生成用时 {elapsed_time:.2f} 秒")
            
            # 记录统计查询审计日志
            current_username = SessionManager.get_username() or 'anonymous'
            audit_log(
                username=current_username,
                action_type='query',
                action_desc=f'统计分析: {analysis_type}',
                module='data_analysis',
                target_table='evdata',
                execution_time_ms=int(elapsed_time * 1000),
                request_params={
                    'analysis_type': analysis_type,
                    'total_records': stats.get('total_records', 0),
                    'region_filter': region_filter,
                    'operator_filter': operator_filter[:3] if operator_filter else None  # 只记录前3个
                }
            )
            
            # 准备图表数据（传入region_filter用于智能下钻）
            chart_data = handlers['analysis'].prepare_chart_data(stats, analysis_type, region_filter)
            
            # 显示总体统计
            col1, col2 = st.columns(2)
            with col1:
                st.metric("总记录数", stats.get('total_records', 0))
            
            # 对于总体统计，显示基本统计信息
            if analysis_type == "总体统计":
                if 'basic_stats' in stats:
                    bs = stats['basic_stats']
                    st.subheader("📊 基本统计信息")
                    col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
                    with col_stats1:
                        if 'unique_piles' in bs:
                            st.metric("唯一充电桩数", bs['unique_piles'])
                    with col_stats2:
                        if 'unique_stations' in bs:
                            st.metric("唯一充电站数", bs['unique_stations'])
                    with col_stats3:
                        if 'by_operator' in bs:
                            st.metric("运营商数量", len(bs['by_operator']))
                    with col_stats4:
                        if 'by_location' in bs:
                            st.metric("区域数量", len(bs['by_location']))
            
            # 显示功率统计（新增）
            if 'basic_stats' in stats and 'by_power' in stats['basic_stats']:
                power_stats_data = handlers['analysis'].prepare_power_statistics(stats)
                if power_stats_data:
                    st.subheader("⚡ 额定功率统计")
                    
                    # 功率统计概览
                    col_power1, col_power2 = st.columns([1, 2])
                    with col_power1:
                        st.write("**功率统计概览**")
                        st.dataframe(power_stats_data['summary'], width="stretch", hide_index=True)
                    
                    # 功率区间分布
                    if not power_stats_data['by_range'].empty:
                        with col_power2:
                            st.write("**功率区间分布**")
                            # 创建功率区间分布图表
                            col_chart_power1, col_chart_power2 = st.columns(2)
                            with col_chart_power1:
                                handlers['analysis'].create_bar_chart(
                                    power_stats_data['by_range'],
                                    '功率区间',
                                    '数量',
                                    '功率区间分布柱状图'
                                )
                            with col_chart_power2:
                                fig_power_pie = handlers['analysis'].create_pie_chart(
                                    power_stats_data['by_range'],
                                    '数量',
                                    '功率区间',
                                    '功率区间分布饼图'
                                )
                                if fig_power_pie:
                                    st.plotly_chart(fig_power_pie, width="stretch")
                                else:
                                    st.info("📊 暂无数据可供展示")
                            
                            # 显示功率区间分布数据表格
                            st.dataframe(power_stats_data['by_range'], width="stretch", hide_index=True)
            
            # 在总体统计下，显示运营商和区域统计饼图
            if analysis_type == "总体统计" and 'basic_stats' in stats:
                bs = stats['basic_stats']
                
                # 运营商统计饼图（使用fragment实现局部刷新）
                if 'by_operator' in bs and bs['by_operator']:
                    st.subheader("📊 运营商分布统计")
                    
                    # 准备数据
                    operator_df_full = pd.DataFrame(
                        list(bs['by_operator'].items()),
                        columns=['运营商', '数量']
                    ).sort_values('数量', ascending=False).reset_index(drop=True)
                    total_operator = operator_df_full['数量'].sum()
                    operator_df_full['占比'] = (operator_df_full['数量'] / total_operator * 100).round(2).astype(str) + '%'
                    all_operators = operator_df_full['运营商'].tolist()
                    
                    # 初始化默认选择（图表用，与上方筛选区的 operator_multiselect_filter 区分）
                    if 'op_selection_init_chart' not in st.session_state:
                        st.session_state.op_selection_init_chart = True
                        st.session_state.operator_multiselect_chart = all_operators[:10]
                    
                    # 使用fragment装饰器实现局部刷新
                    @st.fragment
                    def render_operator_chart():
                        col1, col2 = st.columns([1, 1])
                        
                        with col2:
                            st.markdown("**📌 快捷选择**")
                            btn_cols = st.columns(4)
                            
                            with btn_cols[0]:
                                if st.button("前10", key="op_top10_frag", width="stretch"):
                                    st.session_state.operator_multiselect_chart = all_operators[:10]
                            with btn_cols[1]:
                                if st.button("前20", key="op_top20_frag", width="stretch"):
                                    st.session_state.operator_multiselect_chart = all_operators[:20]
                            with btn_cols[2]:
                                if st.button("前50", key="op_top50_frag", width="stretch"):
                                    st.session_state.operator_multiselect_chart = all_operators[:50]
                            with btn_cols[3]:
                                if st.button("全部", key="op_all_frag", width="stretch"):
                                    st.session_state.operator_multiselect_chart = all_operators
                            
                            selected = st.multiselect(
                                "🔍 自定义选择运营商",
                                options=all_operators,
                                key="operator_multiselect_chart"
                            )
                            
                            st.markdown(f"**📊 运营商列表** (已选 {len(selected)} / {len(operator_df_full)})")
                            st.dataframe(
                                operator_df_full[['运营商', '数量', '占比']],
                                width="stretch",
                                hide_index=True,
                                height=400
                            )
                            
                            # 导出按钮
                            if st.button("📥 导出运营商统计", key="export_operator_stats", width="stretch"):
                                import io
                                from datetime import datetime
                                
                                output = io.BytesIO()
                                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                    # Sheet1: 饼图所选运营商
                                    if selected:
                                        selected_df = operator_df_full[operator_df_full['运营商'].isin(selected)].copy()
                                        selected_df.to_excel(writer, sheet_name='饼图所选运营商', index=False)
                                    # Sheet2: 全部运营商列表
                                    operator_df_full.to_excel(writer, sheet_name='全部运营商统计', index=False)
                                output.seek(0)
                                
                                st.download_button(
                                    label="⬇️ 点击下载 Excel",
                                    data=output.getvalue(),
                                    file_name=f"运营商统计_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="download_operator_excel"
                                )
                        
                        with col1:
                            if selected:
                                filtered_df = operator_df_full[operator_df_full['运营商'].isin(selected)]
                                fig = handlers['analysis'].create_pie_chart(
                                    filtered_df, '数量', '运营商',
                                    f'运营商分布饼图 ({len(selected)}个)'
                                )
                                if fig:
                                    st.plotly_chart(fig, width="stretch")
                                else:
                                    st.info("📊 暂无运营商数据可供展示")
                            else:
                                st.warning("⚠️ 请至少选择一个运营商")
                    
                    # 调用fragment
                    render_operator_chart()
                
                # 区域统计饼图（根据查询颗粒度动态选择统计字段）
                # 根据 region_filter 选择显示的统计字段
                location_key = None
                location_label = "区域分布"
                
                # 直辖市列表
                direct_cities = ['北京市', '上海市', '天津市', '重庆市', '北京', '上海', '天津', '重庆']
                
                if region_filter:
                    province = region_filter.get('province', '')
                    city = region_filter.get('city', '')
                    
                    # 直辖市特殊处理：选择直辖市时直接显示区县
                    if province in direct_cities or city in direct_cities:
                        location_key = 'by_location'
                        location_label = "区县分布"
                    elif city:
                        # 城市级别查询 → 显示区县统计
                        location_key = 'by_location'
                        location_label = "区县分布"
                    elif province:
                        # 省份级别查询 → 显示城市统计
                        location_key = 'by_city'
                        location_label = "城市分布"
                    else:
                        # 其他情况 → 显示区县统计（兼容）
                        location_key = 'by_location'
                        location_label = "区县分布"
                else:
                    # 全局查询 → 显示省份统计
                    location_key = 'by_province'
                    location_label = "省份分布"
                
                # 显示对应的统计（使用fragment实现局部刷新）
                if location_key and location_key in bs and bs[location_key]:
                    st.subheader(f"📊 {location_label}统计")
                    
                    # 准备数据
                    location_df_full = pd.DataFrame(
                        list(bs[location_key].items()),
                        columns=['区域', '数量']
                    ).sort_values('数量', ascending=False).reset_index(drop=True)
                    total_location = location_df_full['数量'].sum()
                    location_df_full['占比'] = (location_df_full['数量'] / total_location * 100).round(2).astype(str) + '%'
                    all_locations = location_df_full['区域'].tolist()
                    
                    # 动态key
                    loc_multiselect_key = f"location_multiselect_{location_key}"
                    loc_init_key = f"loc_selection_init_{location_key}"
                    
                    if loc_init_key not in st.session_state:
                        st.session_state[loc_init_key] = True
                        st.session_state[loc_multiselect_key] = all_locations[:10]
                    
                    # 使用fragment实现局部刷新
                    @st.fragment
                    def render_location_chart():
                        col1, col2 = st.columns([1, 1])
                        
                        with col2:
                            st.markdown("**📌 快捷选择**")
                            btn_cols = st.columns(4)
                            
                            with btn_cols[0]:
                                if st.button("前10", key=f"loc_top10_{location_key}_frag", width="stretch"):
                                    st.session_state[loc_multiselect_key] = all_locations[:10]
                            with btn_cols[1]:
                                if st.button("前20", key=f"loc_top20_{location_key}_frag", width="stretch"):
                                    st.session_state[loc_multiselect_key] = all_locations[:20]
                            with btn_cols[2]:
                                if st.button("前50", key=f"loc_top50_{location_key}_frag", width="stretch"):
                                    st.session_state[loc_multiselect_key] = all_locations[:50]
                            with btn_cols[3]:
                                if st.button("全部", key=f"loc_all_{location_key}_frag", width="stretch"):
                                    st.session_state[loc_multiselect_key] = all_locations
                            
                            selected = st.multiselect(
                                f"🔍 自定义选择{location_label.replace('分布', '')}",
                                options=all_locations,
                                key=loc_multiselect_key
                            )
                            
                            st.markdown(f"**📊 {location_label.replace('分布', '')}列表** (已选 {len(selected)} / {len(location_df_full)})")
                            st.dataframe(
                                location_df_full[['区域', '数量', '占比']],
                                width="stretch",
                                hide_index=True,
                                height=400
                            )
                            
                            # 导出按钮
                            location_type = location_label.replace('分布', '')
                            if st.button(f"📥 导出{location_type}统计", key=f"export_{location_key}_stats", width="stretch"):
                                import io
                                from datetime import datetime
                                
                                output = io.BytesIO()
                                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                    # Sheet1: 饼图所选区域
                                    if selected:
                                        selected_df = location_df_full[location_df_full['区域'].isin(selected)].copy()
                                        selected_df.to_excel(writer, sheet_name=f'饼图所选{location_type}', index=False)
                                    # Sheet2: 全部区域列表
                                    location_df_full.to_excel(writer, sheet_name=f'全部{location_type}统计', index=False)
                                output.seek(0)
                                
                                st.download_button(
                                    label="⬇️ 点击下载 Excel",
                                    data=output.getvalue(),
                                    file_name=f"{location_type}统计_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"download_{location_key}_excel"
                                )
                        
                        with col1:
                            if selected:
                                filtered_df = location_df_full[location_df_full['区域'].isin(selected)]
                                fig = handlers['analysis'].create_pie_chart(
                                    filtered_df, '数量', '区域',
                                    f'{location_label}饼图 ({len(selected)}个)'
                                )
                                if fig:
                                    st.plotly_chart(fig, width="stretch")
                                else:
                                    st.info("📊 暂无区域数据可供展示")
                            else:
                                st.warning("⚠️ 请至少选择一个区域")
                    
                    # 调用fragment
                    render_location_chart()
                            
                elif 'by_location' in bs and bs['by_location']:
                    # 向后兼容
                    st.subheader("📊 区域分布统计")
                    
                    location_df_full = pd.DataFrame(
                        list(bs['by_location'].items()),
                        columns=['区域', '数量']
                    ).sort_values('数量', ascending=False).reset_index(drop=True)
                    total_location = location_df_full['数量'].sum()
                    location_df_full['占比'] = (location_df_full['数量'] / total_location * 100).round(2).astype(str) + '%'
                    all_locations_fb = location_df_full['区域'].tolist()
                    
                    if 'loc_fb_init' not in st.session_state:
                        st.session_state.loc_fb_init = True
                        st.session_state.location_multiselect_fallback = all_locations_fb[:10]
                    
                    @st.fragment
                    def render_fallback_location_chart():
                        col1, col2 = st.columns([1, 1])
                        
                        with col2:
                            st.markdown("**📌 快捷选择**")
                            btn_cols = st.columns(4)
                            
                            with btn_cols[0]:
                                if st.button("前10", key="loc_fb_top10_frag", width="stretch"):
                                    st.session_state.location_multiselect_fallback = all_locations_fb[:10]
                            with btn_cols[1]:
                                if st.button("前20", key="loc_fb_top20_frag", width="stretch"):
                                    st.session_state.location_multiselect_fallback = all_locations_fb[:20]
                            with btn_cols[2]:
                                if st.button("前50", key="loc_fb_top50_frag", width="stretch"):
                                    st.session_state.location_multiselect_fallback = all_locations_fb[:50]
                            with btn_cols[3]:
                                if st.button("全部", key="loc_fb_all_frag", width="stretch"):
                                    st.session_state.location_multiselect_fallback = all_locations_fb
                            
                            selected = st.multiselect(
                                "🔍 自定义选择区域",
                                options=all_locations_fb,
                                key="location_multiselect_fallback"
                            )
                            
                            st.markdown(f"**📊 区域列表** (已选 {len(selected)} / {len(location_df_full)})")
                            st.dataframe(
                                location_df_full[['区域', '数量', '占比']],
                                width="stretch",
                                hide_index=True,
                                height=400
                            )
                        
                        with col1:
                            if selected:
                                filtered_df = location_df_full[location_df_full['区域'].isin(selected)]
                                fig = handlers['analysis'].create_pie_chart(
                                    filtered_df, '数量', '区域',
                                    f'区域分布饼图 ({len(selected)}个)'
                                )
                                if fig:
                                    st.plotly_chart(fig, width="stretch")
                                else:
                                    st.info("📊 暂无区域数据可供展示")
                            else:
                                st.warning("⚠️ 请至少选择一个区域")
                    
                    render_fallback_location_chart()
            
            # 显示分组统计和图表
            if chart_data:
                df = chart_data['dataframe']
                chart_type = chart_data['type']
                
                st.subheader(f"📊 {chart_type}")
                
                # 显示柱状图和饼图
                col_chart1, col_chart2 = st.columns(2)
                with col_chart1:
                    handlers['analysis'].create_bar_chart(
                        df,
                        df.columns[0],
                        df.columns[1],
                        f'{chart_type}柱状图'
                    )
                with col_chart2:
                    fig_pie = handlers['analysis'].create_pie_chart(
                        df,
                        df.columns[1],
                        df.columns[0],
                        f'{chart_type}饼图'
                    )
                    if fig_pie is not None:
                        st.plotly_chart(fig_pie, width="stretch")
                    else:
                        st.info("📊 暂无数据可供展示")
                
                st.dataframe(df, width="stretch")
            else:
                st.info("📊 暂无图表数据可供显示")
            
            # 自动生成文字分析报告（在页面底部，使用流式生成）
            st.markdown("---")
            st.subheader("📊 文字分析报告")
            
            # 初始化文字报告处理器
            text_report_handler = handlers['text_report']
            
            # 检查LLM配置是否启用
            def get_llm_enabled():
                try:
                    from config import LLM_CONFIG
                    return LLM_CONFIG.get('enabled', False)
                except:
                    return False
            
            llm_config_enabled = get_llm_enabled()
            
            # 检查LLM是否可用（仅在配置启用时才尝试连接）
            use_llm_available = False
            if llm_config_enabled:
                try:
                    from langchain_community.llms import Ollama
                    llm = Ollama(model="qwen3:30b", temperature=0.1)
                    text_report_handler.llm = llm
                    use_llm_available = True
                except Exception as e:
                    use_llm_available = False
            
            # 生成模板部分（立即显示）
            try:
                stats_summary = text_report_handler._extract_stats_summary(stats, chart_data, region_filter)
                template_part = text_report_handler._generate_template_part(
                    stats, stats_summary, analysis_type,
                    region_filter, operator_filter, power_filter,
                    date_field_value, start_date_value, end_date_value
                )
                
                # 显示模板部分
                st.markdown(template_part)
                
                # 根据LLM配置决定分析方式
                if use_llm_available:
                    # LLM可用：流式生成AI深度分析
                    st.markdown("---")
                    st.markdown("### 🤖 AI深度分析")
                    analysis_placeholder = st.empty()
                    full_analysis = ""
                    
                    try:
                        for chunk in text_report_handler.generate_llm_part_stream(
                            stats_summary=stats_summary,
                            analysis_type=analysis_type,
                            region_filter=region_filter,
                            operator_filter=operator_filter,
                            power_filter=power_filter,
                            target_length=1500
                        ):
                            full_analysis += chunk
                            analysis_placeholder.markdown(full_analysis)
                        
                        # 保存完整报告到session_state
                        full_report = template_part + "\n\n---\n\n## 三、AI深度分析\n\n" + full_analysis
                        st.session_state['text_report'] = full_report
                        
                        # 下载按钮
                        st.markdown("---")
                        from datetime import datetime
                        report_filename = f"分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
                        st.download_button(
                            label="📥 下载完整报告（Markdown）",
                            data=full_report,
                            file_name=report_filename,
                            mime="text/markdown",
                            width="stretch"
                        )
                    except Exception as e:
                        st.error(f"❌ AI分析生成失败: {str(e)}")
                        # 只保存模板部分
                        st.session_state['text_report'] = template_part
                else:
                    # LLM未启用或不可用：使用模板化分析
                    st.markdown("---")
                    st.markdown("## 三、数据洞察分析\n")
                    
                    # 生成模板化深度分析
                    template_analysis = text_report_handler.generate_template_analysis(
                        stats_summary=stats_summary,
                        analysis_type=analysis_type,
                        region_filter=region_filter,
                        operator_filter=operator_filter,
                        power_filter=power_filter
                    )
                    st.markdown(template_analysis)
                    
                    # 保存完整报告
                    full_report = template_part + "\n" + template_analysis
                    st.session_state['text_report'] = full_report
                    
                    # 下载按钮
                    st.markdown("---")
                    from datetime import datetime
                    report_filename = f"分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
                    st.download_button(
                        label="📥 下载报告（Markdown）",
                        data=full_report,
                        file_name=report_filename,
                        mime="text/markdown",
                        width="stretch"
                    )
            except Exception as e:
                st.error(f"❌ 生成文字分析报告时发生错误: {str(e)}")
                import traceback
                with st.expander("🔍 查看详细错误信息"):
                    st.code(traceback.format_exc())
        else:
            error_msg = analysis_result.get('error', '未知错误')
            st.error(f"❌ 生成统计报告失败: {error_msg}")
            
            with st.expander("🔍 查看详细错误信息"):
                error_details = analysis_result.get('error_details', {})
                if error_details:
                    st.code(ErrorHandler.format_error_report(error_details))
                else:
                    st.code(str(error_msg))

# ========== 标签页4: 数据比对 ==========
with tab4:
    st.header("🔄 数据比对")
    
    st.info("💡 比对两个EXCEL文件或比对文件与数据库数据")
    
    col_compare1, col_compare2 = st.columns(2)
    with col_compare1:
        file1 = st.file_uploader("上传第一个文件", type=['xlsx', 'xls', 'csv'], key="file1")
    with col_compare2:
        file2 = st.file_uploader("上传第二个文件", type=['xlsx', 'xls', 'csv'], key="file2")
    
    compare_key = st.text_input("比对键字段", value="充电桩编号", help="用于比对的关键字段")
    
    if st.button("🔄 开始比对", type="primary", width="stretch"):
        if file1 and file2:
            # 保存文件
            file1_path = handlers['upload'].save_uploaded_file(file1)
            file2_path = handlers['upload'].save_uploaded_file(file2)
            
            if file1_path and file2_path:
                # 使用DataCompareHandler处理比对
                result = handlers['compare'].compare_files(
                    str(file1_path),
                    str(file2_path),
                    key_column=compare_key
                )
                
                if result['success']:
                    st.success("✅ 比对完成！")
                    
                    summary = handlers['compare'].get_compare_summary(result)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("文件1记录数", summary['file1_count'])
                    with col2:
                        st.metric("文件2记录数", summary['file2_count'])
                    with col3:
                        st.metric("差异记录数", summary['difference_count'])
                    
                    if result.get('differences') is not None and not result['differences'].empty:
                        st.subheader("🔍 差异记录")
                        st.dataframe(result['differences'], width="stretch")
                else:
                    st.error(f"❌ 比对失败: {result.get('error', '未知错误')}")
                    
                    with st.expander("🔍 查看详细错误信息"):
                        st.code(ErrorHandler.format_error_report(result.get('error_details', {})))
            else:
                st.error("❌ 文件保存失败")
        else:
            st.warning("⚠️ 请上传两个文件进行比对")

# ========== 标签页5: 数据质量 ==========
with tab5:
    st.header("📋 数据质量报告")
    
    quality_limit = st.number_input("统计记录数", min_value=0, value=None, placeholder="全部", help="留空或输入0表示统计全部记录")
    limit_value = None if (quality_limit is None or quality_limit == 0) else int(quality_limit)
    
    if st.button("📊 生成质量报告", type="primary", width="stretch"):
        # 使用DataQualityHandler处理质量报告
        quality_result = handlers['quality'].get_quality_report(limit=limit_value)
        
        if quality_result['success']:
            quality_report = quality_result['report']
            st.success("✅ 质量报告生成成功！")
            
            # 字段质量统计
            st.subheader("📊 字段质量统计")
            quality_df = handlers['quality'].get_quality_statistics(quality_report)
            st.dataframe(quality_df, width="stretch")
            
            # 查找重复记录
            st.subheader("🔍 重复记录检查")
            duplicates = handlers['quality'].find_duplicates(
                key_column='充电桩编号',
                limit=limit_value
            )
            
            if not duplicates.empty:
                st.warning(f"⚠️ 发现 {len(duplicates)} 条重复记录")
                st.dataframe(duplicates, width="stretch")
            else:
                st.success("✅ 未发现重复记录")
        else:
            st.error(f"❌ 生成质量报告失败: {quality_result.get('error', '未知错误')}")
            
            with st.expander("🔍 查看详细错误信息"):
                st.code(ErrorHandler.format_error_report(quality_result.get('error_details', {})))

# ========== 标签页6: 数据清洗 ==========
with tab6:
    st.header("🧹 数据清洗")
    
    # 权限检查：管理员和操作员可使用
    from auth.permission_checker import PermissionChecker
    
    def is_cleaning_allowed():
        """检查是否有数据清洗权限"""
        return PermissionChecker.is_admin() or PermissionChecker.is_operator()
    
    if not is_cleaning_allowed():
        st.warning("⚠️ 数据清洗功能仅限管理员和操作员使用")
        st.info("如需使用数据清洗功能，请联系管理员。")
    else:
        # 获取当前用户
        current_user = SessionManager.get_username() or 'unknown'
        
        # 清洗统计
        stats_result = handlers['cleaning'].get_cleaning_stats()
        if stats_result['success']:
            stats = stats_result['stats']
            col_s1, col_s2, col_s3, col_s4, col_s5 = st.columns(5)
            with col_s1:
                st.metric("正常数据", stats.get('normal', 0))
            with col_s2:
                st.metric("已标记重复", stats.get('duplicate', 0))
            with col_s3:
                st.metric("已修复", stats.get('fixed', 0))
            with col_s4:
                st.metric("已删除", stats.get('deleted', 0))
            with col_s5:
                st.metric("总计", stats.get('total', 0))
        
        st.markdown("---")
        
        # 清洗模式选择
        cleaning_mode = st.radio(
            "选择清洗模式",
            ["重复数据检测", "地址一致性校验", "已删除数据管理"],
            horizontal=True
        )
        
        # ========== 重复数据检测 ==========
        if cleaning_mode == "重复数据检测":
            st.subheader("🔍 重复数据检测")
            
            col_type, col_mode = st.columns(2)
            with col_type:
                duplicate_type = st.selectbox(
                    "检测类型",
                    ["完全重复（除UID外所有字段相同）", "高度疑似（编号+地址+运营商+功率相同）"]
                )
            with col_mode:
                scan_mode = st.selectbox(
                    "扫描模式",
                    ["分页浏览（快速）", "全量扫描（带进度）"],
                    help="分页浏览：每次显示20条，适合快速查看；全量扫描：一次性扫描所有数据，显示实时进度"
                )
            
            # 初始化session state
            if 'cleaning_page' not in st.session_state:
                st.session_state.cleaning_page = 1
            if 'full_scan_result' not in st.session_state:
                st.session_state.full_scan_result = None
            if 'full_scan_page' not in st.session_state:
                st.session_state.full_scan_page = 1
            
            # ===== 全量扫描模式 =====
            if "全量扫描" in scan_mode:
                if st.button("🚀 开始全量扫描", type="primary"):
                    st.session_state.full_scan_result = None
                    st.session_state.full_scan_page = 1
                    
                    # 进度显示组件
                    progress_bar = st.progress(0, text="🔍 正在准备扫描...")
                    progress_text = st.empty()
                    progress_cols = st.columns(4)
                    metric_processed = progress_cols[0].empty()
                    metric_total = progress_cols[1].empty()
                    metric_time = progress_cols[2].empty()
                    metric_speed = progress_cols[3].empty()
                    
                    # 进度回调函数（漏斗筛选版，带状态消息）
                    def update_progress(processed, total, elapsed, message=None):
                        pct = processed / total if total > 0 else 0
                        speed = processed / elapsed if elapsed > 0 else 0
                        remaining = (total - processed) / speed if speed > 0 else 0
                        
                        # 显示当前阶段信息
                        msg = message if message else f"正在扫描... {pct:.1%}"
                        progress_bar.progress(pct, text=f"🔍 {msg}")
                        progress_text.text(f"⏳ 预计剩余时间: {remaining:.1f}秒")
                        metric_processed.metric("✅ 已处理", f"{processed:,}")
                        metric_total.metric("📊 总计", f"{total:,}")
                        metric_time.metric("⏱️ 已用时", f"{elapsed:.1f}秒")
                        metric_speed.metric("⚡ 速度", f"{speed:.0f}条/秒")
                    
                    # 执行全量扫描
                    if "完全重复" in duplicate_type:
                        result = handlers['cleaning'].scan_full_duplicates_all(
                            batch_size=100,
                            progress_callback=update_progress
                        )
                    else:
                        result = handlers['cleaning'].scan_high_suspect_all(
                            batch_size=100,
                            progress_callback=update_progress
                        )
                    
                    if result['success']:
                        progress_bar.progress(1.0, text=f"✅ 扫描完成！共发现 {result['total']} 组问题数据")
                        progress_text.text(f"🎉 总用时: {result.get('elapsed', 0):.1f}秒")
                        st.session_state.full_scan_result = result
                    else:
                        progress_bar.progress(0, text="❌ 扫描失败")
                        st.error(f"扫描失败: {result.get('error', '未知错误')}")
                
                # 显示全量扫描结果
                if st.session_state.full_scan_result and st.session_state.full_scan_result['success']:
                    result = st.session_state.full_scan_result
                    if result['data']:
                        st.success(f"📊 共发现 {result['total']} 组重复数据")
                        
                        # ===== 一键批量删除功能 =====
                        st.markdown("---")
                        st.subheader("⚡ 一键批量删除")
                        
                        # 计算待删除数量
                        total_duplicates = sum(
                            max(0, g.get('duplicate_count', 0) - 1) 
                            for g in result['data']
                        )
                        total_keep = len(result['data'])
                        
                        col_info1, col_info2, col_info3 = st.columns(3)
                        with col_info1:
                            st.metric("重复组数", f"{result['total']}")
                        with col_info2:
                            st.metric("待删除记录", f"{total_duplicates:,}")
                        with col_info3:
                            st.metric("将保留记录", f"{total_keep:,}")
                        
                        col_opt1, col_opt2 = st.columns([2, 1])
                        with col_opt1:
                            keep_strategy = st.selectbox(
                                "保留策略",
                                ["first", "last"],
                                format_func=lambda x: "保留第一条（最早）" if x == "first" else "保留最后一条（最新）",
                                key="batch_keep_strategy"
                            )
                        with col_opt2:
                            st.warning(f"⚠️ 将删除 **{total_duplicates:,}** 条记录")
                        
                        # 初始化确认状态
                        if 'batch_delete_confirmed' not in st.session_state:
                            st.session_state.batch_delete_confirmed = False
                        
                        # 二次确认机制
                        if not st.session_state.batch_delete_confirmed:
                            if st.button("🗑️ 一键删除全部重复数据", type="primary", width="stretch"):
                                st.session_state.batch_delete_confirmed = True
                                st.rerun()
                        else:
                            st.error(f"⚠️ **确认删除 {total_duplicates:,} 条重复记录？此操作为软删除，可在「已删除数据管理」中恢复。**")
                            
                            col_confirm1, col_confirm2 = st.columns(2)
                            with col_confirm1:
                                if st.button("✅ 确认删除", type="primary", width="stretch"):
                                    # 按运营商分批删除
                                    import time
                                    start_time = time.time()
                                    
                                    progress_bar = st.progress(0, text="准备中...")
                                    status_text = st.empty()
                                    
                                    def update_progress(current, total, message):
                                        pct = current / total if total > 0 else 0
                                        progress_bar.progress(pct, text=f"⚡ {message}")
                                        status_text.text(f"进度: {current}/{total} 个运营商")
                                    
                                    delete_result = handlers['cleaning'].delete_all_duplicates_fast(
                                        keep_strategy=keep_strategy,
                                        operator=current_user,
                                        progress_callback=update_progress
                                    )
                                    
                                    elapsed = time.time() - start_time
                                    progress_bar.progress(1.0, text="✅ 完成")
                                    
                                    if delete_result['success']:
                                        st.success(
                                            f"✅ 批量删除完成！耗时 {elapsed:.1f} 秒\n"
                                            f"- 删除记录: {delete_result.get('affected_rows', 0):,} 条\n"
                                            f"- 处理运营商: {delete_result.get('operators_processed', 0)} 个"
                                        )
                                        
                                        # 记录审计日志
                                        audit_log(
                                            username=current_user,
                                            action_type='batch_delete_duplicates',
                                            action_desc=f'批量删除重复数据: {delete_result.get("affected_rows", 0)}条',
                                            module='data_cleaning',
                                            target_table='evdata',
                                            affected_rows=delete_result.get('affected_rows', 0),
                                            request_params={
                                                'keep_strategy': keep_strategy,
                                                'operators_processed': delete_result.get('operators_processed', 0),
                                                'elapsed_seconds': round(elapsed, 2),
                                                'method': 'batch_by_operator'
                                            }
                                        )
                                        
                                        # 清除扫描结果，需要重新扫描
                                        st.session_state.full_scan_result = None
                                        st.session_state.batch_delete_confirmed = False
                                    else:
                                        st.error(f"❌ 批量删除失败: {delete_result.get('error', '未知错误')}")
                                    
                                    st.session_state.batch_delete_confirmed = False
                                    
                            with col_confirm2:
                                if st.button("❌ 取消", width="stretch"):
                                    st.session_state.batch_delete_confirmed = False
                                    st.rerun()
                        
                        st.markdown("---")
                        
                        # 分页显示结果
                        page_size = 20
                        total_pages = (len(result['data']) + page_size - 1) // page_size
                        current_page = st.session_state.full_scan_page
                        
                        start_idx = (current_page - 1) * page_size
                        end_idx = start_idx + page_size
                        page_data = result['data'][start_idx:end_idx]
                        
                        for idx, group in enumerate(page_data):
                            global_idx = start_idx + idx
                            with st.expander(
                                f"📋 第{global_idx+1}组: {group.get('充电桩编号', 'N/A')} - "
                                f"{group.get('运营商名称', 'N/A')} "
                                f"({group.get('duplicate_count', 0)}条重复)",
                                expanded=(idx == 0)
                            ):
                                uid_list = group.get('uid_list', '')
                                records = handlers['cleaning'].get_duplicate_records_by_uids(uid_list)
                                
                                if records:
                                    df = pd.DataFrame(records)
                                    display_cols = ['UID', '充电桩编号', '运营商名称', '省份_中文', 
                                                  '城市_中文', '区县_中文', '充电站位置', '额定功率']
                                    display_cols = [c for c in display_cols if c in df.columns]
                                    st.dataframe(df[display_cols], width="stretch")
                                    
                                    st.markdown("**🔧 处理操作**")
                                    uids = [r['UID'] for r in records]
                                    keep_uid = st.selectbox(
                                        "选择要保留的记录（其他将被软删除）",
                                        uids,
                                        key=f"full_keep_{global_idx}"
                                    )
                                    
                                    col_a1, col_a2 = st.columns(2)
                                    with col_a1:
                                        if st.button("🗑️ 软删除其他", key=f"full_del_{global_idx}", type="primary"):
                                            delete_uids = [u for u in uids if u != keep_uid]
                                            if delete_uids:
                                                del_result = handlers['cleaning'].batch_soft_delete(
                                                    delete_uids, f"完全重复，保留UID:{keep_uid}", current_user
                                                )
                                                if del_result['success']:
                                                    st.success(f"✅ 已软删除 {del_result['affected_rows']} 条记录")
                                                    audit_log(username=current_user, action_type='data_cleaning',
                                                             action_desc='软删除重复数据', module='data_cleaning',
                                                             target_table='evdata', affected_rows=del_result['affected_rows'])
                                                else:
                                                    st.error(f"❌ 操作失败: {del_result.get('error')}")
                                    with col_a2:
                                        if st.button("✅ 标记正常", key=f"full_normal_{global_idx}"):
                                            for uid in uids:
                                                handlers['cleaning'].mark_as_normal(uid, current_user)
                                            st.success("✅ 已标记为正常")
                        
                        # 分页控制
                        if total_pages > 1:
                            st.markdown("---")
                            col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                            with col_p1:
                                if st.button("⬅️ 上一页", key="full_prev") and current_page > 1:
                                    st.session_state.full_scan_page -= 1
                                    st.rerun()
                            with col_p2:
                                st.write(f"第 {current_page} / {total_pages} 页（共 {result['total']} 组）")
                            with col_p3:
                                if st.button("➡️ 下一页", key="full_next") and current_page < total_pages:
                                    st.session_state.full_scan_page += 1
                                    st.rerun()
                    else:
                        st.success("✅ 未发现重复数据！")
            
            # ===== 分页浏览模式 =====
            else:
                if st.button("🔍 开始扫描", type="primary"):
                    st.session_state.cleaning_page = 1
                    st.session_state.scan_triggered = True
                
                if st.session_state.get('scan_triggered', False):
                    with st.spinner("正在扫描..."):
                        if "完全重复" in duplicate_type:
                            result = handlers['cleaning'].scan_full_duplicates(
                                page=st.session_state.cleaning_page, 
                                page_size=20
                            )
                        else:
                            result = handlers['cleaning'].scan_high_suspect_duplicates(
                                page=st.session_state.cleaning_page, 
                                page_size=20
                            )
                    
                    if result['success'] and result['data']:
                        st.info(f"📊 发现 {result['total']} 组重复数据")
                        
                        # 显示重复数据列表
                        for idx, group in enumerate(result['data']):
                            with st.expander(
                                f"📋 第{idx+1}组: {group.get('充电桩编号', 'N/A')} - "
                                f"{group.get('运营商名称', 'N/A')} "
                                f"({group.get('duplicate_count', 0)}条重复)",
                                expanded=(idx == 0)
                            ):
                                # 获取该组的详细记录
                                uid_list = group.get('uid_list', '')
                                records = handlers['cleaning'].get_duplicate_records_by_uids(uid_list)
                                
                                if records:
                                    # 显示对比表格
                                    df = pd.DataFrame(records)
                                    display_cols = ['UID', '充电桩编号', '运营商名称', '省份_中文', 
                                                  '城市_中文', '区县_中文', '充电站位置', '额定功率']
                                    display_cols = [c for c in display_cols if c in df.columns]
                                    st.dataframe(df[display_cols], width="stretch")
                                    
                                    # 处理操作
                                    st.markdown("**🔧 处理操作**")
                                    
                                    # 选择要保留的记录
                                    uids = [r['UID'] for r in records]
                                    keep_uid = st.selectbox(
                                        "选择要保留的记录（其他将被软删除）",
                                        uids,
                                        key=f"keep_{idx}"
                                    )
                                    
                                    col_action1, col_action2, col_action3 = st.columns(3)
                                    
                                    with col_action1:
                                        if st.button("🗑️ 软删除其他", key=f"delete_{idx}", type="primary"):
                                            delete_uids = [u for u in uids if u != keep_uid]
                                            if delete_uids:
                                                del_result = handlers['cleaning'].batch_soft_delete(
                                                    delete_uids, 
                                                    f"完全重复，保留UID:{keep_uid}", 
                                                    current_user
                                                )
                                                if del_result['success']:
                                                    st.success(f"✅ 已软删除 {del_result['affected_rows']} 条记录")
                                                    audit_log(
                                                        username=current_user,
                                                        action_type='data_cleaning',
                                                        action_desc=f'软删除重复数据',
                                                        module='data_cleaning',
                                                        target_table='evdata',
                                                        affected_rows=del_result['affected_rows']
                                                    )
                                                    st.rerun()
                                                else:
                                                    st.error(f"❌ 操作失败: {del_result.get('error')}")
                                    
                                    with col_action2:
                                        if st.button("✅ 标记为正常", key=f"normal_{idx}"):
                                            for uid in uids:
                                                handlers['cleaning'].mark_as_normal(uid, current_user)
                                            st.success("✅ 已标记为正常")
                                            st.rerun()
                                    
                                    with col_action3:
                                        if st.button("⏭️ 跳过", key=f"skip_{idx}"):
                                            st.info("已跳过")
                        
                        # 分页控制
                        total_pages = (result['total'] + 19) // 20
                        if total_pages > 1:
                            col_page1, col_page2, col_page3 = st.columns([1, 2, 1])
                            with col_page1:
                                if st.button("⬅️ 上一页") and st.session_state.cleaning_page > 1:
                                    st.session_state.cleaning_page -= 1
                                    st.rerun()
                            with col_page2:
                                st.write(f"第 {st.session_state.cleaning_page} / {total_pages} 页")
                            with col_page3:
                                if st.button("➡️ 下一页") and st.session_state.cleaning_page < total_pages:
                                    st.session_state.cleaning_page += 1
                                    st.rerun()
                    
                    elif result['success'] and not result['data']:
                        st.success("✅ 未发现重复数据！")
                    else:
                        st.error(f"❌ 扫描失败: {result.get('error', '未知错误')}")
        
        # ========== 地址一致性校验 ==========
        elif cleaning_mode == "地址一致性校验":
            st.subheader("📍 地址一致性校验")
            st.caption("检测省份/城市/区县与场站地址不一致的记录，以场站地址为准进行修正")

            # 概览统计（快速检查数据库整体情况）
            st.markdown("### 📊 地址匹配概览")
            if 'addr_summary' not in st.session_state:
                st.session_state.addr_summary = None

            col_sum_btn, col_sum_note = st.columns([1, 3])
            with col_sum_btn:
                if st.button("🔄 刷新统计", key="addr_summary_refresh", width="stretch"):
                    st.session_state.addr_summary = None  # 强制刷新
            with col_sum_note:
                st.caption("统计口径：只统计 `is_active=1` 且 `充电站位置` 非空的记录；不匹配规则与下方扫描一致。")

            if st.session_state.addr_summary is None:
                with st.spinner("正在统计地址匹配情况..."):
                    st.session_state.addr_summary = handlers['cleaning'].get_address_consistency_summary(top_n=10)

            summary = st.session_state.addr_summary or {}
            if summary.get('success'):
                col_a, col_b, col_c, col_d = st.columns(4)
                col_a.metric("活跃总记录", f"{summary.get('total_active', 0):,}")
                col_b.metric("有地址记录", f"{summary.get('total_with_address', 0):,}")
                col_c.metric("地址为空", f"{summary.get('total_address_empty', 0):,}")
                col_d.metric("不匹配总数", f"{summary.get('total_mismatch', 0):,}",
                             delta=f"{summary.get('mismatch_rate', 0.0) * 100:.2f}%")

                col_e, col_f = st.columns(2)
                with col_e:
                    st.markdown("**Top 省份（不匹配）**")
                    df_tp = pd.DataFrame(summary.get('top_provinces', []))
                    if not df_tp.empty:
                        df_tp.columns = ['省份', '不匹配数']
                        st.dataframe(df_tp, width="stretch", hide_index=True)
                    else:
                        st.info("暂无数据")
                with col_f:
                    st.markdown("**Top 城市（不匹配）**")
                    df_tc = pd.DataFrame(summary.get('top_cities', []))
                    if not df_tc.empty:
                        df_tc.columns = ['城市', '不匹配数']
                        st.dataframe(df_tc, width="stretch", hide_index=True)
                    else:
                        st.info("暂无数据")
            else:
                st.warning(f"概览统计失败: {summary.get('error', '未知错误')}")
            
            addr_scan_mode = st.selectbox(
                "扫描模式",
                ["分页浏览（快速）", "全量扫描（带进度）"],
                key="addr_scan_mode",
                help="分页浏览：每次显示10条；全量扫描：一次性扫描所有数据，显示实时进度"
            )
            
            if 'address_page' not in st.session_state:
                st.session_state.address_page = 1
            if 'addr_full_scan_result' not in st.session_state:
                st.session_state.addr_full_scan_result = None
            if 'addr_full_scan_page' not in st.session_state:
                st.session_state.addr_full_scan_page = 1
            
            # ===== 全量扫描模式 =====
            if "全量扫描" in addr_scan_mode:
                if st.button("🚀 开始全量检测", type="primary", key="addr_full_scan"):
                    st.session_state.addr_full_scan_result = None
                    st.session_state.addr_full_scan_page = 1
                    
                    # 进度显示组件
                    progress_bar = st.progress(0, text="🔍 正在准备检测...")
                    progress_text = st.empty()
                    progress_cols = st.columns(4)
                    metric_processed = progress_cols[0].empty()
                    metric_total = progress_cols[1].empty()
                    metric_time = progress_cols[2].empty()
                    metric_speed = progress_cols[3].empty()
                    
                    def update_addr_progress(processed, total, elapsed, message=None):
                        pct = processed / total if total > 0 else 0
                        speed = processed / elapsed if elapsed > 0 else 0
                        remaining = (total - processed) / speed if speed > 0 else 0
                        
                        msg = message if message else f"正在检测... {pct:.1%}"
                        progress_bar.progress(pct, text=f"🔍 {msg}")
                        progress_text.text(f"⏳ 预计剩余时间: {remaining:.1f}秒")
                        metric_processed.metric("✅ 已处理", f"{processed:,}")
                        metric_total.metric("📊 总计", f"{total:,}")
                        metric_time.metric("⏱️ 已用时", f"{elapsed:.1f}秒")
                        metric_speed.metric("⚡ 速度", f"{speed:.0f}条/秒")
                    
                    result = handlers['cleaning'].scan_address_issues_all(
                        batch_size=100,
                        progress_callback=update_addr_progress
                    )
                    
                    if result['success']:
                        progress_bar.progress(1.0, text=f"✅ 检测完成！共发现 {result['total']} 条问题数据")
                        progress_text.text(f"🎉 总用时: {result.get('elapsed', 0):.1f}秒")
                        st.session_state.addr_full_scan_result = result
                    else:
                        progress_bar.progress(0, text="❌ 检测失败")
                        st.error(f"检测失败: {result.get('error', '未知错误')}")
                
                # 显示全量检测结果
                if st.session_state.addr_full_scan_result and st.session_state.addr_full_scan_result['success']:
                    result = st.session_state.addr_full_scan_result
                    if result['data']:
                        st.success(f"📊 共发现 {result['total']} 条地址不一致的记录")
                        
                        page_size = 10
                        total_pages = (len(result['data']) + page_size - 1) // page_size
                        current_page = st.session_state.addr_full_scan_page
                        
                        start_idx = (current_page - 1) * page_size
                        end_idx = start_idx + page_size
                        page_data = result['data'][start_idx:end_idx]
                        
                        for idx, record in enumerate(page_data):
                            global_idx = start_idx + idx
                            with st.expander(
                                f"📋 记录 {global_idx+1}: {record.get('充电桩编号', 'N/A')} - {record.get('运营商名称', 'N/A')}",
                                expanded=(idx == 0)
                            ):
                                col_info1, col_info2 = st.columns(2)
                                with col_info1:
                                    st.markdown("**📍 当前信息**")
                                    st.write(f"省份: {record.get('省份_中文', '-')}")
                                    st.write(f"城市: {record.get('城市_中文', '-')}")
                                    st.write(f"区县: {record.get('区县_中文', '-')}")
                                with col_info2:
                                    st.markdown("**🔍 解析建议**")
                                    st.write(f"解析省份: {record.get('parsed_province', '-')}")
                                    st.write(f"解析城市: {record.get('parsed_city', '-')}")
                                    st.write(f"解析区县: {record.get('parsed_district', '-')}")
                                    st.write(f"置信度: {record.get('parse_confidence', 0):.0%}")
                                
                                st.markdown("**📍 场站地址**")
                                st.info(record.get('充电站位置', '-'))
                                
                                col_fix1, col_fix2, col_fix3 = st.columns(3)
                                with col_fix1:
                                    new_province = st.text_input("修正省份", 
                                        value=record.get('parsed_province') or record.get('省份_中文') or '',
                                        key=f"addr_full_prov_{global_idx}")
                                with col_fix2:
                                    new_city = st.text_input("修正城市",
                                        value=record.get('parsed_city') or record.get('城市_中文') or '',
                                        key=f"addr_full_city_{global_idx}")
                                with col_fix3:
                                    new_district = st.text_input("修正区县",
                                        value=record.get('parsed_district') or record.get('区县_中文') or '',
                                        key=f"addr_full_dist_{global_idx}")
                                
                                col_b1, col_b2 = st.columns(2)
                                with col_b1:
                                    if st.button("✅ 确认修正", key=f"addr_full_fix_{global_idx}", type="primary"):
                                        fix_result = handlers['cleaning'].fix_address(
                                            record['UID'], new_province, new_city, new_district,
                                            current_user, f"地址修正: {record.get('充电站位置', '')[:50]}"
                                        )
                                        if fix_result['success']:
                                            st.success("✅ 地址已修正")
                                            audit_log(username=current_user, action_type='data_cleaning',
                                                     action_desc=f'修正地址: {record.get("充电桩编号")}',
                                                     module='data_cleaning', target_table='evdata', affected_rows=1)
                                        else:
                                            st.error(f"❌ 修正失败: {fix_result.get('error')}")
                                with col_b2:
                                    if st.button("✅ 标记正常", key=f"addr_full_normal_{global_idx}"):
                                        handlers['cleaning'].mark_as_normal(record['UID'], current_user)
                                        st.success("✅ 已标记为正常")
                        
                        # 分页控制
                        if total_pages > 1:
                            st.markdown("---")
                            col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                            with col_p1:
                                if st.button("⬅️ 上一页", key="addr_full_prev") and current_page > 1:
                                    st.session_state.addr_full_scan_page -= 1
                                    st.rerun()
                            with col_p2:
                                st.write(f"第 {current_page} / {total_pages} 页（共 {result['total']} 条）")
                            with col_p3:
                                if st.button("➡️ 下一页", key="addr_full_next") and current_page < total_pages:
                                    st.session_state.addr_full_scan_page += 1
                                    st.rerun()
                    else:
                        st.success("✅ 未发现地址不一致的数据！")
            
            # ===== 分页浏览模式 =====
            else:
                if st.button("🔍 开始检测", type="primary", key="scan_address"):
                    st.session_state.address_page = 1
                    st.session_state.address_scan_triggered = True
                
                if st.session_state.get('address_scan_triggered', False):
                    with st.spinner("正在检测地址不一致的数据..."):
                        addr_result = handlers['cleaning'].scan_address_issues(
                            page=st.session_state.address_page,
                            page_size=10
                        )
                    
                    if not addr_result['success']:
                        st.error(f"❌ 检测失败: {addr_result.get('error', '未知错误')}")
                    elif not addr_result['data']:
                        st.success("✅ 未发现地址不一致的数据！")
                    else:
                        st.info(f"📊 发现 {addr_result['total']} 条地址不一致的记录")
                    
                        for idx, record in enumerate(addr_result['data']):
                            with st.expander(
                                f"📋 记录 {idx+1}: {record.get('充电桩编号', 'N/A')} - {record.get('运营商名称', 'N/A')}",
                                expanded=(idx == 0)
                            ):
                                col_info1, col_info2 = st.columns(2)
                                
                                with col_info1:
                                    st.markdown("**📍 当前信息**")
                                    st.write(f"省份: {record.get('省份_中文', '-')}")
                                    st.write(f"城市: {record.get('城市_中文', '-')}")
                                    st.write(f"区县: {record.get('区县_中文', '-')}")
                                
                                with col_info2:
                                    st.markdown("**🔍 解析建议**")
                                    confidence = record.get('parse_confidence', 0)
                                    st.write(f"解析省份: {record.get('parsed_province', '-')}")
                                    st.write(f"解析城市: {record.get('parsed_city', '-')}")
                                    st.write(f"解析区县: {record.get('parsed_district', '-')}")
                                    st.write(f"置信度: {confidence:.0%}")
                                
                                st.markdown("**📍 场站地址**")
                                st.info(record.get('充电站位置', '-'))
                                
                                st.markdown("**🔧 处理操作**")
                                
                                # 手动修正输入
                                col_fix1, col_fix2, col_fix3 = st.columns(3)
                                with col_fix1:
                                    new_province = st.text_input(
                                        "修正省份", 
                                        value=record.get('parsed_province') or record.get('省份_中文') or '',
                                        key=f"prov_{idx}"
                                    )
                                with col_fix2:
                                    new_city = st.text_input(
                                        "修正城市",
                                        value=record.get('parsed_city') or record.get('城市_中文') or '',
                                        key=f"city_{idx}"
                                    )
                                with col_fix3:
                                    new_district = st.text_input(
                                        "修正区县",
                                        value=record.get('parsed_district') or record.get('区县_中文') or '',
                                        key=f"dist_{idx}"
                                    )
                                
                                col_btn1, col_btn2, col_btn3 = st.columns(3)
                                
                                with col_btn1:
                                    if st.button("✅ 确认修正", key=f"fix_{idx}", type="primary"):
                                        fix_result = handlers['cleaning'].fix_address(
                                            record['UID'],
                                            new_province,
                                            new_city,
                                            new_district,
                                            current_user,
                                            f"地址修正: {record.get('充电站位置', '')[:50]}"
                                        )
                                        if fix_result['success']:
                                            st.success("✅ 地址已修正")
                                            audit_log(
                                                username=current_user,
                                                action_type='data_cleaning',
                                                action_desc=f'修正地址: {record.get("充电桩编号")}',
                                                module='data_cleaning',
                                                target_table='evdata',
                                                affected_rows=1
                                            )
                                            st.rerun()
                                        else:
                                            st.error(f"❌ 修正失败: {fix_result.get('error')}")
                                
                                with col_btn2:
                                    if st.button("✅ 标记正常", key=f"addr_normal_{idx}"):
                                        handlers['cleaning'].mark_as_normal(record['UID'], current_user)
                                        st.success("✅ 已标记为正常")
                                        st.rerun()
                                
                                with col_btn3:
                                    if st.button("⏭️ 跳过", key=f"addr_skip_{idx}"):
                                        st.info("已跳过")
                        
                        # 分页
                        total_pages = (addr_result['total'] + 9) // 10
                        if total_pages > 1:
                            col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                            with col_p1:
                                if st.button("⬅️ 上一页", key="addr_prev") and st.session_state.address_page > 1:
                                    st.session_state.address_page -= 1
                                    st.rerun()
                            with col_p2:
                                st.write(f"第 {st.session_state.address_page} / {total_pages} 页")
                            with col_p3:
                                if st.button("➡️ 下一页", key="addr_next") and st.session_state.address_page < total_pages:
                                    st.session_state.address_page += 1
                                    st.rerun()
        
        # ========== 已删除数据管理 ==========
        elif cleaning_mode == "已删除数据管理":
            st.subheader("🗑️ 已删除数据管理")
            st.caption("查看和恢复被软删除的数据")
            
            if 'deleted_page' not in st.session_state:
                st.session_state.deleted_page = 1
            
            # 获取已删除数据
            result = handlers['cleaning'].get_deleted_records(
                page=st.session_state.deleted_page,
                page_size=20
            )
            
            if result['success']:
                if result['data']:
                    st.info(f"📊 共 {result['total']} 条已删除数据")
                    
                    # 显示数据表格
                    df = pd.DataFrame(result['data'])
                    
                    # 格式化时间列
                    if 'cleaned_at' in df.columns:
                        df['cleaned_at'] = pd.to_datetime(df['cleaned_at']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    # 添加选择列
                    selected_uids = []
                    
                    for idx, row in df.iterrows():
                        col_sel, col_info = st.columns([1, 9])
                        with col_sel:
                            if st.checkbox("选择", key=f"sel_del_{row['UID']}", label_visibility="collapsed"):
                                selected_uids.append(row['UID'])
                        with col_info:
                            st.write(
                                f"**{row.get('充电桩编号', 'N/A')}** | "
                                f"{row.get('运营商名称', '-')} | "
                                f"{row.get('省份_中文', '-')} {row.get('城市_中文', '-')} | "
                                f"删除时间: {row.get('cleaned_at', '-')} | "
                                f"操作人: {row.get('cleaned_by', '-')}"
                            )
                            if row.get('status_note'):
                                st.caption(f"备注: {row.get('status_note')}")
                    
                    # 恢复按钮
                    st.markdown("---")
                    col_restore1, col_restore2 = st.columns(2)
                    
                    with col_restore1:
                        if st.button("🔄 恢复选中数据", type="primary", disabled=len(selected_uids) == 0):
                            restored_count = 0
                            for uid in selected_uids:
                                result = handlers['cleaning'].restore_deleted(uid, current_user)
                                if result['success']:
                                    restored_count += 1
                            
                            if restored_count > 0:
                                st.success(f"✅ 已恢复 {restored_count} 条数据")
                                audit_log(
                                    username=current_user,
                                    action_type='data_cleaning',
                                    action_desc=f'恢复已删除数据',
                                    module='data_cleaning',
                                    target_table='evdata',
                                    affected_rows=restored_count
                                )
                                st.rerun()
                    
                    with col_restore2:
                        st.write(f"已选择 {len(selected_uids)} 条记录")
                    
                    # 分页
                    total_pages = (result['total'] + 19) // 20
                    if total_pages > 1:
                        col_dp1, col_dp2, col_dp3 = st.columns([1, 2, 1])
                        with col_dp1:
                            if st.button("⬅️ 上一页", key="del_prev") and st.session_state.deleted_page > 1:
                                st.session_state.deleted_page -= 1
                                st.rerun()
                        with col_dp2:
                            st.write(f"第 {st.session_state.deleted_page} / {total_pages} 页")
                        with col_dp3:
                            if st.button("➡️ 下一页", key="del_next") and st.session_state.deleted_page < total_pages:
                                st.session_state.deleted_page += 1
                                st.rerun()
                else:
                    st.success("✅ 没有已删除的数据")
            else:
                st.error(f"❌ 获取数据失败: {result.get('error', '未知错误')}")

# ========== 标签页7: 区域编码清洗 ==========
with tab7:
    st.header("🗺️ 区域编码清洗")
    st.caption("修复省/市/区县编码不规范或隶属关系不一致的问题")
    
    # 权限检查：管理员和操作员可使用
    if not PermissionChecker.can_clean_data():
        st.warning("⚠️ 区域编码清洗功能仅限管理员和操作员使用")
        st.info("如需清洗数据，请联系管理员或使用操作员账号登录。")
    else:
        # 初始化 RegionCleaningHandler
        if 'region_handler' not in st.session_state:
            st.session_state.region_handler = RegionCleaningHandler(st.session_state.selected_table)
        
        region_handler = st.session_state.region_handler
        
        # 清洗模式选择
        region_cleaning_mode = st.radio(
            "选择清洗类型",
            ["城市编码修复", "区域名称同步", "区县编码修复", "修复历史记录"],
            horizontal=True,
            key="region_cleaning_mode"
        )
        
        # ========== 城市编码修复 ==========
        if region_cleaning_mode == "城市编码修复":
            st.subheader("🏙️ 城市编码修复")
            st.caption("修复城市代码最后两位非00的异常记录（如将区县代码误存为城市代码）")
            
            # 扫描按钮
            col_scan1, col_scan2 = st.columns([1, 3])
            with col_scan1:
                scan_city_btn = st.button("🔍 扫描城市编码问题", type="primary", key="scan_city")
            
            if scan_city_btn or 'city_scan_result' in st.session_state:
                if scan_city_btn:
                    with st.spinner("正在扫描城市编码异常..."):
                        try:
                            result = region_handler.scan_city_code_issues()
                            st.session_state.city_scan_result = result
                        except Exception as e:
                            st.error(f"扫描失败: {e}")
                            st.session_state.city_scan_result = None
                
                if 'city_scan_result' in st.session_state and st.session_state.city_scan_result:
                    result = st.session_state.city_scan_result
                    
                    # 统计卡片
                    st.markdown("### 📊 问题概览")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("总记录数", f"{result['total_records']:,}")
                    with col2:
                        st.metric("异常记录", f"{result['abnormal_count']:,}", 
                                 delta=f"-{result['abnormal_percent']}%", delta_color="inverse")
                    with col3:
                        auto_fix = result['by_strategy'].get('direct_city', 0) + \
                                  result['by_strategy'].get('from_district', 0) + \
                                  result['by_strategy'].get('from_current', 0)
                        auto_percent = round(auto_fix * 100 / result['abnormal_count'], 1) if result['abnormal_count'] else 0
                        st.metric("可自动修复", f"{auto_fix:,}", delta=f"{auto_percent}%")
                    with col4:
                        st.metric("待人工处理", f"{result['by_strategy'].get('manual_required', 0):,}")
                    
                    # 按省份分布
                    if result['by_province']:
                        st.markdown("### 📍 按省份分布 (Top 15)")
                        import pandas as pd
                        df_province = pd.DataFrame(result['by_province'])
                        df_province.columns = ['省份', '异常记录数']
                        st.dataframe(df_province, width="stretch", hide_index=True)
                    
                    # 修复策略分布
                    st.markdown("### 🔧 修复策略分布")
                    strategy_names = {
                        'direct_city': '直辖市规则',
                        'from_district': '从区县推导',
                        'from_current': '从当前城市推导',
                        'manual_required': '待人工处理'
                    }
                    strategy_data = [
                        {'策略': strategy_names.get(k, k), '记录数': v}
                        for k, v in result['by_strategy'].items()
                    ]
                    df_strategy = pd.DataFrame(strategy_data)
                    st.dataframe(df_strategy, width="stretch", hide_index=True)
                    
                    # 修复预览
                    st.markdown("### 👁️ 修复预览")
                    
                    # 初始化页码
                    if 'city_fix_page' not in st.session_state:
                        st.session_state.city_fix_page = 1
                    
                    try:
                        preview = region_handler.get_city_fix_preview(
                            page=st.session_state.city_fix_page, 
                            page_size=20
                        )
                        
                        if preview['data']:
                            # 转换为DataFrame显示
                            preview_df = pd.DataFrame([
                                {
                                    '省份': r['province_name'],
                                    '当前城市代码': r['city_code'],
                                    '当前城市名': r['city_name'],
                                    '→': '→',
                                    '修复后代码': r['new_city_code'] or '-',
                                    '修复后名称': r['new_city_name'] or '-',
                                    '策略': r['strategy'],
                                    '置信度': f"{r['confidence']*100:.0f}%"
                                }
                                for r in preview['data']
                            ])
                            st.dataframe(preview_df, width="stretch", hide_index=True)
                            
                            # 分页控制
                            col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
                            with col_p1:
                                if st.button("⬅️ 上一页", key="city_prev") and st.session_state.city_fix_page > 1:
                                    st.session_state.city_fix_page -= 1
                                    st.rerun()
                            with col_p2:
                                st.write(f"第 {preview['page']} / {preview['total_pages']} 页 "
                                        f"(共 {preview['total']:,} 条)")
                            with col_p3:
                                if st.button("➡️ 下一页", key="city_next") and st.session_state.city_fix_page < preview['total_pages']:
                                    st.session_state.city_fix_page += 1
                                    st.rerun()
                    except Exception as e:
                        st.error(f"获取预览失败: {e}")
                    
                    # 执行修复
                    st.markdown("---")
                    st.markdown("### 🚀 执行修复")
                    
                    col_fix1, col_fix2 = st.columns(2)
                    with col_fix1:
                        dry_run_btn = st.button("📋 预览修复方案 (Dry Run)", key="city_dry_run")
                    with col_fix2:
                        if PermissionChecker.is_admin():
                            execute_btn = st.button("🚀 执行修复", type="primary", key="city_execute")
                        else:
                            st.info("仅管理员可执行修复操作")
                            execute_btn = False
                    
                    if dry_run_btn:
                        with st.spinner("正在分析修复方案..."):
                            fix_result = region_handler.fix_city_codes(dry_run=True)
                            st.success(f"✅ 预览完成")
                            st.json({
                                '待修复总数': fix_result['total'],
                                '可修复': fix_result['success'],
                                '跳过(待人工)': fix_result['skip'],
                                '策略分布': fix_result['by_strategy']
                            })
                    
                    # 使用 session_state 跟踪确认状态
                    if 'confirm_city_fix' not in st.session_state:
                        st.session_state.confirm_city_fix = False
                    
                    if execute_btn:
                        st.session_state.confirm_city_fix = True
                    
                    if st.session_state.confirm_city_fix:
                        st.warning("⚠️ 即将修复城市编码，此操作会修改数据库。修复前会自动创建备份，支持回滚。")
                        
                        confirm_col1, confirm_col2 = st.columns(2)
                        with confirm_col1:
                            confirm_btn = st.button("✅ 确认执行", key="confirm_city_fix_btn", type="primary")
                        with confirm_col2:
                            cancel_btn = st.button("❌ 取消", key="cancel_city_fix_btn")
                        
                        if cancel_btn:
                            st.session_state.confirm_city_fix = False
                            st.rerun()
                        
                        if confirm_btn:
                            # 创建进度显示区域
                            progress_container = st.container()
                            with progress_container:
                                progress_bar = st.progress(0)
                                col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                                with col_stat1:
                                    metric_processed = st.empty()
                                with col_stat2:
                                    metric_remaining = st.empty()
                                with col_stat3:
                                    metric_elapsed = st.empty()
                                with col_stat4:
                                    metric_eta = st.empty()
                                status_text = st.empty()
                            
                            def update_progress(info):
                                progress_bar.progress(info['percent'] / 100)
                                metric_processed.metric("已处理", f"{info['processed']:,}")
                                metric_remaining.metric("剩余", f"{info['remaining']:,}")
                                elapsed_str = f"{info['elapsed']:.1f}秒" if info['elapsed'] else "-"
                                metric_elapsed.metric("已用时", elapsed_str)
                                eta_str = f"{info['eta']:.0f}秒" if info.get('eta') else "-"
                                metric_eta.metric("预计剩余", eta_str)
                                status_text.text(info['message'])
                            
                            fix_result = region_handler.fix_city_codes(
                                dry_run=False,
                                progress_callback=update_progress,
                                operator=SessionManager.get_username() or 'admin'
                            )
                            
                            st.session_state.confirm_city_fix = False
                            
                            if 'error' not in fix_result:
                                st.success(f"✅ 修复完成！耗时 {fix_result.get('elapsed_seconds', 0):.1f} 秒")
                                
                                col_r1, col_r2, col_r3, col_r4 = st.columns(4)
                                col_r1.metric("总处理", f"{fix_result['total']:,}")
                                col_r2.metric("成功修复", f"{fix_result['success']:,}")
                                col_r3.metric("跳过(待人工)", f"{fix_result['skip']:,}")
                                col_r4.metric("失败", f"{fix_result['fail']:,}")
                                
                                st.info(f"备份ID: {fix_result['backup_id']}")
                                
                                # 显示需要人工处理的记录
                                if fix_result.get('manual_records'):
                                    st.session_state.city_manual_records = fix_result['manual_records']
                                
                                # 清除缓存
                                if 'city_scan_result' in st.session_state:
                                    del st.session_state.city_scan_result
                            else:
                                st.error(f"❌ 修复失败: {fix_result['error']}")
                    
                    # 显示需要人工处理的记录
                    if 'city_manual_records' in st.session_state and st.session_state.city_manual_records:
                        st.markdown("---")
                        st.markdown("### ⚠️ 需要人工处理的记录")
                        st.caption(f"共 {len(st.session_state.city_manual_records)} 条记录无法自动修复")
                        
                        import pandas as pd
                        df_manual = pd.DataFrame(st.session_state.city_manual_records)
                        st.dataframe(df_manual, width="stretch", height=400)
                        
                        # 导出功能
                        csv = df_manual.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            label="📥 导出待处理记录",
                            data=csv,
                            file_name="城市编码待人工处理.csv",
                            mime="text/csv"
                        )
        
        # ========== 区域名称同步 ==========
        elif region_cleaning_mode == "区域名称同步":
            st.subheader("🔄 区域名称同步")
            st.caption("以最新JSON字典为准，同步数据库中的省份/城市/区县中文名称")
            
            col_scan1, col_scan2 = st.columns([1, 3])
            with col_scan1:
                scan_name_btn = st.button("🔍 扫描名称差异", type="primary", key="scan_name_sync")
            
            if scan_name_btn or 'name_sync_result' in st.session_state:
                if scan_name_btn:
                    with st.spinner("正在扫描名称差异..."):
                        try:
                            result = region_handler.scan_name_sync_issues()
                            st.session_state.name_sync_result = result
                        except Exception as e:
                            st.error(f"扫描失败: {e}")
                            st.session_state.name_sync_result = None
                
                if 'name_sync_result' in st.session_state and st.session_state.name_sync_result:
                    result = st.session_state.name_sync_result
                    
                    # 统计卡片
                    st.markdown("### 📊 差异概览")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("省份名称差异", f"{result['province_count']}")
                    with col2:
                        st.metric("城市名称差异", f"{result['city_count']}")
                    with col3:
                        st.metric("区县名称差异", f"{result['district_count']}")
                    with col4:
                        st.metric("总受影响记录", f"{result['total_affected']:,}")
                    
                    # 省份差异详情
                    if result['province_issues']:
                        st.markdown("### 📍 省份名称差异")
                        import pandas as pd
                        df = pd.DataFrame(result['province_issues'][:20])
                        df.columns = ['代码', '数据库名称', 'JSON名称', '记录数']
                        st.dataframe(df, width="stretch", hide_index=True)
                    
                    # 城市差异详情
                    if result['city_issues']:
                        st.markdown("### 🏙️ 城市名称差异")
                        df = pd.DataFrame(result['city_issues'][:30])
                        df.columns = ['代码', '数据库名称', 'JSON名称', '记录数']
                        st.dataframe(df, width="stretch", hide_index=True)
                    
                    # 区县差异详情
                    if result['district_issues']:
                        st.markdown("### 🏘️ 区县名称差异")
                        df = pd.DataFrame(result['district_issues'][:30])
                        df.columns = ['代码', '数据库名称', 'JSON名称', '记录数']
                        st.dataframe(df, width="stretch", hide_index=True)
                    
                    # 执行同步
                    st.markdown("---")
                    st.markdown("### 🚀 执行同步")
                    
                    col_fix1, col_fix2 = st.columns(2)
                    with col_fix1:
                        dry_run_btn = st.button("📋 预览同步方案", key="name_sync_dry_run")
                    with col_fix2:
                        if PermissionChecker.is_admin():
                            execute_btn = st.button("🚀 执行名称同步", type="primary", key="name_sync_execute")
                        else:
                            st.info("仅管理员可执行同步操作")
                            execute_btn = False
                    
                    if dry_run_btn:
                        with st.spinner("正在预览..."):
                            fix_result = region_handler.fix_region_names(dry_run=True)
                            st.success("✅ 预览完成")
                            st.json({
                                '省份修复': fix_result['province_fixed'],
                                '城市修复': fix_result['city_fixed'],
                                '区县修复': fix_result['district_fixed'],
                                '总计': fix_result['total_records']
                            })
                    
                    # 使用 session_state 跟踪确认状态
                    if 'confirm_name_sync' not in st.session_state:
                        st.session_state.confirm_name_sync = False
                    
                    if execute_btn:
                        st.session_state.confirm_name_sync = True
                    
                    if st.session_state.confirm_name_sync:
                        st.warning("⚠️ 即将同步区域名称，以JSON字典为准更新数据库。")
                        
                        col_confirm1, col_confirm2 = st.columns(2)
                        with col_confirm1:
                            confirm_btn = st.button("✅ 确认执行", key="confirm_name_sync_btn", type="primary")
                        with col_confirm2:
                            cancel_btn = st.button("❌ 取消", key="cancel_name_sync_btn")
                        
                        if cancel_btn:
                            st.session_state.confirm_name_sync = False
                            st.rerun()
                        
                        if confirm_btn:
                            # 创建进度显示区域
                            progress_container = st.container()
                            with progress_container:
                                progress_bar = st.progress(0)
                                col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                                with col_stat1:
                                    metric_processed = st.empty()
                                with col_stat2:
                                    metric_remaining = st.empty()
                                with col_stat3:
                                    metric_elapsed = st.empty()
                                with col_stat4:
                                    metric_eta = st.empty()
                                status_text = st.empty()
                            
                            def update_progress(info):
                                progress_bar.progress(info['percent'] / 100)
                                metric_processed.metric("已处理", f"{info['processed']:,}")
                                metric_remaining.metric("剩余", f"{info['remaining']:,}")
                                elapsed_str = f"{info['elapsed']:.1f}秒" if info['elapsed'] else "-"
                                metric_elapsed.metric("已用时", elapsed_str)
                                eta_str = f"{info['eta']:.0f}秒" if info.get('eta') else "-"
                                metric_eta.metric("预计剩余", eta_str)
                                status_text.text(info['message'])
                            
                            fix_result = region_handler.fix_region_names(
                                dry_run=False,
                                progress_callback=update_progress,
                                operator=SessionManager.get_username() or 'admin'
                            )
                            
                            st.session_state.confirm_name_sync = False
                            
                            if 'error' not in fix_result:
                                st.success(f"✅ 同步完成！耗时 {fix_result.get('elapsed_seconds', 0):.1f} 秒")
                                
                                col_r1, col_r2, col_r3 = st.columns(3)
                                col_r1.metric("省份修复", f"{fix_result['province_fixed']:,}")
                                col_r2.metric("城市修复", f"{fix_result['city_fixed']:,}")
                                col_r3.metric("区县修复", f"{fix_result['district_fixed']:,}")
                                
                                st.info(f"备份ID: {fix_result['backup_id']} | 总计修复: {fix_result['total_records']:,} 条")
                                if 'name_sync_result' in st.session_state:
                                    del st.session_state.name_sync_result
                            else:
                                st.error(f"❌ 同步失败: {fix_result['error']}")
        
        # ========== 区县编码修复 ==========
        elif region_cleaning_mode == "区县编码修复":
            st.subheader("🏘️ 区县编码修复")
            st.caption("修复区县代码与城市代码前4位不一致的记录（以充电站位置为准）")
            
            col_scan1, col_scan2 = st.columns([1, 3])
            with col_scan1:
                scan_district_btn = st.button("🔍 扫描区县编码问题", type="primary", key="scan_district")
            
            if scan_district_btn or 'district_scan_result' in st.session_state:
                if scan_district_btn:
                    with st.spinner("正在扫描区县编码异常..."):
                        try:
                            result = region_handler.scan_district_code_issues()
                            st.session_state.district_scan_result = result
                        except Exception as e:
                            st.error(f"扫描失败: {e}")
                            st.session_state.district_scan_result = None
                
                if 'district_scan_result' in st.session_state and st.session_state.district_scan_result:
                    result = st.session_state.district_scan_result
                    
                    # 统计卡片
                    st.markdown("### 📊 问题概览")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("总记录数", f"{result['total_records']:,}")
                    with col2:
                        st.metric("不匹配记录", f"{result['abnormal_count']:,}",
                                 delta=f"-{result['abnormal_percent']}%", delta_color="inverse")
                    with col3:
                        st.metric("地址可验证", f"{result['address_verifiable']:,}",
                                 delta=f"{result['address_verifiable_percent']}%")
                    with col4:
                        need_manual = result['abnormal_count'] - result['address_verifiable']
                        st.metric("需人工核查", f"{need_manual:,}")
                    
                    # 按省份分布
                    if result['by_province']:
                        st.markdown("### 📍 按省份分布 (Top 15)")
                        import pandas as pd
                        df_province = pd.DataFrame(result['by_province'])
                        df_province.columns = ['省份', '不匹配记录数']
                        st.dataframe(df_province, width="stretch", hide_index=True)
                    
                    st.info("💡 区县编码修复功能开发中。当前建议：先完成城市编码修复，再处理区县编码问题。")
        
        # ========== 修复历史记录 ==========
        elif region_cleaning_mode == "修复历史记录":
            st.subheader("📜 修复历史记录")
            st.caption("查看修复记录，支持回滚操作")
            
            try:
                backup_list = region_handler.get_backup_list()
                
                if backup_list:
                    import pandas as pd
                    df_backup = pd.DataFrame(backup_list)
                    df_backup = df_backup.rename(columns={
                        'backup_id': '备份ID',
                        'fix_type': '修复类型',
                        'record_count': '记录数',
                        'fixed_at': '修复时间',
                        'fixed_by': '操作人',
                        'rollback_at': '回滚时间',
                        'can_rollback': '可回滚'
                    })
                    
                    st.dataframe(df_backup, width="stretch", hide_index=True)
                    
                    # 回滚操作
                    st.markdown("### 🔄 回滚操作")
                    rollback_options = [b['backup_id'] for b in backup_list if b['can_rollback']]
                    
                    if rollback_options:
                        selected_backup = st.selectbox(
                            "选择要回滚的备份",
                            rollback_options,
                            key="rollback_select"
                        )
                        
                        if st.button("🔄 执行回滚", type="secondary", key="rollback_btn"):
                            with st.spinner("正在回滚..."):
                                result = region_handler.rollback_fix(selected_backup)
                                if result['success']:
                                    st.success(f"✅ {result['message']}")
                                    st.rerun()
                                else:
                                    st.error(f"❌ 回滚失败: {result['message']}")
                    else:
                        st.info("没有可回滚的记录")
                else:
                    st.info("暂无修复历史记录")
            except Exception as e:
                st.warning(f"获取历史记录失败: {e}（可能是备份表尚未创建）")

# ========== 标签页8: 多表合并 ==========
with tab8:
    st.header("📎 多表合并")
    st.caption("上传多个运营商 Excel/CSV，按统一表头自动识别并纵向合并，最左侧填充「上报机构」")
    
    st.markdown("---")
    
    merge_upload = st.file_uploader(
        "选择要合并的 Excel 或 CSV 文件（可多选）",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="table_merge_upload",
        help="支持 .xlsx / .xls / .csv；表头将按方案自动判定。",
    )
    
    if merge_upload:
        st.markdown("#### 📁 已选文件")
        file_list = []
        for i, f in enumerate(merge_upload, 1):
            size_mb = f.size / (1024 * 1024)
            file_list.append({"序号": i, "文件名": f.name, "大小 (MB)": f"{size_mb:.2f}"})
        st.dataframe(pd.DataFrame(file_list), use_container_width=True, hide_index=True)
        
        do_merge = st.button("▶ 开始合并", type="primary", key="do_table_merge", use_container_width=False)
        
        if do_merge:
            files = [(f.name, f.getvalue()) for f in merge_upload]
            with st.spinner("正在解析并合并..."):
                try:
                    merged_df, success_list, error_list, row_counts = merge_files(files)
                    if merged_df is not None:
                        st.success("合并完成")
                        m1, m2, m3 = st.columns(3)
                        with m1:
                            st.metric("合并表行数", f"{len(merged_df):,}")
                        with m2:
                            st.metric("合并文件数", len(success_list))
                        with m3:
                            st.metric("未合并文件数", len(error_list), delta=None if not error_list else "见下方")
                        if row_counts:
                            st.markdown("**各表行数**")
                            st.dataframe(
                                pd.DataFrame(row_counts, columns=["文件名", "行数"]),
                                use_container_width=True,
                                hide_index=True,
                            )
                        st.markdown("#### 📊 合并结果预览")
                        st.dataframe(merged_df.head(500), use_container_width=True, hide_index=True)
                        if len(merged_df) > 500:
                            st.caption(f"仅展示前 500 行，共 {len(merged_df):,} 行。")
                        st.markdown("#### 📥 导出")
                        c1, c2 = st.columns(2)
                        with c1:
                            buf = BytesIO()
                            merged_df.to_excel(buf, index=False, engine="openpyxl")
                            buf.seek(0)
                            st.download_button(
                                "下载 Excel",
                                data=buf.getvalue(),
                                file_name="合并结果.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_merged_xlsx",
                            )
                        with c2:
                            buf_csv = BytesIO()
                            merged_df.to_csv(buf_csv, index=False, encoding="utf-8-sig")
                            buf_csv.seek(0)
                            st.download_button(
                                "下载 CSV",
                                data=buf_csv.getvalue(),
                                file_name="合并结果.csv",
                                mime="text/csv",
                                key="download_merged_csv",
                            )
                        if error_list:
                            st.markdown("---")
                            with st.expander("⚠️ 未合并的文件及原因", expanded=False):
                                for e in error_list:
                                    st.markdown(f"- {e}")
                    else:
                        st.error("没有可合并的数据。")
                        if error_list:
                            with st.expander("⚠️ 未合并的文件及原因", expanded=True):
                                for e in error_list:
                                    st.markdown(f"- {e}")
                except Exception as e:
                    st.error(f"合并失败：{e}")
                    import traceback
                    st.markdown("---")
                    with st.expander("错误详情", expanded=False):
                        st.code(traceback.format_exc())
    else:
        st.info("👆 请在上方选择至少一个 Excel 或 CSV 文件后点击「开始合并」。")
        with st.expander("📋 合并规则说明"):
            st.markdown("""
- **表头**：首行含「单位」「参考」「编码方法」之一时，取第二行含「充电桩编号」为表头；否则取首行含「充电桩编号」为表头。
- **多 Sheet**：若存在名称含「1.1」的 Sheet，则以其为主表；否则取第一个有内容的 Sheet。
- **1.2 / 1.3**：多 Sheet 时用 1.2 补全运营商名称/类型，用 1.3 补全厂商名称/类型。
- **上报机构**：每表最左侧添加一列「上报机构」，由文件名清洗得到（去掉 `202512_公共桩_`、`_公共桩`、`附件一：`及其后内容）。
            """)
