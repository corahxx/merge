# pile_model_query.py - 充电桩型号查询页面

import sys
import os
import warnings

# 过滤 Streamlit 缓存相关的 RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*expire_cache.*")

import streamlit as st
import pandas as pd
from datetime import datetime

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_CONFIG
from handlers.pile_model_query_handler import PileModelQueryHandler
from data.error_handler import ErrorHandler

# Streamlit页面配置
st.set_page_config(
    page_title="🔍 充电桩型号查询",
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
        text-align: center;
    }
    
    /* 副标题样式 */
    .stMarkdown {
        color: #4A90A4;
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
        width: 100%;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #0096C7 0%, #0077B6 100%);
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0, 180, 216, 0.4);
    }
    
    /* 输入框样式 */
    .stTextArea > div > div > textarea {
        border: 2px solid #E0E0E0;
        border-radius: 8px;
        transition: border-color 0.3s ease;
    }
    
    .stTextArea > div > div > textarea:focus {
        border-color: #00B4D8;
        box-shadow: 0 0 0 3px rgba(0, 180, 216, 0.1);
    }
    
    /* 选择框样式 */
    .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid #E0E0E0;
    }
    
    /* 表格样式 */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
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
</style>
""", unsafe_allow_html=True)

# 标题区域
st.title("🔍 充电桩型号查询")
st.markdown("""
<div style='text-align: center; color: #4A90A4; font-size: 1.2rem; margin-bottom: 2rem;'>
    📊 支持按充电桩型号和省份查询全部信息
</div>
""", unsafe_allow_html=True)

# 侧边栏 - 表名选择
with st.sidebar:
    st.header("⚙️ 配置选项")
    
    # 表名选择
    table_name = st.text_input(
        "数据表名",
        value="evdata",
        help="输入要查询的数据表名称"
    )
    
    st.info(f"📋 当前表: **{table_name}**")
    
    st.divider()
    
    # 刷新省份列表按钮
    if st.button("🔄 刷新省份列表", width="stretch"):
        st.rerun()

# 初始化Handler（不使用缓存，因为表名可能变化）
def init_handler(table_name: str):
    """初始化查询处理器"""
    return PileModelQueryHandler(table_name)

# 获取省份列表（缓存，但包含表名作为key的一部分）
@st.cache_data(ttl=300)  # 缓存5分钟
def get_provinces_cached(table_name: str):
    """获取省份列表（带缓存）"""
    handler = PileModelQueryHandler(table_name)
    return handler.get_provinces()

# 初始化处理器
handler = init_handler(table_name)

# 获取省份列表
try:
    provinces = get_provinces_cached(table_name)
    if not provinces:
        st.warning("⚠️ 无法获取省份列表，请检查数据库连接和表名是否正确")
        provinces = []
except Exception as e:
    st.error(f"❌ 获取省份列表失败: {str(e)}")
    provinces = []

# 查询表单
st.header("📝 查询条件")

col1, col2 = st.columns([2, 1])

with col1:
    # 充电桩型号输入框
    st.subheader("充电桩型号")
    model_input = st.text_area(
        "输入充电桩型号（每行一个，或用逗号分隔，最多300个）",
        height=200,
        help="可以输入多个充电桩型号，每行一个，或用逗号分隔。最多支持300个型号。",
        placeholder="例如：\nTAC_MODEL\nABC123\n或者：TAC_MODEL, ABC123, XYZ789"
    )
    
    # 解析输入的型号
    models = []
    if model_input:
        # 按换行符分割
        lines = [line.strip() for line in model_input.split('\n') if line.strip()]
        for line in lines:
            # 如果一行中有逗号，再按逗号分割
            if ',' in line:
                models.extend([m.strip() for m in line.split(',') if m.strip()])
            else:
                models.append(line.strip())
        
        # 去重
        models = list(dict.fromkeys(models))  # 保持顺序的去重
        
        # 显示解析结果
        if models:
            st.info(f"✅ 已识别 {len(models)} 个充电桩型号")
            if len(models) > 300:
                st.error(f"❌ 充电桩型号数量不能超过300个，当前有 {len(models)} 个")
                models = models[:300]  # 只取前300个
                st.warning(f"⚠️ 已自动截取前300个型号")
            
            # 显示前10个型号预览
            if len(models) <= 10:
                st.write("**型号列表：**", ", ".join(models))
            else:
                st.write("**型号列表（前10个）：**", ", ".join(models[:10]), f"... 等共 {len(models)} 个")

with col2:
    # 省份选择框（多选）
    st.subheader("省份筛选")
    if provinces:
        selected_provinces = st.multiselect(
            "选择省份（可选，可多选）",
            options=provinces,
            default=[],
            help="可以选择多个省份进行筛选，不选择则不限制省份"
        )
        
        # 如果选择了省份，显示选择的数量
        if selected_provinces:
            st.info(f"✅ 已选择 {len(selected_provinces)} 个省份")
    else:
        st.warning("⚠️ 省份列表为空")
        selected_provinces = []

# 查询按钮
st.divider()

col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])

with col_btn1:
    query_button = st.button("🔍 开始查询", type="primary", width="stretch")

with col_btn2:
    if st.button("🔄 清空条件", width="stretch"):
        st.rerun()

# 执行查询
if query_button:
    if not models:
        st.error("❌ 请至少输入一个充电桩型号")
    else:
        with st.spinner("正在查询数据..."):
            # 如果选择了省份，传递列表；否则传递None
            provinces_to_query = selected_provinces if selected_provinces else None
            result = handler.query_by_model_and_province(models, provinces_to_query)
        
        if result['success']:
            st.success(f"✅ 查询成功！找到 {result['row_count']} 条记录")
            
            # 显示查询统计
            col_stat1, col_stat2, col_stat3 = st.columns(3)
            with col_stat1:
                st.metric("查询结果数", result['row_count'])
            with col_stat2:
                st.metric("查询型号数", result['model_count'])
            with col_stat3:
                province_count = result.get('province_count', 0)
                if province_count > 0:
                    st.metric("筛选省份数", f"{province_count}个")
                else:
                    st.metric("筛选省份", result['province'])
            
            # 检查哪些型号没有查询到信息
            if result['row_count'] > 0:
                df = result['data']
                # 从查询结果中提取所有唯一的充电桩型号
                found_models = set(df['充电桩型号'].dropna().unique().tolist())
                # 与输入的型号列表进行比较（不区分大小写）
                input_models_set = {m.upper() for m in models}
                found_models_set = {str(m).upper() for m in found_models if pd.notna(m)}
                # 找出未找到的型号（保持原始大小写）
                not_found_models = [m for m in models if m.upper() not in found_models_set]
                
                if not_found_models:
                    st.warning(f"⚠️ 以下 {len(not_found_models)} 个充电桩型号未查询到信息：{', '.join(not_found_models)}")
                else:
                    st.info("ℹ️ 所有输入的充电桩型号都已查询到信息")
            else:
                # 如果查询结果为空，所有输入的型号都未找到
                st.warning(f"⚠️ 以下 {len(models)} 个充电桩型号均未查询到信息：{', '.join(models)}")
            
            # 显示查询结果
            if result['row_count'] > 0:
                st.subheader("📊 查询结果")
                
                # 显示数据表格
                df = result['data']
                st.dataframe(df, width="stretch", height=600)
                
                # 导出功能
                st.divider()
                st.subheader("💾 导出数据")
                
                col_export1, col_export2 = st.columns(2)
                
                with col_export1:
                    # 导出为CSV
                    csv = df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 下载CSV文件",
                        data=csv,
                        file_name=f"充电桩型号查询结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        width="stretch"
                    )
                
                with col_export2:
                    # 导出为Excel
                    from io import BytesIO
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='查询结果')
                    excel_data = output.getvalue()
                    st.download_button(
                        label="📥 下载Excel文件",
                        data=excel_data,
                        file_name=f"充电桩型号查询结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width="stretch"
                    )
        else:
            st.error(f"❌ 查询失败: {result.get('error', '未知错误')}")
            
            with st.expander("🔍 查看详细错误信息"):
                error_details = result.get('error_details', {})
                if error_details:
                    st.code(ErrorHandler.format_error_report(error_details))
                else:
                    st.code(str(result.get('error', '未知错误')))

# 使用说明
with st.expander("📖 使用说明", expanded=False):
    st.markdown("""
    ### 功能说明
    
    1. **充电桩型号输入**：
       - 可以输入多个充电桩型号
       - 每行输入一个型号，或用逗号分隔
       - 最多支持300个型号
       - 示例：
         ```
         TAC_MODEL
         ABC123
         XYZ789
         ```
         或
         ```
         TAC_MODEL, ABC123, XYZ789
         ```
    
    2. **省份筛选**：
       - 可以选择多个省份进行筛选（支持多选）
       - 不选择任何省份则不限制省份，查询全部省份
       - 省份列表从数据库中自动读取
    
    3. **查询结果**：
       - 显示所有匹配的记录
       - 支持导出为CSV或Excel格式
       - 结果按充电桩型号、省份、城市、区县排序
    
    4. **注意事项**：
       - 确保输入的表名正确
       - 充电桩型号区分大小写
       - 如果查询结果为空，请检查型号是否正确
    """)

