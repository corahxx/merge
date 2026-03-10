# main.py - 充电数据管理系统主入口 | 带侧边导航栏 + 用户认证

import sys
import os
import warnings

# 过滤 Streamlit 缓存相关的 RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*expire_cache.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*was never awaited.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="streamlit.*")

import streamlit as st

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Streamlit页面配置
st.set_page_config(
    page_title="🔋 充电数据管理系统",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 导入认证模块
from auth.authenticator import Authenticator, show_login_page, show_user_info_sidebar
from auth.permission_checker import PermissionChecker

# 初始化认证器
auth = Authenticator()

# ============ 登录检查 ============
if not auth.is_authenticated():
    # 未登录，显示登录页面
    show_login_page()
    st.stop()

# ============ 已登录，显示主系统 ============

# 自定义CSS样式 - EVCIPA中国充电联盟风格
st.markdown("""
<style>
    /* ========== 全局样式 ========== */
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;500;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Noto Sans SC', 'Microsoft YaHei', sans-serif;
    }
    
    /* ========== 顶部标题栏样式 ========== */
    header[data-testid="stHeader"] {
        background: linear-gradient(135deg, #2DB7B0 0%, #1A9B95 100%);
        color: white;
    }
    
    /* ========== 侧边栏样式 ========== */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #F0F8F7 0%, #E5F2F1 100%);
        border-right: 3px solid #2DB7B0;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {
        color: #1A9B95;
        font-weight: 700;
        border-bottom: 2px solid #2DB7B0;
        padding-bottom: 10px;
    }
    
    /* ========== 导航按钮样式 ========== */
    .stRadio > div {
        display: flex;
        flex-direction: column;
        gap: 0.3rem;
    }
    
    .stRadio > div > label {
        font-size: 1rem;
        font-weight: 500;
        padding: 0.6rem 1rem;
        border-radius: 6px;
        border-left: 3px solid transparent;
        transition: all 0.25s ease;
        cursor: pointer;
        color: #333333;
    }
    
    .stRadio > div > label:hover {
        background-color: rgba(45, 183, 176, 0.15);
        border-left: 3px solid #2DB7B0;
        color: #1A9B95;
    }
    
    .stRadio > div > label[data-checked="true"] {
        background-color: rgba(45, 183, 176, 0.2);
        border-left: 3px solid #2DB7B0;
        color: #1A9B95;
        font-weight: 600;
    }
    
    /* ========== 主内容区域 ========== */
    .main > div {
        padding-top: 1rem;
    }
    
    /* ========== 按钮样式 ========== */
    .stButton > button {
        background: linear-gradient(135deg, #2DB7B0 0%, #1A9B95 100%);
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #1A9B95 0%, #158B85 100%);
        box-shadow: 0 4px 12px rgba(45, 183, 176, 0.3);
        transform: translateY(-1px);
    }
    
    /* ========== 链接和强调色 ========== */
    a {
        color: #FF6600 !important;
        text-decoration: none;
    }
    
    a:hover {
        color: #E55A00 !important;
        text-decoration: underline;
    }
    
    /* ========== 标签页样式 ========== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #F0F8F7;
        border-radius: 8px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        color: #333333;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #2DB7B0;
        color: white;
    }
    
    /* ========== 数据框/表格样式 ========== */
    [data-testid="stDataFrame"] {
        border: 1px solid #E5F2F1;
        border-radius: 8px;
    }
    
    /* ========== 指标卡片样式 ========== */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #F0F8F7 0%, #E5F2F1 100%);
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #2DB7B0;
    }
    
    [data-testid="stMetricValue"] {
        color: #1A9B95;
        font-weight: 700;
    }
    
    /* ========== 成功/警告/错误提示 ========== */
    .stSuccess {
        background-color: rgba(45, 183, 176, 0.1);
        border-left: 4px solid #2DB7B0;
    }
    
    .stWarning {
        border-left: 4px solid #FF6600;
    }
    
    /* ========== 输入框样式 ========== */
    .stTextInput > div > div > input,
    .stSelectbox > div > div,
    .stNumberInput > div > div > input {
        border-color: #2DB7B0;
        border-radius: 6px;
    }
    
    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div:focus {
        border-color: #1A9B95;
        box-shadow: 0 0 0 2px rgba(45, 183, 176, 0.2);
    }
    
    /* ========== 科技感装饰横幅 ========== */
    .tech-banner {
        background: linear-gradient(135deg, #1E5799 0%, #207cca 50%, #2989d8 100%);
        color: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin: 1rem 0;
        text-align: center;
        font-weight: 500;
    }
    
    /* ========== 板块标题装饰 ========== */
    .section-title {
        border-left: 4px solid #2DB7B0;
        padding-left: 12px;
        font-size: 1.2rem;
        font-weight: 600;
        color: #333333;
        margin: 1rem 0;
    }
    
    /* ========== 卡片样式 ========== */
    .card {
        background: white;
        border-radius: 8px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        border: 1px solid #E5F2F1;
        transition: all 0.3s ease;
    }
    
    .card:hover {
        box-shadow: 0 4px 16px rgba(45, 183, 176, 0.15);
        border-color: #2DB7B0;
    }
    
    /* ========== 页脚样式 ========== */
    .footer {
        text-align: center;
        color: #666666;
        font-size: 0.85rem;
        padding: 1rem;
        border-top: 1px solid #E5F2F1;
        margin-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# 侧边栏导航 - EVCIPA风格（带LOGO）
import os as _os
_logo_path = _os.path.join(_os.path.dirname(__file__), 'assets', 'logo.png')

if _os.path.exists(_logo_path):
    # 显示LOGO
    col_l1, col_l2, col_l3 = st.sidebar.columns([1, 2, 1])
    with col_l2:
        st.image(_logo_path, width=80)
    st.sidebar.markdown("""
    <div style="text-align: center; padding: 5px 0;">
        <div style="font-size: 1rem; font-weight: 700; color: #1A9B95;">充电数据管理系统</div>
        <div style="font-size: 0.7rem; color: #666; margin-top: 2px;">Charging Data Management</div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.sidebar.markdown("""
    <div style="text-align: center; padding: 10px 0;">
        <div style="font-size: 2rem; margin-bottom: 5px;">⚡</div>
        <div style="font-size: 1.1rem; font-weight: 700; color: #1A9B95;">充电数据管理系统</div>
        <div style="font-size: 0.75rem; color: #666; margin-top: 3px;">Charging Data Management</div>
    </div>
    """, unsafe_allow_html=True)
st.sidebar.markdown("---")

# 初始化session_state中的页面选择
if 'current_page' not in st.session_state:
    st.session_state.current_page = "🤖 AI助手"

# 根据用户权限构建页面列表
page_options = [
    "🤖 AI助手",
    "📊 数据管理系统",
    "🔍 充电桩型号查询"
]

# 管理员专属页面
if PermissionChecker.is_admin():
    page_options.append("👥 用户管理")
    page_options.append("📋 审计日志")

# 页面选择
page = st.sidebar.radio(
    "📑 选择页面",
    page_options,
    index=0,
    key="page_selector"
)

# 显示用户信息和退出按钮
show_user_info_sidebar()

st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style='text-align: center; padding: 15px 10px; background: linear-gradient(135deg, #E5F2F1 0%, #F0F8F7 100%); border-radius: 8px; margin-top: 1rem;'>
    <div style='color: #1A9B95; font-size: 0.85rem; font-weight: 600;'>🚀 BY VDBP</div>
    <div style='color: #666; font-size: 0.75rem; margin-top: 5px;'>本地私有化部署</div>
</div>
""", unsafe_allow_html=True)

# 根据选择加载不同的页面
if page == "🤖 AI助手":
    # 导入并运行AI助手页面
    try:
        # 直接导入app.py的代码，但需要处理st.set_page_config
        # 使用importlib动态导入
        import importlib.util
        import re
        
        app_path = os.path.join(os.path.dirname(__file__), "app.py")
        
        # 读取代码
        with open(app_path, 'r', encoding='utf-8') as f:
            app_lines = f.readlines()
        
        # 注释掉st.set_page_config整个调用块，避免重复配置
        # 逐行处理，找到 st.set_page_config( 后注释掉直到找到匹配的 )
        app_code_lines = []
        in_page_config = False
        paren_count = 0
        
        for line in app_lines:
            if 'st.set_page_config(' in line:
                in_page_config = True
                paren_count = line.count('(') - line.count(')')
                app_code_lines.append('# ' + line)
            elif in_page_config:
                paren_count += line.count('(') - line.count(')')
                app_code_lines.append('# ' + line)
                if paren_count == 0:
                    in_page_config = False
            else:
                app_code_lines.append(line)
        
        app_code = ''.join(app_code_lines)
        
        # 准备执行环境
        exec_globals = {
            '__name__': '__main__',
            '__file__': app_path,
            '__builtins__': __builtins__,
        }
        
        # 导入必要的模块到执行环境
        exec("import sys", exec_globals)
        exec("import os", exec_globals)
        exec("import streamlit as st", exec_globals)
        exec("sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))", exec_globals)
        
        # 执行代码
        exec(compile(app_code, app_path, 'exec'), exec_globals)
        
    except Exception as e:
        st.error(f"❌ 加载AI助手页面失败：{str(e)}")
        import traceback
        st.code(traceback.format_exc())
        
elif page == "📊 数据管理系统":
    # 导入并运行数据管理系统页面
    try:
        import importlib.util
        import re
        
        data_manager_path = os.path.join(os.path.dirname(__file__), "data_manager.py")
        
        with open(data_manager_path, 'r', encoding='utf-8') as f:
            data_manager_lines = f.readlines()
        
        # 注释掉st.set_page_config整个调用块
        data_manager_code_lines = []
        in_page_config = False
        paren_count = 0
        
        for line in data_manager_lines:
            if 'st.set_page_config(' in line:
                in_page_config = True
                paren_count = line.count('(') - line.count(')')
                data_manager_code_lines.append('# ' + line)
            elif in_page_config:
                paren_count += line.count('(') - line.count(')')
                data_manager_code_lines.append('# ' + line)
                if paren_count == 0:
                    in_page_config = False
            else:
                data_manager_code_lines.append(line)
        
        data_manager_code = ''.join(data_manager_code_lines)
        
        # 准备执行环境
        exec_globals = {
            '__name__': '__main__',
            '__file__': data_manager_path,
            '__builtins__': __builtins__,
        }
        
        # 导入必要的模块
        exec("import sys", exec_globals)
        exec("import os", exec_globals)
        exec("import streamlit as st", exec_globals)
        exec("import warnings", exec_globals)
        exec("sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))", exec_globals)
        
        # 执行代码
        exec(compile(data_manager_code, data_manager_path, 'exec'), exec_globals)
        
    except Exception as e:
        st.error(f"❌ 加载数据管理系统页面失败：{str(e)}")
        import traceback
        st.code(traceback.format_exc())
        
elif page == "🔍 充电桩型号查询":
    # 导入并运行充电桩型号查询页面
    try:
        import importlib.util
        import re
        
        pile_query_path = os.path.join(os.path.dirname(__file__), "pile_model_query.py")
        
        with open(pile_query_path, 'r', encoding='utf-8') as f:
            pile_query_lines = f.readlines()
        
        # 注释掉st.set_page_config整个调用块
        pile_query_code_lines = []
        in_page_config = False
        paren_count = 0
        
        for line in pile_query_lines:
            if 'st.set_page_config(' in line:
                in_page_config = True
                paren_count = line.count('(') - line.count(')')
                pile_query_code_lines.append('# ' + line)
            elif in_page_config:
                paren_count += line.count('(') - line.count(')')
                pile_query_code_lines.append('# ' + line)
                if paren_count == 0:
                    in_page_config = False
            else:
                pile_query_code_lines.append(line)
        
        pile_query_code = ''.join(pile_query_code_lines)
        
        # 准备执行环境
        exec_globals = {
            '__name__': '__main__',
            '__file__': pile_query_path,
            '__builtins__': __builtins__,
        }
        
        # 导入必要的模块
        exec("import sys", exec_globals)
        exec("import os", exec_globals)
        exec("import streamlit as st", exec_globals)
        exec("import warnings", exec_globals)
        exec("sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))", exec_globals)
        
        # 执行代码
        exec(compile(pile_query_code, pile_query_path, 'exec'), exec_globals)
        
    except Exception as e:
        st.error(f"❌ 加载充电桩型号查询页面失败：{str(e)}")
        import traceback
        st.code(traceback.format_exc())

elif page == "👥 用户管理":
    # 管理员专属：用户管理页面
    if not PermissionChecker.is_admin():
        st.error("⛔ 您没有权限访问此页面")
        st.stop()
    
    from pages.user_management import show_user_management_page
    show_user_management_page()

elif page == "📋 审计日志":
    # 管理员专属：审计日志页面
    if not PermissionChecker.is_admin():
        st.error("⛔ 您没有权限访问此页面")
        st.stop()
    
    from pages.audit_logs import show_audit_logs_page
    show_audit_logs_page()