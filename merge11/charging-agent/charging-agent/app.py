# app.py - 充电数据AI助手 | 极简稳定版 | 老韩出品 | 模块化升级版

import sys
import os
import warnings

# 过滤 Streamlit 缓存相关的 RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*expire_cache.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*was never awaited.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*")
warnings.filterwarnings("ignore", category=RuntimeWarning, module="streamlit.*")

import streamlit as st
from utils.db_utils import get_db_url

# 🔧 关键修复：将项目根目录加入 Python 路径，解决相对导入问题
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 现在可以安全导入 agent 和 core 模块
from agent import ChargingDataAgent
from langchain_community.utilities import SQLDatabase

# 读取LLM配置
def get_llm_enabled():
    """检查LLM是否启用"""
    try:
        from config import LLM_CONFIG
        return LLM_CONFIG.get('enabled', False)
    except:
        return False

LLM_ENABLED = get_llm_enabled()


st.set_page_config(
    page_title="🔋 充电数据AI助手", 
    layout="centered",
    page_icon="⚡"
)

# 自定义CSS样式 - 现代化绿色和蓝色主题
st.markdown("""
<style>
    /* 全局样式 */
    .main > div {
        padding-top: 3rem;
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
        text-align: center;
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
    .stChatInput > div > div > input {
        border: 2px solid #E0E0E0;
        border-radius: 12px;
        transition: border-color 0.3s ease;
    }
    
    .stChatInput > div > div > input:focus {
        border-color: #00B4D8;
        box-shadow: 0 0 0 3px rgba(0, 180, 216, 0.1);
    }
</style>
""", unsafe_allow_html=True)

st.title("⚡ 充电数据 AI 助手")
st.markdown("""
<div style='text-align: center; color: #4A90A4; font-size: 1.1rem; margin-bottom: 2rem;'>
    🚀 BY VDBP | 本地私有化部署
</div>
""", unsafe_allow_html=True)


@st.cache_resource
def init_database():
    """初始化数据库连接"""
    db_url = get_db_url()  # 使用统一工具函数获取数据库URL
    try:
        return SQLDatabase.from_uri(db_url)
    except Exception as e:
        st.error(f"❌ 数据库连接失败：{str(e)}")
        st.stop()


@st.cache_resource
def init_llm():
    """初始化大模型"""
    if not LLM_ENABLED:
        return None
    try:
        from langchain_community.llms import Ollama
        return Ollama(model="qwen3:30b", temperature=0.1)
    except Exception as e:
        st.warning(f"⚠️ Ollama连接失败: {str(e)}")
        return None


@st.cache_resource
def init_agent():
    """初始化智能体（单例）"""
    db = init_database()
    llm = init_llm()
    if llm is None:
        return None
    # 默认使用evdata表，也可以从数据库获取表名
    table_name = 'evdata'
    return ChargingDataAgent(llm, db, table_name=table_name)


# 检查LLM是否启用
if not LLM_ENABLED:
    st.warning("⚠️ AI大模型功能未启用")
    st.info("""
💡 **如需使用AI智能对话功能，请按以下步骤操作：**

1. 关闭当前应用窗口
2. 重新运行 `charging-agent-launcher.exe`
3. 在配置界面勾选「启用AI大模型功能」
4. 确保本地 Ollama 服务已启动（运行 `ollama run qwen3:30b`）

**当前可用功能**：请使用左侧菜单进入「📊 数据管理系统」查看数据统计和图表分析。
    """)
    st.stop()

# 初始化 Agent
agent = init_agent()

if agent is None:
    st.error("❌ AI助手初始化失败，请检查Ollama服务是否正常运行")
    st.info("💡 确保 Ollama 已启动并运行了 qwen3:30b 模型")
    st.stop()

# 初始化聊天历史
if "messages" not in st.session_state:
    st.session_state.messages = []

if "sql_statements" not in st.session_state:
    st.session_state.sql_statements = {}

if "thinking_processes" not in st.session_state:
    st.session_state.thinking_processes = {}

# 显示历史消息
for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        # 检查是否有对应的SQL语句和思考过程
        sql_to_show = None
        thinking_steps = None
        if msg["role"] == "assistant":
            if str(idx) in st.session_state.sql_statements:
                sql_to_show = st.session_state.sql_statements[str(idx)]
            if str(idx) in st.session_state.thinking_processes:
                thinking_steps = st.session_state.thinking_processes[str(idx)]
        
        # 显示思考过程（如果有，折叠显示）
        if thinking_steps:
            with st.expander("🧠 思考过程", expanded=False):
                thinking_html = "<div style='font-family: monospace; line-height: 1.8;'>"
                for step_name, step_content in thinking_steps:
                    # 根据步骤类型设置不同的图标和颜色
                    if "意图" in step_name or "理解" in step_name:
                        icon = "🔍"
                        color = "#4A90A4"
                    elif "SQL" in step_name or "生成" in step_name:
                        icon = "🤖"
                        color = "#00B4D8"
                    elif "执行" in step_name or "查询" in step_name:
                        icon = "🔍"
                        color = "#0096C7"
                    elif "格式化" in step_name or "结果" in step_name:
                        icon = "📊"
                        color = "#00695C"
                    elif "完成" in step_name or "识别" in step_name:
                        icon = "✅"
                        color = "#4CAF50"
                    elif "错误" in step_name:
                        icon = "❌"
                        color = "#F44336"
                    elif "重定向" in step_name:
                        icon = "🔄"
                        color = "#FF9800"
                    elif "耗时" in step_name:
                        icon = "⏱️"
                        color = "#9E9E9E"
                    else:
                        icon = "💭"
                        color = "#757575"
                    
                    thinking_html += f"""
                    <div style='margin-bottom: 12px; padding: 8px; background: #f5f5f5; border-radius: 6px; border-left: 4px solid {color};'>
                        <strong style='color: {color};'>{icon} {step_name}</strong>
                        <div style='margin-top: 6px; color: #333; font-size: 0.9em;'>
                    """
                    
                    # 如果是SQL代码，特殊处理
                    if step_content.startswith("```sql"):
                        thinking_html += "<pre style='background: #263238; color: #aed581; padding: 10px; border-radius: 4px; overflow-x: auto;'><code>"
                        sql_code = step_content.replace("```sql\n", "").replace("\n```", "")
                        thinking_html += sql_code.replace("<", "&lt;").replace(">", "&gt;")
                        thinking_html += "</code></pre>"
                    else:
                        formatted_content = step_content.replace("\n", "<br>")
                        thinking_html += formatted_content
                    
                    thinking_html += """
                        </div>
                    </div>
                    """
                
                thinking_html += "</div>"
                st.markdown(thinking_html, unsafe_allow_html=True)
        
        # 显示SQL语句（如果有）
        if sql_to_show:
            with st.expander("📋 查看SQL查询语句", expanded=False):
                st.code(sql_to_show, language="sql")
        
        # 显示消息内容
        st.markdown(msg["content"])

# 用户输入
prompt = st.chat_input("问点什么？")
if prompt:
    # 显示用户消息
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # 显示助手回复
    with st.chat_message("assistant"):
        # 执行查询并获取结果
        result = agent.ask(prompt, debug=True)
        # 处理返回结果（可能是4元组或6元组）
        needs_streaming = False
        stats = None
        report_dict = None
        
        if len(result) == 6:
            response, sql, thinking_steps, report_dict, needs_streaming, stats = result
        elif len(result) == 4:
            response, sql, thinking_steps, report_dict = result
        else:
            # 兼容旧格式（3元组）
            response, sql, thinking_steps = result[0], result[1] if len(result) > 1 else None, result[2] if len(result) > 2 else []
        
        # 显示思考过程
        with st.expander("🧠 思考过程", expanded=True):
            thinking_html = "<div style='font-family: monospace; line-height: 1.8;'>"
            for step_name, step_content in thinking_steps:
                # 根据步骤类型设置不同的图标和颜色
                if "意图" in step_name or "理解" in step_name:
                    icon = "🔍"
                    color = "#4A90A4"
                elif "SQL" in step_name or "生成" in step_name:
                    icon = "🤖"
                    color = "#00B4D8"
                elif "执行" in step_name or "查询" in step_name:
                    icon = "🔍"
                    color = "#0096C7"
                elif "格式化" in step_name or "结果" in step_name:
                    icon = "📊"
                    color = "#00695C"
                elif "完成" in step_name or "识别" in step_name:
                    icon = "✅"
                    color = "#4CAF50"
                elif "错误" in step_name:
                    icon = "❌"
                    color = "#F44336"
                elif "重定向" in step_name:
                    icon = "🔄"
                    color = "#FF9800"
                elif "耗时" in step_name:
                    icon = "⏱️"
                    color = "#9E9E9E"
                else:
                    icon = "💭"
                    color = "#757575"
                
                thinking_html += f"""
                <div style='margin-bottom: 12px; padding: 8px; background: #f5f5f5; border-radius: 6px; border-left: 4px solid {color};'>
                    <strong style='color: {color};'>{icon} {step_name}</strong>
                    <div style='margin-top: 6px; color: #333; font-size: 0.9em;'>
                """
                
                # 如果是SQL代码，特殊处理
                if step_content.startswith("```sql"):
                    thinking_html += "<pre style='background: #263238; color: #aed581; padding: 10px; border-radius: 4px; overflow-x: auto;'><code>"
                    sql_code = step_content.replace("```sql\n", "").replace("\n```", "")
                    thinking_html += sql_code.replace("<", "&lt;").replace(">", "&gt;")
                    thinking_html += "</code></pre>"
                else:
                    # 普通文本，支持换行
                    formatted_content = step_content.replace("\n", "<br>")
                    thinking_html += formatted_content
                
                thinking_html += """
                    </div>
                </div>
                """
            
            thinking_html += "</div>"
            st.markdown(thinking_html, unsafe_allow_html=True)
        
        # 显示SQL语句（如果有）
        if sql:
            with st.expander("📋 查看SQL查询语句", expanded=False):
                st.code(sql, language="sql")
        
        # 显示回答内容
        # 检查是否是报告类型，如果是，需要特殊处理图表显示
        if report_dict and 'sections' in report_dict:
            # 显示报告标题和元信息
            if 'title' in report_dict:
                st.markdown(f"## {report_dict['title']}")
            if 'generation_date' in report_dict:
                st.caption(f"生成日期：{report_dict['generation_date']}")
            
            # 遍历所有章节，显示内容和图表
            for section in report_dict['sections']:
                section_title = section.get('title', '')
                section_content = section.get('content', '')
                chart_data = section.get('chart_data')
                needs_ai = section.get('needs_ai_generation', False)
                
                # 显示章节标题
                if section_title:
                    st.markdown(f"### {section_title}")
                
                # 显示章节内容
                if section_content:
                    st.markdown(section_content)
                
                # 显示图表（如果有）
                if chart_data:
                    col1, col2 = st.columns(2)
                    with col1:
                        if 'bar_chart' in chart_data and chart_data['bar_chart']:
                            st.plotly_chart(chart_data['bar_chart'], width="stretch")
                    with col2:
                        if 'pie_chart' in chart_data and chart_data['pie_chart']:
                            st.plotly_chart(chart_data['pie_chart'], width="stretch")
                
                # 如果是需要AI生成的章节（结论与建议），进行流式生成
                if needs_ai and needs_streaming and stats:
                    st.markdown("---\n")
                    with st.spinner("🤖 AI正在生成结论与建议..."):
                        conclusion_placeholder = st.empty()
                        full_conclusion = ""
                        
                        try:
                            handler = agent.orchestrator.report_generator.handler
                            for chunk in handler.generate_ai_conclusions_stream(report_dict, stats, target_length=800):
                                full_conclusion += chunk
                                conclusion_placeholder.markdown(full_conclusion)
                        except Exception as e:
                            st.error(f"❌ 流式生成失败: {str(e)}")
                            if section.get('fallback_content'):
                                conclusion_placeholder.markdown(section['fallback_content'])
                
                st.markdown("---\n")
        elif needs_streaming and report_dict and stats:
            # 兼容旧格式：先显示模板内容
            st.markdown(response)
            
            # 流式生成并显示AI结论
            st.markdown("---\n")
            with st.spinner("🤖 AI正在生成结论与建议..."):
                conclusion_placeholder = st.empty()
                full_conclusion = ""
                
                try:
                    handler = agent.orchestrator.report_generator.handler
                    for chunk in handler.generate_ai_conclusions_stream(report_dict, stats, target_length=800):
                        full_conclusion += chunk
                        conclusion_placeholder.markdown(full_conclusion)
                except Exception as e:
                    st.error(f"❌ 流式生成失败: {str(e)}")
                    if report_dict and 'sections' in report_dict:
                        for section in report_dict['sections']:
                            if section.get('needs_ai_generation') and section.get('fallback_content'):
                                conclusion_placeholder.markdown(section['fallback_content'])
                                break
        else:
            # 不需要流式生成，直接显示
            st.markdown(response)
    
    # 保存消息
    st.session_state.messages.append({"role": "assistant", "content": response})
    
    # 保存SQL语句和思考过程（使用消息索引作为key）
    message_idx = len(st.session_state.messages) - 1
    if sql:
        st.session_state.sql_statements[str(message_idx)] = sql
    if thinking_steps:
        st.session_state.thinking_processes[str(message_idx)] = thinking_steps
