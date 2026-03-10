# pages/audit_logs.py - 审计日志页面（管理员专属）

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from services.audit_service import AuditService


def show_audit_logs_page():
    """显示审计日志页面"""
    
    st.header("📋 审计日志")
    st.markdown("---")
    
    audit_service = AuditService()
    
    # Tab: 操作日志 | 登录日志 | 统计概览
    tab1, tab2, tab3 = st.tabs(["📝 操作日志", "🔐 登录日志", "📊 统计概览"])
    
    with tab1:
        show_audit_logs(audit_service)
    
    with tab2:
        show_login_logs(audit_service)
    
    with tab3:
        show_audit_statistics(audit_service)


def show_audit_logs(audit_service: AuditService):
    """显示操作审计日志"""
    
    st.subheader("操作审计日志")
    
    # 筛选条件
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        filter_username = st.text_input("用户名", placeholder="筛选用户", key="audit_filter_user")
    
    with col2:
        action_types = ['', 'login', 'logout', 'login_failed', 'query', 'import_start', 
                       'import_success', 'import_failed', 'user_create', 'user_update', 
                       'user_delete', 'password_reset']
        filter_action = st.selectbox("操作类型", action_types, key="audit_filter_action")
    
    with col3:
        modules = ['', 'auth', 'data_import', 'data_query', 'data_analysis', 
                  'user_manage', 'report']
        filter_module = st.selectbox("功能模块", modules, key="audit_filter_module")
    
    with col4:
        result_options = ['', 'success', 'failed']
        filter_result = st.selectbox(
            "执行结果", 
            result_options,
            format_func=lambda x: {'': '全部', 'success': '成功', 'failed': '失败'}.get(x, x),
            key="audit_filter_result"
        )
    
    # 日期范围
    col5, col6, col7 = st.columns([1, 1, 1])
    with col5:
        days_options = [7, 14, 30, 90]
        days = st.selectbox("时间范围", days_options, format_func=lambda x: f"最近{x}天", key="audit_days")
    
    with col6:
        limit = st.number_input("显示条数", min_value=10, max_value=500, value=100, step=10, key="audit_limit")
    
    # 查询日志
    start_date = datetime.now() - timedelta(days=days)
    
    logs = audit_service.get_audit_logs(
        username=filter_username or None,
        action_type=filter_action or None,
        module=filter_module or None,
        start_date=start_date,
        result_status=filter_result or None,
        limit=limit
    )
    
    if not logs:
        st.info("📭 暂无审计日志")
        return
    
    # 转换为DataFrame
    df = pd.DataFrame(logs)
    
    # 格式化
    df['时间'] = df['created_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if x else '')
    df['结果'] = df['result_status'].map({'success': '✅ 成功', 'failed': '❌ 失败'})
    df['耗时'] = df['execution_time_ms'].apply(lambda x: f"{x}ms" if x else '-')
    
    # 显示
    display_df = df[['时间', 'username', 'action_type', 'action_desc', 'module', '结果', 'affected_rows', '耗时']]
    display_df.columns = ['时间', '用户', '操作类型', '操作描述', '模块', '结果', '影响行数', '耗时']
    
    st.dataframe(display_df, width="stretch", hide_index=True)
    
    st.caption(f"共显示 {len(logs)} 条记录")
    
    # 详情展开
    with st.expander("📋 查看日志详情"):
        if logs:
            selected_idx = st.selectbox(
                "选择日志条目",
                range(len(logs)),
                format_func=lambda i: f"{logs[i]['created_at']} - {logs[i]['username']} - {logs[i]['action_desc']}"
            )
            
            if selected_idx is not None:
                log = logs[selected_idx]
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**ID:** {log['id']}")
                    st.markdown(f"**用户:** {log['username']}")
                    st.markdown(f"**操作类型:** {log['action_type']}")
                    st.markdown(f"**操作描述:** {log['action_desc']}")
                    st.markdown(f"**模块:** {log['module']}")
                
                with col2:
                    st.markdown(f"**目标表:** {log['target_table'] or '-'}")
                    st.markdown(f"**目标ID:** {log['target_id'] or '-'}")
                    st.markdown(f"**结果:** {log['result_status']}")
                    st.markdown(f"**影响行数:** {log['affected_rows']}")
                    st.markdown(f"**IP地址:** {log['ip_address'] or '-'}")
                
                if log['error_message']:
                    st.error(f"**错误信息:** {log['error_message']}")
                
                if log['request_params']:
                    st.markdown("**请求参数:**")
                    st.code(log['request_params'])


def show_login_logs(audit_service: AuditService):
    """显示登录日志"""
    
    st.subheader("登录日志")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        filter_username = st.text_input("用户名", placeholder="筛选用户", key="login_filter_user")
    
    with col2:
        days = st.selectbox("时间范围", [7, 14, 30, 90], format_func=lambda x: f"最近{x}天", key="login_days")
    
    with col3:
        limit = st.number_input("显示条数", min_value=10, max_value=500, value=100, step=10, key="login_limit")
    
    logs = audit_service.get_login_logs(
        username=filter_username or None,
        days=days,
        limit=limit
    )
    
    if not logs:
        st.info("📭 暂无登录日志")
        return
    
    df = pd.DataFrame(logs)
    
    # 格式化
    df['时间'] = df['created_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if x else '')
    df['类型'] = df['login_type'].map({
        'login': '🔓 登录',
        'logout': '🔒 登出',
        'failed': '❌ 失败'
    })
    df['结果'] = df['login_result'].map({'success': '✅', 'failed': '❌'})
    
    display_df = df[['时间', 'username', '类型', '结果', 'failure_reason', 'ip_address']]
    display_df.columns = ['时间', '用户', '类型', '结果', '失败原因', 'IP地址']
    display_df['失败原因'] = display_df['失败原因'].fillna('-')
    display_df['IP地址'] = display_df['IP地址'].fillna('-')
    
    st.dataframe(display_df, width="stretch", hide_index=True)
    
    st.caption(f"共显示 {len(logs)} 条记录")


def show_audit_statistics(audit_service: AuditService):
    """显示审计统计概览"""
    
    st.subheader("审计统计概览")
    
    days = st.selectbox("统计周期", [7, 14, 30, 90], format_func=lambda x: f"最近{x}天", key="stats_days")
    
    stats = audit_service.get_audit_statistics(days=days)
    
    if not stats:
        st.info("暂无统计数据")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 操作类型统计")
        action_stats = stats.get('action_stats', {})
        if action_stats:
            action_df = pd.DataFrame(
                list(action_stats.items()),
                columns=['操作类型', '次数']
            ).sort_values('次数', ascending=False)
            st.bar_chart(action_df.set_index('操作类型'))
            st.dataframe(action_df, width="stretch", hide_index=True)
        else:
            st.info("暂无数据")
    
    with col2:
        st.markdown("### 👥 用户活跃度 TOP10")
        user_stats = stats.get('user_stats', {})
        if user_stats:
            user_df = pd.DataFrame(
                list(user_stats.items()),
                columns=['用户', '操作次数']
            )
            st.bar_chart(user_df.set_index('用户'))
            st.dataframe(user_df, width="stretch", hide_index=True)
        else:
            st.info("暂无数据")
    
    st.markdown("---")
    st.markdown("### 🔐 登录统计")
    
    login_stats = stats.get('login_stats', {})
    if login_stats:
        col3, col4, col5 = st.columns(3)
        with col3:
            st.metric("登录成功", login_stats.get('success', 0))
        with col4:
            st.metric("登录失败", login_stats.get('failed', 0))
        with col5:
            total = login_stats.get('success', 0) + login_stats.get('failed', 0)
            success_rate = (login_stats.get('success', 0) / total * 100) if total > 0 else 0
            st.metric("成功率", f"{success_rate:.1f}%")
    else:
        st.info("暂无登录统计数据")
