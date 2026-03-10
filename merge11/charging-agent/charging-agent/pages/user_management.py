# pages/user_management.py - 用户管理页面（管理员专属）

import streamlit as st
import pandas as pd
from datetime import datetime
from services.user_service import UserService
from services.audit_service import AuditService
from auth.session_manager import SessionManager


def show_user_management_page():
    """显示用户管理页面"""
    
    st.header("👥 用户管理")
    st.markdown("---")
    
    user_service = UserService()
    audit_service = AuditService()
    current_user = SessionManager.get_username()
    
    # 用户统计
    role_counts = user_service.get_user_count_by_role()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("管理员", role_counts.get('admin', 0))
    with col2:
        st.metric("操作员", role_counts.get('operator', 0))
    with col3:
        st.metric("查看者", role_counts.get('viewer', 0))
    with col4:
        total = sum(role_counts.values())
        st.metric("总用户数", total)
    
    st.markdown("---")
    
    # Tab: 用户列表 | 新增用户
    tab1, tab2 = st.tabs(["📋 用户列表", "➕ 新增用户"])
    
    with tab1:
        show_user_list(user_service, audit_service, current_user)
    
    with tab2:
        show_add_user_form(user_service, audit_service, current_user)


def show_user_list(user_service: UserService, audit_service: AuditService, current_user: str):
    """显示用户列表"""
    
    users = user_service.get_all_users()
    
    if not users:
        st.info("暂无用户数据")
        return
    
    # 格式化日期的辅助函数（处理None和pandas NaT）
    def format_datetime(dt, default=''):
        if dt is None:
            return default
        # 检查是否是pandas的NaT
        try:
            if pd.isna(dt):
                return default
        except:
            pass
        try:
            return dt.strftime('%Y-%m-%d %H:%M')
        except:
            return default
    
    # 先处理数据再创建DataFrame（避免pandas的NaT问题）
    formatted_users = []
    role_map = {'admin': '🔑 管理员', 'operator': '📝 操作员', 'viewer': '👁️ 查看者'}
    status_map = {'active': '✅ 正常', 'inactive': '⏸️ 停用', 'locked': '🔒 锁定'}
    
    for user in users:
        formatted_users.append({
            '用户名': user['username'],
            '显示名': user['display_name'] or user['username'],
            '角色': role_map.get(user['role'], user['role']),
            '状态': status_map.get(user['status'], user['status']),
            '登录次数': user['login_count'] or 0,
            '最后登录': format_datetime(user['last_login'], '从未登录'),
            '创建时间': format_datetime(user['created_at'], '')
        })
    
    display_df = pd.DataFrame(formatted_users)
    st.dataframe(display_df, width="stretch", hide_index=True)
    
    st.markdown("---")
    st.subheader("🔧 用户操作")
    
    # 选择用户进行操作
    user_options = [u['username'] for u in users]
    selected_username = st.selectbox("选择用户", user_options, key="edit_user_select")
    
    if selected_username:
        selected_user = next((u for u in users if u['username'] == selected_username), None)
        
        if selected_user:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**编辑用户信息**")
                
                new_display_name = st.text_input("显示名称", value=selected_user['display_name'] or '', key="edit_display_name")
                new_email = st.text_input("邮箱", value=selected_user['email'] or '', key="edit_email")
                new_phone = st.text_input("手机号", value=selected_user['phone'] or '', key="edit_phone")
                new_role = st.selectbox(
                    "角色",
                    ['admin', 'operator', 'viewer'],
                    index=['admin', 'operator', 'viewer'].index(selected_user['role']),
                    format_func=lambda x: {'admin': '管理员', 'operator': '操作员', 'viewer': '查看者'}[x],
                    key="edit_role"
                )
                new_status = st.selectbox(
                    "状态",
                    ['active', 'inactive', 'locked'],
                    index=['active', 'inactive', 'locked'].index(selected_user['status']),
                    format_func=lambda x: {'active': '正常', 'inactive': '停用', 'locked': '锁定'}[x],
                    key="edit_status"
                )
                
                if st.button("💾 保存修改", type="primary", key="save_user_btn"):
                    result = user_service.update_user(
                        user_id=selected_user['id'],
                        display_name=new_display_name,
                        email=new_email,
                        phone=new_phone,
                        role=new_role,
                        status=new_status
                    )
                    
                    if result['success']:
                        # 记录审计日志
                        audit_service.log_action(
                            username=current_user,
                            action_type='user_update',
                            action_desc=f'修改用户信息: {selected_username}',
                            module='user_manage',
                            target_table='sys_users',
                            target_id=str(selected_user['id'])
                        )
                        st.success(result['message'])
                        st.rerun()
                    else:
                        st.error(result['message'])
            
            with col2:
                st.markdown("**重置密码**")
                new_password = st.text_input("新密码", type="password", key="reset_pwd")
                confirm_password = st.text_input("确认密码", type="password", key="confirm_pwd")
                
                if st.button("🔄 重置密码", key="reset_pwd_btn"):
                    if not new_password:
                        st.error("请输入新密码")
                    elif new_password != confirm_password:
                        st.error("两次输入的密码不一致")
                    else:
                        result = user_service.reset_password(selected_user['id'], new_password)
                        if result['success']:
                            audit_service.log_action(
                                username=current_user,
                                action_type='password_reset',
                                action_desc=f'重置用户密码: {selected_username}',
                                module='user_manage',
                                target_table='sys_users',
                                target_id=str(selected_user['id'])
                            )
                            st.success(result['message'])
                        else:
                            st.error(result['message'])
                
                st.markdown("---")
                st.markdown("**删除用户**")
                st.warning("⚠️ 删除操作不可恢复，建议使用「停用」代替删除")
                
                if selected_username == current_user:
                    st.info("不能删除当前登录的账号")
                else:
                    if st.button("🗑️ 删除用户", type="secondary", key="delete_user_btn"):
                        result = user_service.delete_user(selected_user['id'])
                        if result['success']:
                            audit_service.log_action(
                                username=current_user,
                                action_type='user_delete',
                                action_desc=f'删除用户: {selected_username}',
                                module='user_manage',
                                target_table='sys_users',
                                target_id=str(selected_user['id'])
                            )
                            st.success(result['message'])
                            st.rerun()
                        else:
                            st.error(result['message'])


def show_add_user_form(user_service: UserService, audit_service: AuditService, current_user: str):
    """显示新增用户表单"""
    
    st.markdown("**创建新用户**")
    
    with st.form("add_user_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            username = st.text_input("用户名 *", placeholder="请输入用户名")
            password = st.text_input("密码 *", type="password", placeholder="至少8位，包含字母和数字")
            confirm_pwd = st.text_input("确认密码 *", type="password")
        
        with col2:
            display_name = st.text_input("显示名称", placeholder="可选，默认为用户名")
            email = st.text_input("邮箱", placeholder="可选")
            phone = st.text_input("手机号", placeholder="可选")
        
        role = st.selectbox(
            "角色",
            ['viewer', 'operator', 'admin'],
            format_func=lambda x: {'admin': '管理员', 'operator': '操作员', 'viewer': '查看者'}[x]
        )
        
        submitted = st.form_submit_button("➕ 创建用户", type="primary", width="stretch")
        
        if submitted:
            # 验证
            if not username:
                st.error("请输入用户名")
            elif not password:
                st.error("请输入密码")
            elif password != confirm_pwd:
                st.error("两次输入的密码不一致")
            else:
                result = user_service.create_user(
                    username=username,
                    password=password,
                    display_name=display_name or username,
                    email=email,
                    phone=phone,
                    role=role,
                    created_by=current_user
                )
                
                if result['success']:
                    audit_service.log_action(
                        username=current_user,
                        action_type='user_create',
                        action_desc=f'创建新用户: {username} (角色: {role})',
                        module='user_manage',
                        target_table='sys_users',
                        target_id=str(result.get('user_id', ''))
                    )
                    st.success(f"✅ {result['message']}")
                    st.balloons()
                else:
                    st.error(f"❌ {result['message']}")
