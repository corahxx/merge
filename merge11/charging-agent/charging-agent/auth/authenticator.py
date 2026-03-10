# auth/authenticator.py - 认证管理器

import streamlit as st
from typing import Dict, Any, Optional
from datetime import datetime

from .session_manager import SessionManager
from .password_utils import verify_password
from .permission_checker import PermissionChecker, ROLE_PERMISSIONS


class Authenticator:
    """用户认证管理器"""
    
    def __init__(self):
        # 延迟导入避免循环导入
        from services.user_service import UserService
        from services.audit_service import AuditService
        self.user_service = UserService()
        self.audit_service = AuditService()
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        用户登录
        :param username: 用户名
        :param password: 密码
        :return: {'success': bool, 'user': dict, 'message': str}
        """
        # 1. 查询用户
        user = self.user_service.get_user_by_username(username)
        if not user:
            self._log_login_failed(username, "用户不存在")
            return {'success': False, 'message': '用户名或密码错误'}
        
        # 2. 检查状态
        if user['status'] != 'active':
            status_messages = {
                'inactive': '账户已停用',
                'locked': '账户已锁定'
            }
            msg = status_messages.get(user['status'], '账户状态异常')
            self._log_login_failed(username, f"账户状态: {user['status']}")
            return {'success': False, 'message': msg}
        
        # 3. 验证密码
        if not verify_password(password, user['password_hash']):
            self._log_login_failed(username, "密码错误")
            return {'success': False, 'message': '用户名或密码错误'}
        
        # 4. 登录成功
        self._log_login_success(username)
        self.user_service.update_last_login(username)
        
        # 5. 获取权限列表
        permissions = ROLE_PERMISSIONS.get(user['role'], [])
        
        # 6. 设置Session
        user_data = {
            'id': user['id'],
            'username': user['username'],
            'display_name': user['display_name'],
            'email': user['email'],
            'role': user['role'],
            'permissions': permissions
        }
        SessionManager.set_user(user_data)
        
        return {
            'success': True,
            'user': user_data,
            'message': '登录成功'
        }
    
    def logout(self):
        """用户登出"""
        user = SessionManager.get_user()
        if user:
            self.audit_service.log_logout(user['username'])
            self.audit_service.log_action(
                username=user['username'],
                action_type='logout',
                action_desc='用户登出系统',
                module='auth'
            )
        SessionManager.clear_session()
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return SessionManager.is_authenticated()
    
    def get_current_user(self) -> Optional[Dict[str, Any]]:
        """获取当前用户信息"""
        return SessionManager.get_user()
    
    def get_current_username(self) -> Optional[str]:
        """获取当前用户名"""
        return SessionManager.get_username()
    
    def check_permission(self, permission: str) -> bool:
        """检查当前用户是否有指定权限"""
        return PermissionChecker.check_permission(permission)
    
    def require_permission(self, permission: str):
        """
        要求指定权限，无权限时显示错误并停止
        """
        if not self.check_permission(permission):
            permission_name = PermissionChecker.get_permission_name(permission)
            st.error(f"⛔ 您没有「{permission_name}」权限")
            st.stop()
    
    def is_admin(self) -> bool:
        """检查当前用户是否是管理员"""
        return PermissionChecker.is_admin()
    
    def _log_login_success(self, username: str):
        """记录登录成功日志"""
        self.audit_service.log_login(username, 'success')
        self.audit_service.log_action(
            username=username,
            action_type='login',
            action_desc='用户登录成功',
            module='auth',
            result_status='success'
        )
    
    def _log_login_failed(self, username: str, reason: str):
        """记录登录失败日志"""
        self.audit_service.log_login(username, 'failed', failure_reason=reason)
        self.audit_service.log_action(
            username=username,
            action_type='login_failed',
            action_desc=f'登录失败: {reason}',
            module='auth',
            result_status='failed',
            error_message=reason
        )


def show_login_page():
    """
    显示登录页面 - EVCIPA风格
    :return: 是否已登录
    """
    auth = Authenticator()
    
    # 检查是否已登录
    if auth.is_authenticated():
        return True
    
    # 注入登录页面专属样式
    st.markdown("""
    <style>
        /* 登录页面背景 */
        .stApp {
            background: linear-gradient(135deg, #F0F8F7 0%, #E5F2F1 50%, #D5EFED 100%);
        }
        
        /* 隐藏侧边栏 */
        [data-testid="stSidebar"] {
            display: none;
        }
        
        /* 登录标题 */
        .login-header {
            text-align: center;
            margin-bottom: 1rem;
        }
        
        .login-logo {
            font-size: 3rem;
            margin-bottom: 0.3rem;
        }
        
        .login-title {
            color: #1A9B95;
            font-size: 1.4rem;
            font-weight: 700;
            margin: 0;
        }
        
        .login-subtitle {
            color: #666;
            font-size: 0.8rem;
            margin-top: 3px;
        }
        
        /* 装饰横幅 */
        .login-banner {
            background: linear-gradient(135deg, #1E5799 0%, #207cca 50%, #2989d8 100%);
            color: white;
            padding: 0.6rem 1rem;
            border-radius: 6px;
            margin-bottom: 1rem;
            text-align: center;
            font-size: 0.85rem;
        }
        
        /* 输入框容器紧凑 */
        .stTextInput {
            margin-bottom: 0.5rem !important;
        }
        
        .stTextInput > div > div > input {
            padding: 0.5rem 0.75rem !important;
            font-size: 0.9rem !important;
        }
        
        .stTextInput > label {
            font-size: 0.85rem !important;
            margin-bottom: 0.2rem !important;
        }
        
        /* 表单按钮 */
        .stFormSubmitButton > button {
            padding: 0.5rem 1rem !important;
            font-size: 0.95rem !important;
        }
        
        /* 页脚 */
        .login-footer {
            text-align: center;
            color: #888;
            font-size: 0.75rem;
            margin-top: 1rem;
            padding-top: 0.8rem;
            border-top: 1px solid #E5F2F1;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # 空白占位
    st.markdown("<br>", unsafe_allow_html=True)
    
    # 更窄的中间列
    col1, col2, col3 = st.columns([1.2, 1, 1.2])
    with col2:
        # 登录卡片头部 - LOGO和标题同行
        import os
        import base64
        logo_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'assets', 'logo.png')
        
        if os.path.exists(logo_path):
            # 读取LOGO并转为base64
            with open(logo_path, "rb") as f:
                logo_base64 = base64.b64encode(f.read()).decode()
            
            # LOGO和标题同行显示
            st.markdown(f"""
            <div style="display: flex; align-items: center; justify-content: center; gap: 15px; margin-bottom: 1rem;">
                <img src="data:image/png;base64,{logo_base64}" width="70" style="flex-shrink: 0;">
                <div style="text-align: left;">
                    <h1 class="login-title" style="margin: 0; font-size: 1.3rem;">充电数据管理系统</h1>
                    <p class="login-subtitle" style="margin: 3px 0 0 0; font-size: 0.75rem;">Charging Data Management System</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            # 无LOGO时使用默认图标
            st.markdown("""
            <div style="display: flex; align-items: center; justify-content: center; gap: 12px; margin-bottom: 1rem;">
                <span style="font-size: 2.5rem;">⚡</span>
                <div style="text-align: left;">
                    <h1 class="login-title" style="margin: 0; font-size: 1.3rem;">充电数据管理系统</h1>
                    <p class="login-subtitle" style="margin: 3px 0 0 0; font-size: 0.75rem;">Charging Data Management System</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # 科技感横幅
        st.markdown("""
        <div class="login-banner">
            🔌 新能源充电基础设施数据分析平台
        </div>
        """, unsafe_allow_html=True)
        
        # 登录表单
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("👤 用户名", placeholder="请输入用户名")
            password = st.text_input("🔑 密码", type="password", placeholder="请输入密码")
            
            submitted = st.form_submit_button("登 录", width="stretch", type="primary")
            
            if submitted:
                if not username or not password:
                    st.error("请输入用户名和密码")
                else:
                    result = auth.login(username, password)
                    if result['success']:
                        st.success(f"✅ 欢迎回来，{result['user']['display_name']}！")
                        st.rerun()
                    else:
                        st.error(f"❌ {result['message']}")
        
        # 页脚
        st.markdown("""
        <div class="login-footer">
            💡 首次使用请联系管理员创建账号<br>
            <span style="color: #1A9B95;">🚀 BY VDBP · 本地私有化部署</span>
        </div>
        """, unsafe_allow_html=True)
    
    return False


def show_user_info_sidebar():
    """在侧边栏显示用户信息 - EVCIPA风格"""
    auth = Authenticator()
    user = auth.get_current_user()
    
    if user:
        with st.sidebar:
            st.markdown("---")
            
            role_names = {
                'admin': '🔑 管理员',
                'operator': '📝 操作员',
                'viewer': '👁️ 查看者'
            }
            role_display = role_names.get(user['role'], user['role'])
            
            # 用户信息卡片
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #E5F2F1 0%, #F0F8F7 100%); 
                        border-radius: 8px; padding: 12px; border-left: 3px solid #2DB7B0;">
                <div style="color: #1A9B95; font-weight: 600; font-size: 0.95rem;">
                    👤 {user['display_name']}
                </div>
                <div style="color: #666; font-size: 0.8rem; margin-top: 3px;">
                    {role_display}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            if st.button("🚪 退出登录", width="stretch"):
                auth.logout()
                st.rerun()
