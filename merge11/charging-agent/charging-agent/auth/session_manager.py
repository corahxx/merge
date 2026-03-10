# auth/session_manager.py - 会话管理

import streamlit as st
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class SessionManager:
    """会话管理器"""
    
    # 会话超时时间（分钟）
    SESSION_TIMEOUT_MINUTES = 30
    
    @staticmethod
    def init_session():
        """初始化会话状态"""
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'user' not in st.session_state:
            st.session_state.user = None
        if 'login_time' not in st.session_state:
            st.session_state.login_time = None
        if 'last_activity' not in st.session_state:
            st.session_state.last_activity = None
    
    @staticmethod
    def is_authenticated() -> bool:
        """检查是否已认证"""
        SessionManager.init_session()
        
        if not st.session_state.authenticated:
            return False
        
        # 检查会话是否超时
        if SessionManager.is_session_expired():
            SessionManager.clear_session()
            return False
        
        # 更新最后活动时间
        st.session_state.last_activity = datetime.now()
        return True
    
    @staticmethod
    def is_session_expired() -> bool:
        """检查会话是否已超时"""
        if st.session_state.last_activity is None:
            return False
        
        timeout = timedelta(minutes=SessionManager.SESSION_TIMEOUT_MINUTES)
        return datetime.now() - st.session_state.last_activity > timeout
    
    @staticmethod
    def set_user(user_data: Dict[str, Any]):
        """
        设置当前登录用户
        :param user_data: 用户信息字典
        """
        SessionManager.init_session()
        st.session_state.authenticated = True
        st.session_state.user = user_data
        st.session_state.login_time = datetime.now()
        st.session_state.last_activity = datetime.now()
    
    @staticmethod
    def get_user() -> Optional[Dict[str, Any]]:
        """获取当前登录用户信息"""
        SessionManager.init_session()
        return st.session_state.user if st.session_state.authenticated else None
    
    @staticmethod
    def get_username() -> Optional[str]:
        """获取当前用户名"""
        user = SessionManager.get_user()
        return user.get('username') if user else None
    
    @staticmethod
    def get_user_role() -> Optional[str]:
        """获取当前用户角色"""
        user = SessionManager.get_user()
        return user.get('role') if user else None
    
    @staticmethod
    def get_permissions() -> list:
        """获取当前用户权限列表"""
        user = SessionManager.get_user()
        return user.get('permissions', []) if user else []
    
    @staticmethod
    def clear_session():
        """清除会话"""
        st.session_state.authenticated = False
        st.session_state.user = None
        st.session_state.login_time = None
        st.session_state.last_activity = None
    
    @staticmethod
    def get_session_info() -> Dict[str, Any]:
        """获取会话信息"""
        SessionManager.init_session()
        return {
            'authenticated': st.session_state.authenticated,
            'user': st.session_state.user,
            'login_time': st.session_state.login_time,
            'last_activity': st.session_state.last_activity,
            'session_timeout_minutes': SessionManager.SESSION_TIMEOUT_MINUTES
        }
