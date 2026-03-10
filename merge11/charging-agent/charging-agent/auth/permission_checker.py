# auth/permission_checker.py - 权限检查器

import streamlit as st
from functools import wraps
from typing import List, Optional
from .session_manager import SessionManager


# 权限定义
PERMISSIONS = {
    'user_manage': '用户管理',
    'data_import': '数据导入',
    'data_export': '数据导出',
    'data_delete': '数据删除',
    'data_query': '数据查询',
    'report_generate': '报告生成',
    'system_config': '系统配置',
    'audit_view': '审计日志查看'
}

# 角色权限映射
ROLE_PERMISSIONS = {
    'admin': ['user_manage', 'data_import', 'data_export', 'data_delete', 
              'data_query', 'report_generate', 'system_config', 'audit_view'],
    'operator': ['data_import', 'data_export', 'data_query', 'report_generate'],
    'viewer': ['data_query', 'report_generate']
}


class PermissionChecker:
    """权限检查器"""
    
    @staticmethod
    def get_role_permissions(role: str) -> List[str]:
        """
        获取角色的权限列表
        :param role: 角色代码
        :return: 权限代码列表
        """
        return ROLE_PERMISSIONS.get(role, [])
    
    @staticmethod
    def check_permission(permission: str) -> bool:
        """
        检查当前用户是否有指定权限
        :param permission: 权限代码
        :return: 是否有权限
        """
        if not SessionManager.is_authenticated():
            return False
        
        user_permissions = SessionManager.get_permissions()
        return permission in user_permissions
    
    @staticmethod
    def check_any_permission(permissions: List[str]) -> bool:
        """
        检查当前用户是否有任意一个指定权限
        :param permissions: 权限代码列表
        :return: 是否有任一权限
        """
        if not SessionManager.is_authenticated():
            return False
        
        user_permissions = SessionManager.get_permissions()
        return any(p in user_permissions for p in permissions)
    
    @staticmethod
    def check_all_permissions(permissions: List[str]) -> bool:
        """
        检查当前用户是否有所有指定权限
        :param permissions: 权限代码列表
        :return: 是否有全部权限
        """
        if not SessionManager.is_authenticated():
            return False
        
        user_permissions = SessionManager.get_permissions()
        return all(p in user_permissions for p in permissions)
    
    @staticmethod
    def is_admin() -> bool:
        """检查当前用户是否是管理员"""
        return SessionManager.get_user_role() == 'admin'
    
    @staticmethod
    def is_operator() -> bool:
        """检查当前用户是否是操作员"""
        return SessionManager.get_user_role() == 'operator'
    
    @staticmethod
    def can_clean_data() -> bool:
        """检查当前用户是否有数据清洗权限（管理员或操作员）"""
        role = SessionManager.get_user_role()
        return role in ('admin', 'operator')
    
    @staticmethod
    def get_permission_name(permission: str) -> str:
        """获取权限的显示名称"""
        return PERMISSIONS.get(permission, permission)


def check_permission(permission: str) -> bool:
    """
    便捷函数：检查当前用户是否有指定权限
    :param permission: 权限代码
    :return: 是否有权限
    """
    return PermissionChecker.check_permission(permission)


def require_permission(permission: str):
    """
    装饰器：要求指定权限，无权限时显示错误并停止
    :param permission: 权限代码
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not PermissionChecker.check_permission(permission):
                permission_name = PermissionChecker.get_permission_name(permission)
                st.error(f"⛔ 您没有「{permission_name}」权限，无法执行此操作")
                st.stop()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_login(func):
    """
    装饰器：要求登录
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not SessionManager.is_authenticated():
            st.warning("🔒 请先登录")
            st.stop()
        return func(*args, **kwargs)
    return wrapper


def require_admin(func):
    """
    装饰器：要求管理员权限
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not PermissionChecker.is_admin():
            st.error("⛔ 此功能仅管理员可用")
            st.stop()
        return func(*args, **kwargs)
    return wrapper
