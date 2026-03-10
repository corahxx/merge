# services/__init__.py - 服务层

# 延迟导入，避免循环导入问题
def get_user_service():
    from .user_service import UserService
    return UserService()

def get_audit_service():
    from .audit_service import AuditService
    return AuditService()

# 直接导入审计服务的便捷函数
from .audit_service import audit_log, get_audit_service as _get_audit_service

__all__ = ['get_user_service', 'get_audit_service', 'audit_log']
