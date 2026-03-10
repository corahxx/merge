# services/audit_service.py - 审计服务

import time
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from sqlalchemy import text
from utils.db_utils import get_shared_engine


class AuditService:
    """操作审计服务"""
    
    def __init__(self):
        self.engine = get_shared_engine()
    
    def log_action(self,
                   username: str,
                   action_type: str,
                   action_desc: str,
                   module: str,
                   target_table: str = None,
                   target_id: str = None,
                   request_params: Dict = None,
                   result_status: str = 'success',
                   error_message: str = None,
                   affected_rows: int = 0,
                   execution_time_ms: int = None,
                   ip_address: str = None):
        """
        记录操作日志
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO sys_audit_logs 
                    (username, action_type, action_desc, module, target_table, 
                     target_id, request_params, result_status, error_message, 
                     affected_rows, execution_time_ms, ip_address, created_at)
                    VALUES 
                    (:username, :action_type, :action_desc, :module, :target_table,
                     :target_id, :request_params, :result_status, :error_message,
                     :affected_rows, :execution_time_ms, :ip_address, NOW())
                """), {
                    'username': username,
                    'action_type': action_type,
                    'action_desc': action_desc,
                    'module': module,
                    'target_table': target_table,
                    'target_id': target_id,
                    'request_params': json.dumps(request_params, ensure_ascii=False) if request_params else None,
                    'result_status': result_status,
                    'error_message': error_message,
                    'affected_rows': affected_rows,
                    'execution_time_ms': execution_time_ms,
                    'ip_address': ip_address
                })
                conn.commit()
        except Exception as e:
            print(f"审计日志记录失败: {e}")
    
    def log_login(self, username: str, login_result: str, failure_reason: str = None,
                  ip_address: str = None, user_agent: str = None):
        """
        记录登录日志
        :param login_result: 'success' 或 'failed'
        """
        try:
            login_type = 'login' if login_result == 'success' else 'failed'
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO sys_login_logs 
                    (username, login_type, login_result, failure_reason, ip_address, user_agent, created_at)
                    VALUES 
                    (:username, :login_type, :login_result, :failure_reason, :ip_address, :user_agent, NOW())
                """), {
                    'username': username,
                    'login_type': login_type,
                    'login_result': login_result,
                    'failure_reason': failure_reason,
                    'ip_address': ip_address,
                    'user_agent': user_agent
                })
                conn.commit()
        except Exception as e:
            print(f"登录日志记录失败: {e}")
    
    def log_logout(self, username: str, ip_address: str = None):
        """记录登出日志"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO sys_login_logs 
                    (username, login_type, login_result, ip_address, created_at)
                    VALUES 
                    (:username, 'logout', 'success', :ip_address, NOW())
                """), {
                    'username': username,
                    'ip_address': ip_address
                })
                conn.commit()
        except Exception as e:
            print(f"登出日志记录失败: {e}")
    
    def get_audit_logs(self, 
                       username: str = None,
                       action_type: str = None,
                       module: str = None,
                       start_date: datetime = None,
                       end_date: datetime = None,
                       result_status: str = None,
                       limit: int = 100,
                       offset: int = 0) -> List[Dict[str, Any]]:
        """
        查询审计日志
        """
        try:
            conditions = []
            params = {'limit': limit, 'offset': offset}
            
            if username:
                conditions.append("username = :username")
                params['username'] = username
            if action_type:
                conditions.append("action_type = :action_type")
                params['action_type'] = action_type
            if module:
                conditions.append("module = :module")
                params['module'] = module
            if start_date:
                conditions.append("created_at >= :start_date")
                params['start_date'] = start_date
            if end_date:
                conditions.append("created_at <= :end_date")
                params['end_date'] = end_date
            if result_status:
                conditions.append("result_status = :result_status")
                params['result_status'] = result_status
            
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT id, username, action_type, action_desc, module, target_table,
                           target_id, request_params, result_status, error_message,
                           affected_rows, execution_time_ms, ip_address, created_at
                    FROM sys_audit_logs
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """), params)
                
                logs = []
                for row in result:
                    logs.append({
                        'id': row[0],
                        'username': row[1],
                        'action_type': row[2],
                        'action_desc': row[3],
                        'module': row[4],
                        'target_table': row[5],
                        'target_id': row[6],
                        'request_params': row[7],
                        'result_status': row[8],
                        'error_message': row[9],
                        'affected_rows': row[10],
                        'execution_time_ms': row[11],
                        'ip_address': row[12],
                        'created_at': row[13]
                    })
                return logs
        except Exception as e:
            print(f"查询审计日志失败: {e}")
            return []
    
    def get_login_logs(self, username: str = None, days: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
        """查询登录日志"""
        try:
            conditions = ["created_at >= :start_date"]
            params = {
                'start_date': datetime.now() - timedelta(days=days),
                'limit': limit
            }
            
            if username:
                conditions.append("username = :username")
                params['username'] = username
            
            where_clause = " WHERE " + " AND ".join(conditions)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT id, username, login_type, login_result, failure_reason,
                           ip_address, user_agent, created_at
                    FROM sys_login_logs
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT :limit
                """), params)
                
                logs = []
                for row in result:
                    logs.append({
                        'id': row[0],
                        'username': row[1],
                        'login_type': row[2],
                        'login_result': row[3],
                        'failure_reason': row[4],
                        'ip_address': row[5],
                        'user_agent': row[6],
                        'created_at': row[7]
                    })
                return logs
        except Exception as e:
            print(f"查询登录日志失败: {e}")
            return []
    
    def get_audit_statistics(self, days: int = 7) -> Dict[str, Any]:
        """获取审计统计信息"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            
            with self.engine.connect() as conn:
                # 操作统计
                result = conn.execute(text("""
                    SELECT action_type, COUNT(*) as count
                    FROM sys_audit_logs
                    WHERE created_at >= :start_date
                    GROUP BY action_type
                    ORDER BY count DESC
                """), {'start_date': start_date})
                action_stats = {row[0]: row[1] for row in result}
                
                # 用户活跃度
                result = conn.execute(text("""
                    SELECT username, COUNT(*) as count
                    FROM sys_audit_logs
                    WHERE created_at >= :start_date
                    GROUP BY username
                    ORDER BY count DESC
                    LIMIT 10
                """), {'start_date': start_date})
                user_stats = {row[0]: row[1] for row in result}
                
                # 登录统计
                result = conn.execute(text("""
                    SELECT login_result, COUNT(*) as count
                    FROM sys_login_logs
                    WHERE created_at >= :start_date
                    GROUP BY login_result
                """), {'start_date': start_date})
                login_stats = {row[0]: row[1] for row in result}
                
                return {
                    'action_stats': action_stats,
                    'user_stats': user_stats,
                    'login_stats': login_stats,
                    'period_days': days
                }
        except Exception as e:
            print(f"获取审计统计失败: {e}")
            return {}
    
    def count_audit_logs(self, **filters) -> int:
        """统计审计日志数量"""
        try:
            conditions = []
            params = {}
            
            if filters.get('username'):
                conditions.append("username = :username")
                params['username'] = filters['username']
            if filters.get('action_type'):
                conditions.append("action_type = :action_type")
                params['action_type'] = filters['action_type']
            if filters.get('module'):
                conditions.append("module = :module")
                params['module'] = filters['module']
            
            where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM sys_audit_logs {where_clause}"), params)
                return result.scalar()
        except Exception as e:
            print(f"统计审计日志失败: {e}")
            return 0


# 全局审计服务实例（便于在各处调用）
_audit_service = None

def get_audit_service() -> AuditService:
    """获取全局审计服务实例"""
    global _audit_service
    if _audit_service is None:
        _audit_service = AuditService()
    return _audit_service


def audit_log(username: str, action_type: str, action_desc: str, module: str, **kwargs):
    """便捷函数：记录审计日志"""
    get_audit_service().log_action(username, action_type, action_desc, module, **kwargs)
