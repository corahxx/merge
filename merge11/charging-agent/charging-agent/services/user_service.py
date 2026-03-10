# services/user_service.py - 用户服务

from typing import Optional, Dict, List, Any
from datetime import datetime
from sqlalchemy import text
from utils.db_utils import get_shared_engine


# 延迟导入密码工具，避免循环导入
def _get_password_utils():
    from auth.password_utils import hash_password, verify_password, validate_password_strength
    return hash_password, verify_password, validate_password_strength


class UserService:
    """用户服务"""
    
    def __init__(self):
        self.engine = get_shared_engine()
    
    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        根据用户名获取用户信息
        :param username: 用户名
        :return: 用户信息字典或None
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, username, password_hash, display_name, email, phone,
                           role, status, last_login, login_count, created_at
                    FROM sys_users 
                    WHERE username = :username
                """), {'username': username})
                row = result.fetchone()
                
                if row:
                    return {
                        'id': row[0],
                        'username': row[1],
                        'password_hash': row[2],
                        'display_name': row[3] or row[1],
                        'email': row[4],
                        'phone': row[5],
                        'role': row[6],
                        'status': row[7],
                        'last_login': row[8],
                        'login_count': row[9],
                        'created_at': row[10]
                    }
                return None
        except Exception as e:
            print(f"获取用户信息失败: {e}")
            return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """根据ID获取用户信息"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, username, display_name, email, phone,
                           role, status, last_login, login_count, created_at
                    FROM sys_users 
                    WHERE id = :user_id
                """), {'user_id': user_id})
                row = result.fetchone()
                
                if row:
                    return {
                        'id': row[0],
                        'username': row[1],
                        'display_name': row[2] or row[1],
                        'email': row[3],
                        'phone': row[4],
                        'role': row[5],
                        'status': row[6],
                        'last_login': row[7],
                        'login_count': row[8],
                        'created_at': row[9]
                    }
                return None
        except Exception as e:
            print(f"获取用户信息失败: {e}")
            return None
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """获取所有用户列表"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT id, username, display_name, email, phone,
                           role, status, last_login, login_count, created_at
                    FROM sys_users 
                    ORDER BY created_at DESC
                """))
                users = []
                for row in result:
                    users.append({
                        'id': row[0],
                        'username': row[1],
                        'display_name': row[2] or row[1],
                        'email': row[3],
                        'phone': row[4],
                        'role': row[5],
                        'status': row[6],
                        'last_login': row[7],
                        'login_count': row[8],
                        'created_at': row[9]
                    })
                return users
        except Exception as e:
            print(f"获取用户列表失败: {e}")
            return []
    
    def create_user(self, username: str, password: str, display_name: str = None,
                    email: str = None, phone: str = None, role: str = 'viewer',
                    created_by: str = None) -> Dict[str, Any]:
        """
        创建新用户
        :return: {'success': bool, 'message': str, 'user_id': int}
        """
        hash_password, verify_password, validate_password_strength = _get_password_utils()
        
        # 验证密码强度
        is_valid, msg = validate_password_strength(password)
        if not is_valid:
            return {'success': False, 'message': msg}
        
        # 检查用户名是否已存在
        if self.get_user_by_username(username):
            return {'success': False, 'message': '用户名已存在'}
        
        try:
            password_hash = hash_password(password)
            
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO sys_users 
                    (username, password_hash, display_name, email, phone, role, status, created_by)
                    VALUES (:username, :password_hash, :display_name, :email, :phone, :role, 'active', :created_by)
                """), {
                    'username': username,
                    'password_hash': password_hash,
                    'display_name': display_name or username,
                    'email': email,
                    'phone': phone,
                    'role': role,
                    'created_by': created_by
                })
                conn.commit()
                
                return {
                    'success': True,
                    'message': '用户创建成功',
                    'user_id': result.lastrowid
                }
        except Exception as e:
            return {'success': False, 'message': f'创建用户失败: {str(e)}'}
    
    def update_user(self, user_id: int, display_name: str = None, email: str = None,
                    phone: str = None, role: str = None, status: str = None) -> Dict[str, Any]:
        """更新用户信息"""
        try:
            updates = []
            params = {'user_id': user_id}
            
            if display_name is not None:
                updates.append("display_name = :display_name")
                params['display_name'] = display_name
            if email is not None:
                updates.append("email = :email")
                params['email'] = email
            if phone is not None:
                updates.append("phone = :phone")
                params['phone'] = phone
            if role is not None:
                updates.append("role = :role")
                params['role'] = role
            if status is not None:
                updates.append("status = :status")
                params['status'] = status
            
            if not updates:
                return {'success': False, 'message': '没有要更新的内容'}
            
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE sys_users SET {', '.join(updates)}
                    WHERE id = :user_id
                """), params)
                conn.commit()
                
                return {'success': True, 'message': '用户信息更新成功'}
        except Exception as e:
            return {'success': False, 'message': f'更新用户失败: {str(e)}'}
    
    def reset_password(self, user_id: int, new_password: str) -> Dict[str, Any]:
        """重置用户密码"""
        hash_password, _, validate_password_strength = _get_password_utils()
        
        is_valid, msg = validate_password_strength(new_password)
        if not is_valid:
            return {'success': False, 'message': msg}
        
        try:
            password_hash = hash_password(new_password)
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE sys_users SET password_hash = :password_hash
                    WHERE id = :user_id
                """), {'user_id': user_id, 'password_hash': password_hash})
                conn.commit()
                
                return {'success': True, 'message': '密码重置成功'}
        except Exception as e:
            return {'success': False, 'message': f'重置密码失败: {str(e)}'}
    
    def change_password(self, user_id: int, old_password: str, new_password: str) -> Dict[str, Any]:
        """修改密码（需验证旧密码）"""
        _, verify_password, _ = _get_password_utils()
        
        user = self.get_user_by_id(user_id)
        if not user:
            return {'success': False, 'message': '用户不存在'}
        
        # 获取完整用户信息（包含密码哈希）
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT password_hash FROM sys_users WHERE id = :user_id"
            ), {'user_id': user_id})
            row = result.fetchone()
            if not row:
                return {'success': False, 'message': '用户不存在'}
            
            if not verify_password(old_password, row[0]):
                return {'success': False, 'message': '原密码错误'}
        
        return self.reset_password(user_id, new_password)
    
    def update_last_login(self, username: str):
        """更新最后登录时间和登录次数"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    UPDATE sys_users 
                    SET last_login = NOW(), login_count = login_count + 1
                    WHERE username = :username
                """), {'username': username})
                conn.commit()
        except Exception as e:
            print(f"更新登录时间失败: {e}")
    
    def delete_user(self, user_id: int) -> Dict[str, Any]:
        """删除用户（建议改为禁用）"""
        try:
            with self.engine.connect() as conn:
                # 检查是否是最后一个管理员
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM sys_users WHERE role = 'admin' AND id != :user_id
                """), {'user_id': user_id})
                admin_count = result.scalar()
                
                # 获取要删除的用户角色
                result = conn.execute(text(
                    "SELECT role FROM sys_users WHERE id = :user_id"
                ), {'user_id': user_id})
                row = result.fetchone()
                
                if row and row[0] == 'admin' and admin_count == 0:
                    return {'success': False, 'message': '不能删除最后一个管理员账号'}
                
                conn.execute(text("DELETE FROM sys_users WHERE id = :user_id"), {'user_id': user_id})
                conn.commit()
                
                return {'success': True, 'message': '用户删除成功'}
        except Exception as e:
            return {'success': False, 'message': f'删除用户失败: {str(e)}'}
    
    def get_user_count_by_role(self) -> Dict[str, int]:
        """统计各角色用户数量"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT role, COUNT(*) as count FROM sys_users GROUP BY role
                """))
                return {row[0]: row[1] for row in result}
        except Exception as e:
            print(f"统计用户数量失败: {e}")
            return {}
