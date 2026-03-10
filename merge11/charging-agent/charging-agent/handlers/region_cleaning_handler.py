# handlers/region_cleaning_handler.py - 区域编码清洗处理器

"""
区域编码清洗处理器
专注于省/市/区县编码的标准化清洗

功能：
1. 城市编码修复 - 最后两位非00的城市代码
2. 区县编码修复 - 与城市归属不一致的区县代码
3. 备份与回滚 - 支持修复前备份和一键回滚
"""

import re
import json
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy import text
from utils.db_utils import get_shared_engine
from data.error_handler import ErrorHandler, logger


class RegionCleaningHandler:
    """区域编码清洗处理器"""
    
    # 直辖市代码映射
    DIRECT_CITY_CODES = {
        '11': ('110100', '北京市'),
        '12': ('120100', '天津市'),
        '31': ('310100', '上海市'),
        '50': ('500100', '重庆市'),
    }
    
    # 直辖市省份代码
    DIRECT_PROVINCE_CODES = {'11', '12', '31', '50'}
    
    def __init__(self, table_name: str = 'evdata'):
        """
        初始化处理器
        
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self._engine = None
        self._province_mapping = {}  # 省份代码 -> 名称
        self._city_mapping = {}      # 城市代码 -> 名称
        self._district_mapping = {}  # 区县代码 -> 名称
        self._reverse_city_mapping = {}  # 城市名称 -> 代码
        self._reverse_district_mapping = {}  # 区县名称 -> 代码列表
        self._load_mappings()
    
    @property
    def engine(self):
        """获取数据库引擎"""
        if self._engine is None:
            self._engine = get_shared_engine()
        return self._engine
    
    def _load_mappings(self):
        """加载区域代码映射字典"""
        try:
            mapping_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'data', 'region_code_mapping.json'
            )
            
            with open(mapping_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 使用中文键名
            self._province_mapping = data.get('省份映射', {})
            self._city_mapping = data.get('城市映射', {})
            self._district_mapping = data.get('区县映射', {})
            
            # 构建反向映射
            for code, name in self._city_mapping.items():
                self._reverse_city_mapping[name] = code
            
            for code, name in self._district_mapping.items():
                if name not in self._reverse_district_mapping:
                    self._reverse_district_mapping[name] = []
                self._reverse_district_mapping[name].append(code)
            
            logger.info(f"加载区域映射: 省份{len(self._province_mapping)}个, "
                       f"城市{len(self._city_mapping)}个, "
                       f"区县{len(self._district_mapping)}个")
        except Exception as e:
            logger.error(f"加载区域映射失败: {e}")
            raise
    
    # ========== 城市编码修复 ==========
    
    def scan_city_code_issues(self) -> Dict:
        """
        扫描城市编码异常（最后两位非00）
        
        :return: {
            'total_records': int,
            'abnormal_count': int,
            'by_province': List[Dict],
            'by_strategy': Dict,
            'sample_records': List[Dict]
        }
        """
        try:
            with self.engine.connect() as conn:
                # 1. 总记录数
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name} WHERE is_active = 1
                """))
                total = result.scalar()
                
                # 2. 异常记录数（城市代码最后两位非00）
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
                """))
                abnormal_count = result.scalar()
                
                # 3. 按省份分布
                result = conn.execute(text(f"""
                    SELECT 省份_中文, COUNT(*) as cnt
                    FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
                    GROUP BY 省份_中文
                    ORDER BY cnt DESC
                    LIMIT 15
                """))
                by_province = [
                    {'province': row[0] or '未知', 'count': row[1]}
                    for row in result.fetchall()
                ]
                
                # 4. 按修复策略预估
                by_strategy = self._estimate_city_fix_strategies(conn)
                
                # 5. 抽样记录
                result = conn.execute(text(f"""
                    SELECT UID, 省份, 省份_中文, 城市, 城市_中文, 区县, 区县_中文,
                           LEFT(充电站位置, 60) as address
                    FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
                    LIMIT 20
                """))
                sample_records = [
                    {
                        'uid': row[0],
                        'province_code': row[1],
                        'province_name': row[2],
                        'city_code': row[3],
                        'city_name': row[4],
                        'district_code': row[5],
                        'district_name': row[6],
                        'address': row[7]
                    }
                    for row in result.fetchall()
                ]
                
                return {
                    'total_records': total,
                    'abnormal_count': abnormal_count,
                    'abnormal_percent': round(abnormal_count * 100 / total, 2) if total else 0,
                    'by_province': by_province,
                    'by_strategy': by_strategy,
                    'sample_records': sample_records
                }
        except Exception as e:
            logger.error(f"扫描城市编码异常失败: {e}")
            raise
    
    def _estimate_city_fix_strategies(self, conn) -> Dict:
        """预估各修复策略的覆盖率"""
        strategies = {
            'direct_city': 0,      # 直辖市规则
            'from_district': 0,    # 从区县推导
            'from_current': 0,     # 从当前城市推导
            'manual_required': 0   # 待人工处理
        }
        
        # 直辖市记录数
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE is_active = 1
            AND 城市 IS NOT NULL AND 城市 != ''
            AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
            AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
            AND LEFT(CAST(省份 AS CHAR), 2) IN ('11', '12', '31', '50')
        """))
        strategies['direct_city'] = result.scalar() or 0
        
        # 有有效区县代码的记录（可从区县推导）
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE is_active = 1
            AND 城市 IS NOT NULL AND 城市 != ''
            AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
            AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
            AND 区县 IS NOT NULL AND 区县 != ''
            AND LENGTH(CAST(区县 AS CHAR)) >= 4
            AND LEFT(CAST(省份 AS CHAR), 2) NOT IN ('11', '12', '31', '50')
        """))
        strategies['from_district'] = result.scalar() or 0
        
        # 总异常数
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE is_active = 1
            AND 城市 IS NOT NULL AND 城市 != ''
            AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
            AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
        """))
        total_abnormal = result.scalar() or 0
        
        # 剩余的归类为从当前推导或待人工
        remaining = total_abnormal - strategies['direct_city'] - strategies['from_district']
        strategies['from_current'] = max(0, int(remaining * 0.9))
        strategies['manual_required'] = max(0, remaining - strategies['from_current'])
        
        return strategies
    
    def derive_city_code(self, record: Dict) -> Tuple[Optional[str], Optional[str], str, float]:
        """
        推导正确的城市代码
        
        :param record: 记录字典，包含 城市、区县、省份、充电站位置 等字段
        :return: (new_code, new_name, strategy, confidence)
        """
        current_city = str(record.get('城市', '') or '').strip()
        district_code = str(record.get('区县', '') or '').strip()
        province_code = str(record.get('省份', '') or '').strip()
        
        # 获取省份前缀
        province_prefix = ''
        if province_code and len(str(province_code)) >= 2:
            province_prefix = str(province_code)[:2]
        elif current_city and len(str(current_city)) >= 2:
            province_prefix = str(current_city)[:2]
        
        if not province_prefix:
            return None, None, 'NO_PREFIX', 0.0
        
        # ========== 策略1: 直辖市规则 ==========
        if province_prefix in self.DIRECT_CITY_CODES:
            code, name = self.DIRECT_CITY_CODES[province_prefix]
            return code, name, 'DIRECT_CITY', 1.0
        
        # ========== 策略2: 从区县代码推导 ==========
        if district_code and len(str(district_code)) >= 4:
            district_str = str(district_code)
            derived = district_str[:4] + '00'
            if derived in self._city_mapping:
                # 校验: 区县前2位应与省份前2位一致
                if district_str[:2] == province_prefix:
                    return derived, self._city_mapping[derived], 'FROM_DISTRICT', 0.95
        
        # ========== 策略3: 从当前城市代码推导 ==========
        if current_city and len(str(current_city)) >= 4:
            city_str = str(current_city)
            derived = city_str[:4] + '00'
            if derived in self._city_mapping:
                # 校验: 城市前2位应与省份前2位一致
                if city_str[:2] == province_prefix:
                    return derived, self._city_mapping[derived], 'FROM_CURRENT', 0.85
        
        # ========== 无法自动推导 ==========
        return None, None, 'MANUAL_REQUIRED', 0.0
    
    def get_city_fix_preview(self, page: int = 1, page_size: int = 20) -> Dict:
        """
        获取城市编码修复预览（分页）
        
        :return: {
            'data': List[Dict],
            'total': int,
            'page': int,
            'page_size': int,
            'total_pages': int
        }
        """
        try:
            offset = (page - 1) * page_size
            
            with self.engine.connect() as conn:
                # 总数
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
                """))
                total = result.scalar()
                
                # 分页数据
                result = conn.execute(text(f"""
                    SELECT UID, 省份, 省份_中文, 城市, 城市_中文, 区县, 区县_中文,
                           LEFT(充电站位置, 50) as address
                    FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND RIGHT(CAST(城市 AS CHAR), 2) != '00'
                    ORDER BY 省份_中文, 城市
                    LIMIT :limit OFFSET :offset
                """), {'limit': page_size, 'offset': offset})
                
                data = []
                for row in result.fetchall():
                    record = {
                        'uid': row[0],
                        'province_code': row[1],
                        'province_name': row[2],
                        'city_code': row[3],
                        'city_name': row[4],
                        'district_code': row[5],
                        'district_name': row[6],
                        'address': row[7],
                        '省份': row[1],
                        '城市': row[3],
                        '区县': row[5]
                    }
                    
                    # 推导修复方案
                    new_code, new_name, strategy, confidence = self.derive_city_code(record)
                    record['new_city_code'] = new_code
                    record['new_city_name'] = new_name
                    record['strategy'] = strategy
                    record['confidence'] = confidence
                    data.append(record)
                
                return {
                    'data': data,
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'total_pages': (total + page_size - 1) // page_size
                }
        except Exception as e:
            logger.error(f"获取城市修复预览失败: {e}")
            raise
    
    # ========== 区县编码修复 ==========
    
    def scan_district_code_issues(self) -> Dict:
        """
        扫描区县编码异常（与城市前4位不匹配）
        
        :return: 类似 scan_city_code_issues
        """
        try:
            with self.engine.connect() as conn:
                # 总记录数
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name} WHERE is_active = 1
                """))
                total = result.scalar()
                
                # 区县与城市前4位不匹配的记录数
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name}
                    WHERE is_active = 1
                    AND 区县 IS NOT NULL AND 区县 != ''
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(区县 AS CHAR) REGEXP '^[0-9]+$'
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND LEFT(CAST(区县 AS CHAR), 4) != LEFT(CAST(城市 AS CHAR), 4)
                """))
                abnormal_count = result.scalar()
                
                # 按省份分布
                result = conn.execute(text(f"""
                    SELECT 省份_中文, COUNT(*) as cnt
                    FROM {self.table_name}
                    WHERE is_active = 1
                    AND 区县 IS NOT NULL AND 区县 != ''
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(区县 AS CHAR) REGEXP '^[0-9]+$'
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND LEFT(CAST(区县 AS CHAR), 4) != LEFT(CAST(城市 AS CHAR), 4)
                    GROUP BY 省份_中文
                    ORDER BY cnt DESC
                    LIMIT 15
                """))
                by_province = [
                    {'province': row[0] or '未知', 'count': row[1]}
                    for row in result.fetchall()
                ]
                
                # 地址中包含区县名的记录数（可验证）
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name}
                    WHERE is_active = 1
                    AND 区县 IS NOT NULL AND 区县 != ''
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND CAST(区县 AS CHAR) REGEXP '^[0-9]+$'
                    AND CAST(城市 AS CHAR) REGEXP '^[0-9]+$'
                    AND LEFT(CAST(区县 AS CHAR), 4) != LEFT(CAST(城市 AS CHAR), 4)
                    AND 充电站位置 IS NOT NULL AND 充电站位置 != ''
                    AND 充电站位置 LIKE CONCAT('%', LEFT(区县_中文, 2), '%')
                """))
                address_verifiable = result.scalar()
                
                return {
                    'total_records': total,
                    'abnormal_count': abnormal_count,
                    'abnormal_percent': round(abnormal_count * 100 / total, 2) if total else 0,
                    'by_province': by_province,
                    'address_verifiable': address_verifiable,
                    'address_verifiable_percent': round(address_verifiable * 100 / abnormal_count, 1) if abnormal_count else 0
                }
        except Exception as e:
            logger.error(f"扫描区县编码异常失败: {e}")
            raise
    
    # ========== 备份与修复执行 ==========
    
    def create_backup_table(self) -> bool:
        """创建备份表（如果不存在）"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS evdata_region_fix_backup (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        backup_id VARCHAR(50) NOT NULL,
                        uid VARCHAR(100) NOT NULL,
                        fix_type ENUM('city', 'district', 'province') NOT NULL,
                        old_code VARCHAR(20),
                        old_name VARCHAR(100),
                        new_code VARCHAR(20),
                        new_name VARCHAR(100),
                        strategy VARCHAR(30),
                        confidence DECIMAL(3,2),
                        fixed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        fixed_by VARCHAR(50),
                        rollback_at DATETIME DEFAULT NULL,
                        INDEX idx_backup_id (backup_id),
                        INDEX idx_uid (uid),
                        INDEX idx_fixed_at (fixed_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"创建备份表失败: {e}")
            return False
    
    def fix_city_codes(self, dry_run: bool = True, batch_size: int = 50,
                       progress_callback=None, operator: str = 'system') -> Dict:
        """
        流式批量修复城市编码 - 分批查询，实时更新
        
        :param dry_run: True=预览模式，不实际修改
        :param batch_size: 每批查询和处理数量（默认50条）
        :param progress_callback: 进度回调函数 (progress_info: dict)
        :param operator: 操作人
        :return: 修复结果统计
        """
        import time
        
        # 确保备份表存在
        self.create_backup_table()
        
        backup_id = f"city_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        stats = {
            'backup_id': backup_id,
            'dry_run': dry_run,
            'total': 0,
            'success': 0,
            'skip': 0,
            'fail': 0,
            'by_strategy': {
                'DIRECT_CITY': 0,
                'FROM_DISTRICT': 0,
                'FROM_CURRENT': 0,
                'MANUAL_REQUIRED': 0
            },
            'manual_records': []  # 需要人工处理的记录
        }
        
        try:
            start_time = time.time()
            
            # 报告开始
            if progress_callback:
                progress_callback({
                    'current': 0, 'total': 1, 'processed': 0, 'remaining': 1,
                    'percent': 0, 'elapsed': 0, 'eta': None,
                    'message': "正在统计异常记录..."
                })
                time.sleep(0.05)
            
            with self.engine.connect() as conn:
                # 第一步：快速统计数量
                count_result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.table_name}
                    WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    AND 城市 REGEXP '^[0-9]+$'
                    AND RIGHT(城市, 2) != '00'
                """))
                total_count = count_result.scalar()
                stats['total'] = total_count
                
                logger.info(f"发现 {total_count} 条城市编码异常")
                
                if progress_callback:
                    progress_callback({
                        'current': 0, 'total': total_count, 'processed': 0,
                        'remaining': total_count, 'percent': 0,
                        'elapsed': time.time() - start_time, 'eta': None,
                        'message': f"发现 {total_count:,} 条异常，开始流式处理..."
                    })
                    time.sleep(0.05)
                
                # 第二步：流式分批处理（使用 LIMIT/OFFSET）
                offset = 0
                processed = 0
                
                while offset < total_count:
                    # 分批查询
                    batch_result = conn.execute(text(f"""
                        SELECT UID, 省份, 省份_中文, 城市, 城市_中文, 区县, 区县_中文, 充电站位置
                        FROM {self.table_name}
                        WHERE is_active = 1
                        AND 城市 IS NOT NULL AND 城市 != ''
                        AND 城市 REGEXP '^[0-9]+$'
                        AND RIGHT(城市, 2) != '00'
                        LIMIT {batch_size} OFFSET {offset}
                    """))
                    rows = batch_result.fetchall()
                    
                    if not rows:
                        break
                    
                    # 处理这一批
                    fix_batch = []
                    for row in rows:
                        record = {
                            'uid': row[0],
                            '省份': row[1],
                            '省份_中文': row[2],
                            '城市': row[3],
                            '城市_中文': row[4],
                            '区县': row[5],
                            '区县_中文': row[6],
                            '充电站位置': row[7]
                        }
                        
                        new_code, new_name, strategy, confidence = self.derive_city_code(record)
                        stats['by_strategy'][strategy] = stats['by_strategy'].get(strategy, 0) + 1
                        
                        if new_code and confidence >= 0.7:
                            fix_batch.append({
                                'uid': record['uid'],
                                'old_code': record['城市'],
                                'old_name': record['城市_中文'],
                                'new_code': new_code,
                                'new_name': new_name,
                                'strategy': strategy,
                                'confidence': confidence
                            })
                        else:
                            stats['skip'] += 1
                            if len(stats['manual_records']) < 200:
                                stats['manual_records'].append({
                                    'uid': record['uid'],
                                    '省份': record['省份_中文'] or record['省份'],
                                    '城市编码': record['城市'],
                                    '城市名称': record['城市_中文'],
                                    '区县': record['区县_中文'] or record['区县'],
                                    '位置': (record['充电站位置'] or '')[:30],
                                    '原因': strategy
                                })
                    
                    # 执行修复
                    if fix_batch:
                        if not dry_run:
                            success = self._execute_city_fix_batch(conn, fix_batch, backup_id, operator)
                            stats['success'] += success
                            stats['fail'] += len(fix_batch) - success
                        else:
                            stats['success'] += len(fix_batch)
                    
                    # 更新进度
                    offset += len(rows)
                    processed = offset
                    
                    if progress_callback:
                        elapsed = time.time() - start_time
                        remaining = total_count - processed
                        percent = processed / total_count * 100 if total_count else 0
                        speed = processed / elapsed if elapsed > 0 else 0
                        eta = remaining / speed if speed > 0 else None
                        
                        progress_callback({
                            'current': processed, 
                            'total': total_count,
                            'processed': processed,
                            'remaining': remaining,
                            'percent': percent,
                            'elapsed': elapsed,
                            'eta': eta,
                            'speed': speed,
                            'message': f"已处理 {processed:,}/{total_count:,} | 修复 {stats['success']:,} | 跳过 {stats['skip']:,}"
                        })
                        time.sleep(0.02)  # 让UI刷新
                    
                    # 每批提交
                    if not dry_run:
                        conn.commit()
                
                # 完成
                elapsed = time.time() - start_time
                if progress_callback:
                    progress_callback({
                        'current': total_count, 
                        'total': total_count,
                        'processed': total_count,
                        'remaining': 0,
                        'percent': 100,
                        'elapsed': elapsed,
                        'eta': 0,
                        'message': f"✅ 完成! 修复 {stats['success']:,} 条，跳过 {stats['skip']:,} 条，耗时 {elapsed:.1f}秒"
                    })
                
                stats['elapsed_seconds'] = elapsed
                logger.info(f"城市编码修复完成: 成功 {stats['success']}, 跳过 {stats['skip']}, 失败 {stats['fail']}")
                return stats
                
        except Exception as e:
            logger.error(f"修复城市编码失败: {e}")
            stats['error'] = str(e)
            return stats
    
    def _execute_city_fix_batch(self, conn, batch: List[Dict], 
                                 backup_id: str, operator: str) -> int:
        """执行一批城市编码修复"""
        success = 0
        
        for item in batch:
            try:
                # 1. 插入备份记录
                conn.execute(text("""
                    INSERT INTO evdata_region_fix_backup 
                    (backup_id, uid, fix_type, old_code, old_name, new_code, new_name, 
                     strategy, confidence, fixed_by)
                    VALUES (:backup_id, :uid, 'city', :old_code, :old_name, 
                            :new_code, :new_name, :strategy, :confidence, :fixed_by)
                """), {
                    'backup_id': backup_id,
                    'uid': item['uid'],
                    'old_code': item['old_code'],
                    'old_name': item['old_name'],
                    'new_code': item['new_code'],
                    'new_name': item['new_name'],
                    'strategy': item['strategy'],
                    'confidence': item['confidence'],
                    'fixed_by': operator
                })
                
                # 2. 更新数据
                conn.execute(text(f"""
                    UPDATE {self.table_name}
                    SET 城市 = :new_code,
                        城市_中文 = :new_name,
                        status_note = CONCAT(IFNULL(status_note, ''), ' | 城市编码修复'),
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid AND is_active = 1
                """), {
                    'new_code': item['new_code'],
                    'new_name': item['new_name'],
                    'operator': operator,
                    'uid': item['uid']
                })
                
                success += 1
            except Exception as e:
                logger.error(f"修复记录 {item['uid']} 失败: {e}")
        
        return success
    
    def rollback_fix(self, backup_id: str) -> Dict:
        """
        回滚指定批次的修复
        
        :param backup_id: 备份批次ID
        :return: 回滚结果
        """
        try:
            with self.engine.connect() as conn:
                # 查询备份记录
                result = conn.execute(text("""
                    SELECT uid, fix_type, old_code, old_name
                    FROM evdata_region_fix_backup
                    WHERE backup_id = :backup_id AND rollback_at IS NULL
                """), {'backup_id': backup_id})
                records = result.fetchall()
                
                if not records:
                    return {'success': False, 'message': '未找到可回滚的记录'}
                
                success = 0
                for row in records:
                    uid, fix_type, old_code, old_name = row
                    
                    if fix_type == 'city':
                        conn.execute(text(f"""
                            UPDATE {self.table_name}
                            SET 城市 = :old_code, 城市_中文 = :old_name
                            WHERE UID = :uid
                        """), {'old_code': old_code, 'old_name': old_name, 'uid': uid})
                    elif fix_type == 'district':
                        conn.execute(text(f"""
                            UPDATE {self.table_name}
                            SET 区县 = :old_code, 区县_中文 = :old_name
                            WHERE UID = :uid
                        """), {'old_code': old_code, 'old_name': old_name, 'uid': uid})
                    
                    success += 1
                
                # 标记已回滚
                conn.execute(text("""
                    UPDATE evdata_region_fix_backup
                    SET rollback_at = NOW()
                    WHERE backup_id = :backup_id
                """), {'backup_id': backup_id})
                
                conn.commit()
                
                return {
                    'success': True,
                    'rollback_count': success,
                    'message': f'成功回滚 {success} 条记录'
                }
        except Exception as e:
            logger.error(f"回滚失败: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_backup_list(self) -> List[Dict]:
        """获取备份列表"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        backup_id,
                        fix_type,
                        COUNT(*) as record_count,
                        MIN(fixed_at) as fixed_at,
                        fixed_by,
                        MAX(rollback_at) as rollback_at
                    FROM evdata_region_fix_backup
                    GROUP BY backup_id, fix_type, fixed_by
                    ORDER BY fixed_at DESC
                    LIMIT 50
                """))
                
                return [
                    {
                        'backup_id': row[0],
                        'fix_type': row[1],
                        'record_count': row[2],
                        'fixed_at': row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
                        'fixed_by': row[4],
                        'rollback_at': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else None,
                        'can_rollback': row[5] is None
                    }
                    for row in result.fetchall()
                ]
        except Exception as e:
            logger.error(f"获取备份列表失败: {e}")
            return []
    
    # ========== 区域名称同步（以JSON为准） ==========
    
    def scan_name_sync_issues(self) -> Dict:
        """
        扫描区域名称与JSON不一致的问题
        
        :return: {
            'province_issues': List[Dict],
            'city_issues': List[Dict],
            'district_issues': List[Dict],
            'total_affected': int
        }
        """
        try:
            with self.engine.connect() as conn:
                # ========== 省份名称检查 ==========
                result = conn.execute(text(f"""
                    SELECT DISTINCT 省份, 省份_中文, COUNT(*) as cnt
                    FROM {self.table_name} WHERE is_active = 1
                    AND 省份 IS NOT NULL AND 省份 != ''
                    GROUP BY 省份, 省份_中文
                    ORDER BY cnt DESC
                """))
                
                province_issues = []
                for row in result.fetchall():
                    code = str(row[0]).strip()
                    db_name = row[1]
                    count = row[2]
                    
                    # 标准化代码（补齐6位）
                    code_norm = self._normalize_code(code, 6)
                    json_name = self._province_mapping.get(code_norm)
                    
                    if json_name and json_name != db_name:
                        province_issues.append({
                            'code': code,
                            'db_name': db_name,
                            'json_name': json_name,
                            'count': count
                        })
                
                # ========== 城市名称检查 ==========
                result = conn.execute(text(f"""
                    SELECT DISTINCT 城市, 城市_中文, COUNT(*) as cnt
                    FROM {self.table_name} WHERE is_active = 1
                    AND 城市 IS NOT NULL AND 城市 != ''
                    GROUP BY 城市, 城市_中文
                    ORDER BY cnt DESC
                """))
                
                city_issues = []
                for row in result.fetchall():
                    code = str(row[0]).strip()
                    db_name = row[1]
                    count = row[2]
                    
                    code_norm = self._normalize_code(code, 6)
                    json_name = self._city_mapping.get(code_norm)
                    
                    # 直辖市特殊处理：城市代码可能是 xx0100，对应省份代码 xx0000
                    if json_name is None and len(code_norm) >= 2:
                        prefix = code_norm[:2]
                        if prefix in self.DIRECT_PROVINCE_CODES:
                            province_code = prefix + '0000'
                            json_name = self._province_mapping.get(province_code)
                    
                    if json_name and json_name != db_name:
                        city_issues.append({
                            'code': code,
                            'db_name': db_name,
                            'json_name': json_name,
                            'count': count
                        })
                
                # ========== 区县名称检查 ==========
                result = conn.execute(text(f"""
                    SELECT DISTINCT 区县, 区县_中文, COUNT(*) as cnt
                    FROM {self.table_name} WHERE is_active = 1
                    AND 区县 IS NOT NULL AND 区县 != ''
                    GROUP BY 区县, 区县_中文
                    ORDER BY cnt DESC
                """))
                
                district_issues = []
                for row in result.fetchall():
                    code = str(row[0]).strip()
                    db_name = row[1]
                    count = row[2]
                    
                    code_norm = self._normalize_code(code, 6)
                    json_name = self._district_mapping.get(code_norm)
                    
                    if json_name and json_name != db_name:
                        district_issues.append({
                            'code': code,
                            'db_name': db_name,
                            'json_name': json_name,
                            'count': count
                        })
                
                total_affected = (
                    sum(p['count'] for p in province_issues) +
                    sum(c['count'] for c in city_issues) +
                    sum(d['count'] for d in district_issues)
                )
                
                return {
                    'province_issues': province_issues,
                    'city_issues': city_issues,
                    'district_issues': district_issues,
                    'province_count': len(province_issues),
                    'city_count': len(city_issues),
                    'district_count': len(district_issues),
                    'total_affected': total_affected
                }
        except Exception as e:
            logger.error(f"扫描名称同步问题失败: {e}")
            raise
    
    def _normalize_code(self, code: str, length: int = 6) -> str:
        """
        标准化区域代码（兼容Excel导入的浮点/科学计数法/混入字符）
        
        目标：返回仅数字、长度为6的字符串，尽量按行政区划规则补齐：
        - 2位：省份 → xx0000
        - 4位：城市 → xxxx00
        - <6位：左补0（兜底）
        - >6位：截断前6位
        """
        if code is None:
            return ''
        
        raw = str(code).strip()
        if raw == '' or raw.lower() in ('nan', 'none', 'null', 'nat'):
            return ''
        
        # 优先走数值解析：处理 520200.0 / 5.202e+05
        try:
            num = float(raw)
            if num == num:  # not NaN
                raw = str(int(round(num)))
        except Exception:
            pass
        
        # 兜底：只保留数字
        raw = re.sub(r'\D+', '', raw)
        if not raw:
            return ''
        
        # 按行政区划常见长度补齐
        if length == 6:
            if len(raw) == 2:
                raw = raw + '0000'
            elif len(raw) == 4:
                raw = raw + '00'
            elif len(raw) < 6:
                raw = raw.zfill(6)
            else:
                raw = raw[:6]
            return raw
        
        # 其他长度：尽量保留原逻辑（右补0/截断）
        if len(raw) < length:
            return raw.ljust(length, '0')
        return raw[:length]
    
    def fix_region_names(self, dry_run: bool = True, 
                         progress_callback=None,
                         operator: str = 'system') -> Dict:
        """
        修复区域名称（以JSON为准）
        
        :param dry_run: True=预览模式
        :param progress_callback: 进度回调 (progress_info: dict)
        :param operator: 操作人
        :return: 修复结果
        """
        import time
        
        self.create_backup_table()
        backup_id = f"name_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        stats = {
            'backup_id': backup_id,
            'dry_run': dry_run,
            'province_fixed': 0,
            'city_fixed': 0,
            'district_fixed': 0,
            'total_records': 0
        }
        
        try:
            # 获取问题列表
            issues = self.scan_name_sync_issues()
            
            with self.engine.connect() as conn:
                total_steps = (len(issues['province_issues']) + 
                              len(issues['city_issues']) + 
                              len(issues['district_issues']))
                current_step = 0
                start_time = time.time()
                
                def report_progress(step, total, msg):
                    if progress_callback:
                        elapsed = time.time() - start_time
                        speed = step / elapsed if elapsed > 0 else 0
                        remaining = total - step
                        eta = remaining / speed if speed > 0 else None
                        progress_callback({
                            'current': step,
                            'total': total,
                            'processed': step,
                            'remaining': remaining,
                            'percent': step / total * 100 if total else 0,
                            'elapsed': elapsed,
                            'eta': eta,
                            'speed': speed,
                            'message': msg
                        })
                
                report_progress(0, total_steps, "开始同步...")
                
                # ========== 修复省份名称 ==========
                for item in issues['province_issues']:
                    current_step += 1
                    report_progress(current_step, total_steps, 
                        f"修复省份: {item['json_name']} ({item['count']}条)")
                    
                    if not dry_run:
                        # 备份
                        conn.execute(text("""
                            INSERT INTO evdata_region_fix_backup
                            (backup_id, uid, fix_type, old_code, old_name, new_code, new_name, 
                             strategy, confidence, fixed_by)
                            SELECT :backup_id, UID, 'province', 省份, 省份_中文, 省份, :new_name,
                                   'NAME_SYNC', 1.0, :operator
                            FROM evdata
                            WHERE is_active = 1 AND 省份 = :code AND 省份_中文 = :old_name
                        """), {
                            'backup_id': backup_id,
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name'],
                            'operator': operator
                        })
                        
                        # 更新
                        result = conn.execute(text(f"""
                            UPDATE {self.table_name}
                            SET 省份_中文 = :new_name
                            WHERE is_active = 1 AND 省份 = :code AND 省份_中文 = :old_name
                        """), {
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name']
                        })
                        stats['province_fixed'] += result.rowcount
                    else:
                        stats['province_fixed'] += item['count']
                
                # ========== 修复城市名称 ==========
                for item in issues['city_issues']:
                    current_step += 1
                    report_progress(current_step, total_steps,
                        f"修复城市: {item['json_name']} ({item['count']}条)")
                    
                    if not dry_run:
                        conn.execute(text("""
                            INSERT INTO evdata_region_fix_backup
                            (backup_id, uid, fix_type, old_code, old_name, new_code, new_name,
                             strategy, confidence, fixed_by)
                            SELECT :backup_id, UID, 'city', 城市, 城市_中文, 城市, :new_name,
                                   'NAME_SYNC', 1.0, :operator
                            FROM evdata
                            WHERE is_active = 1 AND 城市 = :code AND 城市_中文 = :old_name
                        """), {
                            'backup_id': backup_id,
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name'],
                            'operator': operator
                        })
                        
                        result = conn.execute(text(f"""
                            UPDATE {self.table_name}
                            SET 城市_中文 = :new_name
                            WHERE is_active = 1 AND 城市 = :code AND 城市_中文 = :old_name
                        """), {
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name']
                        })
                        stats['city_fixed'] += result.rowcount
                    else:
                        stats['city_fixed'] += item['count']
                
                # ========== 修复区县名称 ==========
                for item in issues['district_issues']:
                    current_step += 1
                    report_progress(current_step, total_steps,
                        f"修复区县: {item['json_name']} ({item['count']}条)")
                    
                    if not dry_run:
                        conn.execute(text("""
                            INSERT INTO evdata_region_fix_backup
                            (backup_id, uid, fix_type, old_code, old_name, new_code, new_name,
                             strategy, confidence, fixed_by)
                            SELECT :backup_id, UID, 'district', 区县, 区县_中文, 区县, :new_name,
                                   'NAME_SYNC', 1.0, :operator
                            FROM evdata
                            WHERE is_active = 1 AND 区县 = :code AND 区县_中文 = :old_name
                        """), {
                            'backup_id': backup_id,
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name'],
                            'operator': operator
                        })
                        
                        result = conn.execute(text(f"""
                            UPDATE {self.table_name}
                            SET 区县_中文 = :new_name
                            WHERE is_active = 1 AND 区县 = :code AND 区县_中文 = :old_name
                        """), {
                            'code': item['code'],
                            'old_name': item['db_name'],
                            'new_name': item['json_name']
                        })
                        stats['district_fixed'] += result.rowcount
                    else:
                        stats['district_fixed'] += item['count']
                
                if not dry_run:
                    conn.commit()
                
                stats['total_records'] = (stats['province_fixed'] + 
                                         stats['city_fixed'] + 
                                         stats['district_fixed'])
                
                elapsed = time.time() - start_time
                stats['elapsed_seconds'] = elapsed
                
                report_progress(total_steps, total_steps, 
                    f"完成! 耗时 {elapsed:.1f}秒")
                
                return stats
        except Exception as e:
            logger.error(f"修复区域名称失败: {e}")
            stats['error'] = str(e)
            return stats
