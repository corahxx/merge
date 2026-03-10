# handlers/pile_model_query_handler.py - 充电桩型号查询处理

from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy import text
from utils.db_utils import create_db_engine
from data.error_handler import ErrorHandler


class PileModelQueryHandler:
    """处理充电桩型号查询逻辑"""
    
    def __init__(self, table_name: str = 'evdata'):
        """
        初始化充电桩型号查询处理器
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self._engine = None
    
    @property
    def engine(self):
        """获取数据库引擎（懒加载）"""
        if self._engine is None:
            self._engine = create_db_engine(echo=False)  # 使用统一工具函数
        return self._engine
    
    def get_provinces(self) -> List[str]:
        """
        获取所有省份_中文列表（去重并排序）
        :return: 省份列表
        """
        try:
            # 转义表名防止SQL注入
            escaped_table_name = self.table_name.replace("`", "``")
            
            query = text(f"""
                SELECT DISTINCT `省份_中文` 
                FROM `{escaped_table_name}` 
                WHERE `省份_中文` IS NOT NULL AND `省份_中文` != ''
                ORDER BY `省份_中文`
            """)
            df = pd.read_sql(query, self.engine)
            return df['省份_中文'].tolist()
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取省份列表")
            return []
    
    def query_by_model_and_province(
        self, 
        models: List[str], 
        provinces: Optional[List[str]] = None
    ) -> Dict:
        """
        根据充电桩型号和省份查询数据
        :param models: 充电桩型号列表（最多300个）
        :param provinces: 省份_中文列表（可选，支持多个省份）
        :return: 查询结果字典
        """
        try:
            # 验证输入
            if not models or len(models) == 0:
                return {
                    'success': False,
                    'error': '请至少输入一个充电桩型号',
                    'data': pd.DataFrame()
                }
            
            if len(models) > 300:
                return {
                    'success': False,
                    'error': '充电桩型号数量不能超过300个',
                    'data': pd.DataFrame()
                }
            
            # 清理型号列表（去除空值和空白）
            models = [m.strip() for m in models if m and m.strip()]
            
            if len(models) == 0:
                return {
                    'success': False,
                    'error': '请输入有效的充电桩型号',
                    'data': pd.DataFrame()
                }
            
            # 构建SQL查询（使用参数化查询防止SQL注入）
            # 使用IN子句查询多个型号
            # 注意：MySQL的text()不支持命名参数，所以使用字符串格式化，但已做转义处理
            escaped_models = [m.replace("'", "''").replace("\\", "\\\\") for m in models]
            placeholders = ','.join([f"'{m}'" for m in escaped_models])
            
            where_conditions = [f"`充电桩型号` IN ({placeholders})"]
            
            # 如果指定了省份，添加省份条件（支持多个省份）
            if provinces and len(provinces) > 0:
                # 清理省份列表（去除空值和空白）
                provinces = [p.strip() for p in provinces if p and p.strip()]
                if len(provinces) > 0:
                    escaped_provinces = [p.replace("'", "''").replace("\\", "\\\\") for p in provinces]
                    province_placeholders = ','.join([f"'{p}'" for p in escaped_provinces])
                    where_conditions.append(f"`省份_中文` IN ({province_placeholders})")
            
            where_clause = " AND ".join(where_conditions)
            
            # 使用text()时，表名也需要转义
            escaped_table_name = self.table_name.replace("`", "``")
            
            query = text(f"""
                SELECT * 
                FROM `{escaped_table_name}` 
                WHERE {where_clause}
                ORDER BY `充电桩型号`, `省份_中文`, `城市_中文`, `区县_中文`
            """)
            
            df = pd.read_sql(query, self.engine)
            
            # 格式化省份显示
            if provinces and len(provinces) > 0:
                province_display = ', '.join(provinces) if len(provinces) <= 3 else f"{', '.join(provinces[:3])} 等{len(provinces)}个省份"
            else:
                province_display = '全部省份'
            
            return {
                'success': True,
                'data': df,
                'row_count': len(df),
                'model_count': len(models),
                'province': province_display,
                'province_count': len(provinces) if provinces else 0
            }
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "充电桩型号查询")
            return {
                'success': False,
                'error': error_info['error_message'],
                'error_details': error_info,
                'data': pd.DataFrame()
            }

