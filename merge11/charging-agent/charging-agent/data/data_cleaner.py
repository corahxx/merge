# data/data_cleaner.py - 数据清洗器

import re
import uuid
import warnings
import pandas as pd
from typing import Dict, List, Optional, Callable
from datetime import datetime
from core.knowledge_base import KnowledgeBase
from .type_converter import TypeConverter
from .table_schema import TableSchemaDict, get_table_schema, get_varchar_lengths
from .region_code_converter import get_converter


class DataCleaner:
    """
    数据清洗器
    负责数据清洗、标准化、验证等功能
    """
    
    def __init__(
        self,
        verbose: bool = True,
        table_name: Optional[str] = None,
        table_schema: Optional[Dict] = None,
        engine=None,
        use_fixed_schema: bool = True,
        enable_strict_region_codes: bool = True,
        enable_region_hierarchy_validation: bool = True,
        enable_date_standardization: bool = True,
    ):
        """
        初始化数据清洗器
        :param verbose: 是否显示详细日志
        :param table_name: 数据库表名（用于自动获取表结构）
        :param table_schema: 数据库表结构字典（如果提供，优先使用；否则根据table_name获取）
        :param engine: SQLAlchemy engine（用于类型转换和查询表结构）
        :param use_fixed_schema: 是否优先使用固化的表结构数据（如果可用）
        """
        self.verbose = verbose
        self.table_name = table_name
        self.engine = engine
        self.use_fixed_schema = use_fixed_schema
        self.enable_strict_region_codes = enable_strict_region_codes
        self.enable_region_hierarchy_validation = enable_region_hierarchy_validation
        self.enable_date_standardization = enable_date_standardization
        
        # 初始化表结构字典
        if table_schema:
            # 如果直接提供了表结构，使用提供的
            self.table_schema = table_schema
            self.schema_dict = None
        elif table_name:
            # 如果有表名，使用TableSchemaDict来管理
            self.schema_dict = TableSchemaDict(table_name, use_fixed=use_fixed_schema)
            self.table_schema = None  # 延迟加载
        else:
            self.table_schema = None
            self.schema_dict = None
        
        self.type_converter = TypeConverter(verbose=verbose) if (table_schema or table_name) else None
        self.cleaning_stats = {
            'rows_before': 0,
            'rows_after': 0,
            'duplicates_removed': 0,
            'null_rows_removed': 0,
            'invalid_rows_removed': 0,
            'normalized_fields': 0,
            'columns_type_converted': 0,
            'strings_truncated': 0,
            'truncation_details': {},  # 记录每个字段的截断统计
            'region_fixed': {  # 区域代码修正统计
                'province_fixed': 0,
                'city_fixed': 0,
                'district_fixed': 0
            },
            'region_issues': {  # 区域编码异常统计（用于data_status标记）
                'city_code_unfixed': 0,       # data_status=5
                'district_code_invalid': 0,   # data_status=6
                'hierarchy_mismatch': 0,      # data_status=8（文档未定义，避免与日期异常7冲突）
            },
            'date_standardization': {  # 日期标准化统计
                'parsed': 0,
                'failed': 0,
            },
        }
    
    def clean(self, df: pd.DataFrame, field_mapping: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        执行完整的数据清洗流程
        :param df: 原始DataFrame
        :param field_mapping: 字段映射字典，将EXCEL列名映射到数据库字段名
        :return: 清洗后的DataFrame
        """
        # 重置统计信息（除了累积统计外）
        self.cleaning_stats['rows_before'] = len(df)
        # 注意：不重置其他统计字段，因为可能需要在多次调用间累积
        # 但在实际使用中，每次clean调用都会产生新的统计，所以这里应该重置
        # 不过，如果是在大文件分批处理中，可能需要累积统计，所以暂时不重置
        # 调用者可以通过创建新的DataCleaner实例来重置统计
        df_cleaned = df.copy()

        # 0. 生成UID主键（文档：数据导入改造可行性分析）
        df_cleaned = self._generate_uids(df_cleaned)

        # 0.5 确保状态字段存在（便于区域/日期异常标记）
        df_cleaned = self._ensure_status_columns(df_cleaned)
        
        # 1. 字段映射（重命名列）
        if field_mapping:
            df_cleaned = self._map_columns(df_cleaned, field_mapping)
        
        # 2. 去除空白
        df_cleaned = self._strip_whitespace(df_cleaned)
        
        # 3. 标准化字段值
        df_cleaned = self._normalize_fields(df_cleaned)
        
        # 3.5 区域编码校验/修正（改造版默认启用；可回滚开关）
        if self.enable_strict_region_codes:
            df_cleaned = self._fix_region_codes_strict(df_cleaned)
        else:
            # 旧逻辑（逐行循环），保留用于回滚
            df_cleaned = self._fix_region_names(df_cleaned)

        # 3.6 日期格式标准化（新增；可回滚开关）
        if self.enable_date_standardization:
            df_cleaned = self._standardize_dates(df_cleaned)
        
        # 4. 根据数据库字段类型转换数据类型（新增）
        # 获取表结构（优先使用schema_dict，否则使用table_schema）
        schema_to_use = None
        if self.schema_dict:
            schema_to_use = self.schema_dict.get_schema(self.engine)
        elif self.table_schema:
            schema_to_use = self.table_schema
        
        if schema_to_use and self.engine:
            df_cleaned = self._convert_to_database_types(df_cleaned, schema_to_use)
        
        # 4.5. 截断VARCHAR字段长度（在类型转换之后）
        if schema_to_use:
            df_cleaned = self._truncate_string_fields(df_cleaned, schema_to_use)
        
        # 5. 处理空值
        df_cleaned = self._handle_nulls(df_cleaned)
        
        # 6. 去重 - 已移除，由数据库唯一约束处理
        # df_cleaned = self._remove_duplicates(df_cleaned)
        
        # 7. 数据验证
        df_cleaned = self._validate_data(df_cleaned)
        
        self.cleaning_stats['rows_after'] = len(df_cleaned)
        
        if self.verbose:
            self._print_stats()
        
        return df_cleaned
    
    def _map_columns(self, df: pd.DataFrame, field_mapping: Dict[str, str]) -> pd.DataFrame:
        """映射列名"""
        # 只映射存在的列
        existing_mapping = {k: v for k, v in field_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)
        
        if self.verbose and existing_mapping:
            print(f"✅ 字段映射完成: {existing_mapping}")
        
        return df

    def _ensure_status_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        确保 data_status / is_active 字段存在且类型可用
        - data_status: 0=正常；5/6/7/8为导入校验异常标记（见文档说明）
        - is_active: 1=活跃
        """
        df_copy = df.copy()

        if 'is_active' not in df_copy.columns:
            df_copy['is_active'] = 1
        else:
            df_copy['is_active'] = pd.to_numeric(df_copy['is_active'], errors='coerce').fillna(1).astype(int)

        if 'data_status' not in df_copy.columns:
            df_copy['data_status'] = 0
        else:
            df_copy['data_status'] = pd.to_numeric(df_copy['data_status'], errors='coerce').fillna(0).astype(int)

        return df_copy

    def _generate_uids(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成/补齐 UID 主键（不覆盖已存在的有效UID）"""
        df_copy = df.copy()

        if 'UID' not in df_copy.columns:
            df_copy['UID'] = [str(uuid.uuid4()) for _ in range(len(df_copy))]
            if self.verbose:
                print(f"✅ 为所有 {len(df_copy)} 行数据生成了唯一标识ID (UID)")
            return df_copy

        mask = (
            df_copy['UID'].isna() |
            (df_copy['UID'].astype(str).str.strip() == '') |
            (df_copy['UID'].astype(str) == 'None') |
            (df_copy['UID'].astype(str).str.lower() == 'nan')
        )
        if mask.any():
            df_copy.loc[mask, 'UID'] = [str(uuid.uuid4()) for _ in range(int(mask.sum()))]
            if self.verbose:
                print(f"✅ 为 {int(mask.sum())} 行数据生成了唯一标识ID (UID)")

        return df_copy

    def _normalize_region_code_series(self, series: pd.Series) -> pd.Series:
        """
        将省/市/区县编码统一规范化为“仅数字”的字符串，处理常见脏格式：
        - Excel 读出来的浮点：520200.0 / 5.202e+05
        - 混入空格/非数字字符
        """
        s = series.astype(str).str.strip()
        s = s.where(~s.str.lower().isin(['nan', 'none', 'null', 'nat']), '')

        # 优先走数值解析（能处理 520200.0 / 5.202e+05 等）
        s_num = pd.to_numeric(s, errors='coerce')
        out = pd.Series([''] * len(s), index=s.index, dtype='object')

        mask_num = s_num.notna()
        if mask_num.any():
            # round(0) 兼容浮点误差
            out.loc[mask_num] = s_num.loc[mask_num].round(0).astype('Int64').astype(str)

        # 其余走字符串兜底：去掉末尾 .0 / 非数字
        if (~mask_num).any():
            s_txt = s.loc[~mask_num]
            s_txt = s_txt.str.replace(r'\.0+$', '', regex=True)
            s_txt = s_txt.str.replace(r'\D+', '', regex=True)
            out.loc[~mask_num] = s_txt

        out = out.where(out != '<NA>', '')
        return out

    def _strip_prefixed_region_name(self, series: pd.Series) -> pd.Series:
        """
        清理“省级前缀拼进城市/区县中文名”的脏数据，例如：
        - 贵州省六盘水市 → 六盘水市
        - 广西壮族自治区南宁市 → 南宁市
        仅对明显包含省级标记且以城市/地区/盟/自治州结尾的字符串做处理，避免误伤。
        """
        s = series.astype(str).str.strip()
        s = s.where(~s.str.lower().isin(['nan', 'none', 'null', 'nat']), '')
        mask = (
            s.str.contains(r'(省|自治区|特别行政区)', regex=True) &
            s.str.contains(r'(市|地区|盟|自治州)$', regex=True)
        )
        if mask.any():
            s = s.where(~mask, s.str.replace(r'^.*?(?:省|自治区|特别行政区)', '', regex=True))
        return s

    def _build_region_name_alias_map(self, standard_names: List[str], level: str = 'province') -> Dict[str, str]:
        """
        从 JSON 标准名建「别名/简称 → 标准名」表，用于反查前格式化中文名。
        使 "河北"、" 河北 " → "河北省"，"石家庄" → "石家庄市" 等能匹配到编码。
        """
        alias: Dict[str, str] = {}
        empty = {'nan', 'none', 'null', 'nat', ''}
        for n in standard_names:
            n = (n or '').strip()
            if not n or n.lower() in empty:
                continue
            alias[n] = n
            if level == 'province':
                if n.endswith('自治区'):
                    alias[n[:-3]] = n  # 内蒙古自治区 → 内蒙古
                elif n.endswith('特别行政区'):
                    alias[n[:-5]] = n  # 香港特别行政区 → 香港
                elif n.endswith('省') or n.endswith('市'):
                    alias[n[:-1]] = n  # 河北省/北京市 → 河北/北京
            elif level == 'city':
                if n.endswith('自治州'):
                    alias[n[:-3]] = n
                elif n.endswith('地区'):
                    alias[n[:-2]] = n
                elif n.endswith('盟') or n.endswith('市'):
                    alias[n[:-1]] = n
            else:
                # district: 区/县/市/旗/自治县/自治旗 等
                if n.endswith('自治县') or n.endswith('自治旗'):
                    alias[n[:-3]] = n
                elif n.endswith('区') or n.endswith('县') or n.endswith('市') or n.endswith('旗'):
                    alias[n[:-1]] = n
        return alias

    def _normalize_region_name_for_lookup(self, series: pd.Series, alias_map: Dict[str, str]) -> pd.Series:
        """
        对中文名列做「格式化 → 标准名」，便于与 name_to_code 匹配反查编码。
        先 strip、去空，再映射别名；未在 alias_map 的保持原值（可能已是标准名）。
        """
        s = series.astype(str).str.strip()
        s = s.where(~s.str.lower().isin(['nan', 'none', 'null', 'nat']), '')
        return s.map(lambda x: (alias_map.get(x, x) if x else ''))

    def _fix_region_codes_strict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        区域编码严格校验（向量化、增量改造版）
        - 省份：格式化 + 名称映射
        - 城市：分级修复（直辖市/区县推导/当前推导），无法修复标记 data_status=5
        - 区县：格式化 + 字典存在性校验，不存在标记 data_status=6
        - 三级联动：省-市-区隶属一致性，不一致标记 data_status=8（避免与日期异常7冲突）
        """
        # 检查是否存在需要处理的字段
        if not any(col in df.columns for col in ['省份', '城市', '区县']):
            return df

        try:
            converter = get_converter()
            province_map = converter.provinces
            city_map = converter.cities
            district_map = converter.districts
        except Exception as e:
            if self.verbose:
                print(f"⚠️  区域代码转换器加载失败: {e}，跳过区域编码校验")
            return df

        df_fixed = df.copy()
        df_fixed = self._ensure_status_columns(df_fixed)

        stats_fixed = {'province_fixed': 0, 'city_fixed': 0, 'district_fixed': 0}
        stats_issues = {'city_code_unfixed': 0, 'district_code_invalid': 0, 'hierarchy_mismatch': 0}

        # 直辖市固定城市编码（文档分级修复策略）
        direct_city_code = {'11': '110100', '12': '120100', '31': '310100', '50': '500100'}

        # ========== 0) 确保中文列存在（导入时名称同步） ==========
        if '省份' in df_fixed.columns and '省份_中文' not in df_fixed.columns:
            df_fixed['省份_中文'] = ''
        if '城市' in df_fixed.columns and '城市_中文' not in df_fixed.columns:
            df_fixed['城市_中文'] = ''
        if '区县' in df_fixed.columns and '区县_中文' not in df_fixed.columns:
            df_fixed['区县_中文'] = ''
        # 导入策略：最终以「编码 + JSON」为准写回三省中文；编码为空时保留 Excel/上游中文用于反查，不做清空。

        # ========== 1) 省份编码校验 ==========
        if '省份' in df_fixed.columns:
            prov = self._normalize_region_code_series(df_fixed['省份'])
            # 截断到最多6位（避免极端脏数据）
            prov = prov.where(prov.str.len() <= 6, prov.str[:6])

            # 标准化：2位 -> 6位；6位非0000 -> 提取前2位
            mask_2 = prov.str.len() == 2
            prov = prov.where(~mask_2, prov + '0000')

            mask_6_not_end = (prov.str.len() == 6) & (~prov.str.endswith('0000'))
            prov = prov.where(~mask_6_not_end, prov.str[:2] + '0000')

            # 名称反查补齐编码：编码为空或无效时，用「省份_中文」经格式化后反查 JSON 得到编码
            name_to_code = {v: k for k, v in province_map.items()}
            province_aliases = self._build_region_name_alias_map(list(province_map.values()), 'province')
            prov_name_norm = self._normalize_region_name_for_lookup(df_fixed['省份_中文'], province_aliases)
            need_reverse = (~prov.isin(set(province_map.keys())) | (prov == '')) & (prov_name_norm != '') & (prov_name_norm.isin(set(name_to_code.keys())))
            if need_reverse.any():
                prov = prov.where(~need_reverse, prov_name_norm.map(name_to_code).fillna(prov))

            df_fixed['省份'] = prov

            # 强制“编码→中文”回写（名称同步核心）
            new_names = df_fixed['省份'].map(province_map)
            changed = new_names.notna() & (df_fixed['省份_中文'] != new_names)
            stats_fixed['province_fixed'] = int(changed.sum())
            df_fixed.loc[new_names.notna(), '省份_中文'] = new_names[new_names.notna()]

        # ========== 2) 城市编码修复（分级） ==========
        if '城市' in df_fixed.columns:
            city = self._normalize_region_code_series(df_fixed['城市'])
            city = city.where(city.str.len() <= 6, city.str[:6])

            # 标准化：4位 -> 6位（补00）
            mask_4 = city.str.len() == 4
            city = city.where(~mask_4, city + '00')

            df_fixed['城市'] = city
            df_fixed['_province_prefix'] = df_fixed['省份'].astype(str).str[:2] if '省份' in df_fixed.columns else ''

            # 先清理城市中文的“省前缀拼接”脏值，便于反查
            df_fixed['城市_中文'] = self._strip_prefixed_region_name(df_fixed['城市_中文'])

            needs_fix = (df_fixed['城市'].str.len() >= 4) & (~df_fixed['城市'].str.endswith('00'))

            # Step 1: 直辖市（仅对needs_fix的记录）
            is_direct = df_fixed['_province_prefix'].isin(list(direct_city_code.keys())) & needs_fix
            if is_direct.any():
                df_fixed.loc[is_direct, '城市'] = df_fixed.loc[is_direct, '_province_prefix'].map(direct_city_code)
                needs_fix = needs_fix & ~is_direct

            # Step 2: 从区县编码推导
            if '区县' in df_fixed.columns:
                df_fixed['_district_code'] = df_fixed['区县'].astype(str).str.strip()
                df_fixed['_district_code'] = df_fixed['_district_code'].where(
                    ~df_fixed['_district_code'].str.lower().isin(['nan', 'none', 'null', 'nat']), ''
                ).str.zfill(6)
                df_fixed['_derived_city'] = df_fixed['_district_code'].str[:4] + '00'

                prov_prefix = df_fixed['_province_prefix'].fillna('')
                prefix_ok = (prov_prefix == '') | (df_fixed['_derived_city'].str[:2] == prov_prefix)

                can_derive = (
                    needs_fix &
                    (df_fixed['_district_code'].str.len() >= 4) &
                    df_fixed['_derived_city'].isin(list(city_map.keys())) &
                    prefix_ok
                )
                if can_derive.any():
                    df_fixed.loc[can_derive, '城市'] = df_fixed.loc[can_derive, '_derived_city']
                    needs_fix = needs_fix & ~can_derive

            # Step 3: 从当前编码推导
            df_fixed['_current_derived'] = df_fixed['城市'].str[:4] + '00'
            prov_prefix = df_fixed['_province_prefix'].fillna('')
            prefix_ok_current = (prov_prefix == '') | (df_fixed['_current_derived'].str[:2] == prov_prefix)
            can_derive_current = (
                needs_fix &
                df_fixed['_current_derived'].isin(list(city_map.keys())) &
                prefix_ok_current
            )
            if can_derive_current.any():
                df_fixed.loc[can_derive_current, '城市'] = df_fixed.loc[can_derive_current, '_current_derived']
                needs_fix = needs_fix & ~can_derive_current

            # Step 4: 地址解析辅助（兜底，置信度较低，仅对仍需修复的记录）
            # 文档策略：无法推导时尝试从“充电站位置”解析城市名称并匹配字典
            if needs_fix.any() and ('充电站位置' in df_fixed.columns):
                addr = df_fixed['充电站位置'].astype(str).str.strip()
                addr = addr.where(~addr.str.lower().isin(['nan', 'none', 'null', 'nat']), '')
                df_fixed['_addr_city_name'] = addr.str.extract(
                    r'([\u4e00-\u9fa5]{2,}(?:市|地区|盟|自治州))', expand=False
                )

                if df_fixed['_addr_city_name'].notna().any():
                    city_items = list(city_map.items())
                    city_lookup_df = pd.DataFrame({
                        '_province_prefix': [code[:2] for code, _ in city_items],
                        '_addr_city_name': [name for _, name in city_items],
                        '_addr_city_code': [code for code, _ in city_items],
                    })

                    tmp = (
                        df_fixed.loc[needs_fix, ['_province_prefix', '_addr_city_name']]
                        .reset_index()
                        .merge(city_lookup_df, on=['_province_prefix', '_addr_city_name'], how='left')
                    )
                    df_fixed['_addr_city_code'] = pd.NA
                    df_fixed.loc[tmp['index'], '_addr_city_code'] = tmp['_addr_city_code'].values

                    can_addr_fix = needs_fix & df_fixed['_addr_city_code'].notna()
                    if can_addr_fix.any():
                        df_fixed.loc[can_addr_fix, '城市'] = df_fixed.loc[can_addr_fix, '_addr_city_code'].astype(str)
                        needs_fix = needs_fix & ~can_addr_fix

            # Step 5: 仍无法修复的标记为异常（data_status=5）
            if needs_fix.any():
                mark_mask = needs_fix & (df_fixed['data_status'] == 0)
                df_fixed.loc[mark_mask, 'data_status'] = 5
                stats_issues['city_code_unfixed'] = int(mark_mask.sum())

            # Step 6: 名称反查补齐城市编码（当编码为空/不在字典中，用「城市_中文」经格式化后反查）
            city_code_valid = df_fixed['城市'].isin(set(city_map.keys()))
            city_aliases = self._build_region_name_alias_map(list(city_map.values()), 'city')
            df_fixed['_city_name_norm'] = self._normalize_region_name_for_lookup(df_fixed['城市_中文'], city_aliases)
            need_city_reverse = (~city_code_valid) & (df_fixed['_city_name_norm'] != '')
            if need_city_reverse.any():
                city_items = list(city_map.items())
                city_lookup_df = pd.DataFrame({
                    '_province_prefix': [code[:2] for code, _ in city_items],
                    '_city_name': [name for _, name in city_items],
                    '_city_code': [code for code, _ in city_items],
                })
                tmp = (
                    df_fixed.loc[need_city_reverse, ['_province_prefix', '_city_name_norm']]
                    .rename(columns={'_city_name_norm': '_city_name'})
                    .reset_index()
                    .merge(city_lookup_df, on=['_province_prefix', '_city_name'], how='left')
                )
                if '_city_code' in tmp.columns:
                    df_fixed['_rev_city_code'] = pd.NA
                    df_fixed.loc[tmp['index'], '_rev_city_code'] = tmp['_city_code'].values
                    can_rev_city = need_city_reverse & df_fixed['_rev_city_code'].notna()
                    if can_rev_city.any():
                        df_fixed.loc[can_rev_city, '城市'] = df_fixed.loc[can_rev_city, '_rev_city_code'].astype(str)

            # 强制“编码→中文”回写（名称同步核心）
            new_city_names = df_fixed['城市'].map(city_map)
            changed = new_city_names.notna() & (df_fixed['城市_中文'] != new_city_names)
            stats_fixed['city_fixed'] = int(changed.sum())
            df_fixed.loc[new_city_names.notna(), '城市_中文'] = new_city_names[new_city_names.notna()]

        # ========== 3) 区县编码校验 ==========
        if '区县' in df_fixed.columns:
            dist = self._normalize_region_code_series(df_fixed['区县'])
            dist = dist.where(dist.str.len() <= 6, dist.str[:6])
            dist = dist.str.zfill(6)
            df_fixed['区县'] = dist

            district_valid = df_fixed['区县'].isin(set(district_map.keys()))
            invalid_mask = (~district_valid) & (df_fixed['区县'] != '') & (df_fixed['data_status'] == 0)
            if invalid_mask.any():
                df_fixed.loc[invalid_mask, 'data_status'] = 6
                stats_issues['district_code_invalid'] = int(invalid_mask.sum())

            # 先清理区县中文的“省前缀拼接”脏值（虽然少见，但有）
            df_fixed['区县_中文'] = self._strip_prefixed_region_name(df_fixed['区县_中文'])

            # 名称反查补齐区县编码（当编码为空/无效，用「区县_中文」经格式化后反查；优先按城市前4位约束）
            dist_code_valid = df_fixed['区县'].isin(set(district_map.keys()))
            district_aliases = self._build_region_name_alias_map(list(district_map.values()), 'district')
            df_fixed['_dist_name_norm'] = self._normalize_region_name_for_lookup(df_fixed['区县_中文'], district_aliases)
            need_dist_reverse = (~dist_code_valid) & (df_fixed['_dist_name_norm'] != '')
            if need_dist_reverse.any() and '城市' in df_fixed.columns:
                dist_items = list(district_map.items())
                dist_lookup_df = pd.DataFrame({
                    '_city_prefix4': [code[:4] for code, _ in dist_items],
                    '_dist_name': [name for _, name in dist_items],
                    '_dist_code': [code for code, _ in dist_items],
                })
                df_fixed['_city_prefix4'] = df_fixed['城市'].astype(str).str[:4]
                tmp = (
                    df_fixed.loc[need_dist_reverse, ['_city_prefix4', '_dist_name_norm']]
                    .rename(columns={'_dist_name_norm': '_dist_name'})
                    .reset_index()
                    .merge(dist_lookup_df, on=['_city_prefix4', '_dist_name'], how='left')
                )
                if '_dist_code' in tmp.columns:
                    df_fixed['_rev_dist_code'] = pd.NA
                    df_fixed.loc[tmp['index'], '_rev_dist_code'] = tmp['_dist_code'].values
                    can_rev_dist = need_dist_reverse & df_fixed['_rev_dist_code'].notna()
                    if can_rev_dist.any():
                        df_fixed.loc[can_rev_dist, '区县'] = df_fixed.loc[can_rev_dist, '_rev_dist_code'].astype(str)

            # 强制“编码→中文”回写（名称同步核心）
            new_dist_names = df_fixed['区县'].map(district_map)
            changed = new_dist_names.notna() & (df_fixed['区县_中文'] != new_dist_names)
            stats_fixed['district_fixed'] = int(changed.sum())
            df_fixed.loc[new_dist_names.notna(), '区县_中文'] = new_dist_names[new_dist_names.notna()]

        # ========== 4) 三级联动校验（省-市-区隶属一致性） ==========
        if self.enable_region_hierarchy_validation and all(col in df_fixed.columns for col in ['省份', '城市', '区县']):
            prov_prefix2 = df_fixed['省份'].astype(str).str[:2]
            city_prefix2 = df_fixed['城市'].astype(str).str[:2]
            city_to_dist = df_fixed['城市'].astype(str).str[:4]
            dist_city_part = df_fixed['区县'].astype(str).str[:4]

            # 只对编码看起来有效的记录做校验（避免空值误判）
            has_codes = (df_fixed['省份'].astype(str).str.len() >= 2) & (df_fixed['城市'].astype(str).str.len() >= 4) & (df_fixed['区县'].astype(str).str.len() >= 6)
            mismatch = has_codes & ((prov_prefix2 != city_prefix2) | (city_to_dist != dist_city_part))
            mark_mismatch = mismatch & (df_fixed['data_status'] == 0)
            if mark_mismatch.any():
                df_fixed.loc[mark_mismatch, 'data_status'] = 8
                stats_issues['hierarchy_mismatch'] = int(mark_mismatch.sum())

        # 清理临时列
        temp_cols = [c for c in df_fixed.columns if c.startswith('_')]
        df_fixed = df_fixed.drop(columns=temp_cols, errors='ignore')

        # 更新统计
        self.cleaning_stats['region_fixed'] = stats_fixed
        self.cleaning_stats['region_issues'] = stats_issues

        total_fixed = sum(stats_fixed.values())
        if self.verbose and total_fixed > 0:
            print(
                f"✅ 区域编码校验: 省份 {stats_fixed['province_fixed']} 条, "
                f"城市 {stats_fixed['city_fixed']} 条, 区县 {stats_fixed['district_fixed']} 条"
            )

        total_issues = sum(stats_issues.values())
        if self.verbose and total_issues > 0:
            print(
                f"⚠️  区域编码异常标记: 城市未修复 {stats_issues['city_code_unfixed']} 条(data_status=5), "
                f"区县无效 {stats_issues['district_code_invalid']} 条(data_status=6), "
                f"隶属不一致 {stats_issues['hierarchy_mismatch']} 条(data_status=8)"
            )

        return df_fixed

    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        日期格式标准化（向量化优先，失败置NULL并标记data_status=7）
        处理字段（按文档）：充电站投入使用时间、充电桩生产日期
        """
        df_fixed = df.copy()
        df_fixed = self._ensure_status_columns(df_fixed)

        date_columns = ['充电站投入使用时间', '充电桩生产日期']
        for col in date_columns:
            if col in df_fixed.columns:
                df_fixed = self._standardize_date_column(df_fixed, col)

        return df_fixed

    def _standardize_date_column(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        标准化单个日期列 → date；失败置 None 并打 data_status=7。
        规则按备份表 evdata_backup 中 充电站投入使用时间 的实际格式重写：
        - 绝大多数为 YYYY/M/D 或 YYYY/MM/DD（单数字月、日很常见，如 2023/9/28、2024/1/8）
        - %Y/%m/%d 要求两位月日，无法匹配，故优先用 pd.to_datetime 宽松推断，再试固定格式。
        """
        df_copy = df.copy()

        raw = df_copy[col]
        s = raw.astype(str).str.strip()
        s = s.where(~s.str.lower().isin(['nan', 'none', 'null', 'nat']), pd.NA)

        # 全角→半角，便于解析
        s = s.astype(str).str.replace('\uff0f', '/', regex=False)   # 全角 /
        s = s.str.replace('\u2212', '-', regex=False)  # 减号
        s = s.str.replace('\u2014', '-', regex=False)  # em dash

        # 截取日期部分（去掉时分秒）
        s = s.astype('string')
        s = s.str.split(' ').str[0]
        s = s.str.split('T').str[0]
        s = s.astype(str)  # 统一为 str，避免 StringDtype 影响 pd.to_datetime

        # 非空 mask（用于统计与打标）
        non_empty = s.notna() & (s != '') & (s != 'nan')

        # Step1：优先宽松推断（覆盖 YYYY/M/D、YYYY/M/DD、YYYY/MM/D、YYYY-MM-DD、YYYYMMDD 等）
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            parsed = pd.to_datetime(s, errors='coerce')

        # Step2：对仍为 NaT 且非空的，用固定格式兜底
        formats = [
            '%Y/%m/%d', '%Y-%m-%d', '%Y%m%d',
            '%Y年%m月%d日', '%Y.%m.%d',
        ]
        remaining = parsed.isna() & non_empty
        for fmt in formats:
            if not remaining.any():
                break
            try:
                attempt = pd.to_datetime(s.where(remaining), format=fmt, errors='coerce')
                parsed = parsed.where(~attempt.notna(), attempt)
                remaining = parsed.isna() & non_empty
            except Exception:
                pass

        # Step3：再对剩余做一次无格式推断（兼容 2023.9.28、1/8/2024 等）
        if remaining.any():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                attempt = pd.to_datetime(s.where(remaining), errors='coerce')
            parsed = parsed.where(~attempt.notna(), attempt)

        # 年份有效性（1990 ~ 当前年+1）
        now_year = datetime.now().year
        if hasattr(parsed, 'dt'):
            year_ok = parsed.dt.year.between(1990, now_year + 1)
            parsed = parsed.where(year_ok, pd.NaT)

        # 统计与打标
        failed_mask = non_empty & parsed.isna()
        failed_count = int(failed_mask.sum())
        parsed_count = int((non_empty & parsed.notna()).sum())
        self.cleaning_stats['date_standardization']['parsed'] += parsed_count
        self.cleaning_stats['date_standardization']['failed'] += failed_count

        if failed_count > 0:
            mark_mask = failed_mask & (df_copy['data_status'] == 0)
            df_copy.loc[mark_mask, 'data_status'] = 7
            if self.verbose:
                sample_values = raw.loc[failed_mask].dropna().astype(str).unique()[:10].tolist()
                print(f"⚠️  日期转换: {failed_count} 条无法解析，已置NULL并标记 data_status=7，示例: {sample_values}")

        # 写回为 Python date 或 None
        def to_date_or_none(x):
            if x is None or (hasattr(x, '__bool__') and pd.isna(x)):
                return None
            if hasattr(x, 'date') and callable(getattr(x, 'date')):
                try:
                    return x.date()
                except Exception:
                    return None
            return None

        df_copy[col] = parsed.apply(to_date_or_none)

        return df_copy
    
    def _strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """去除字符串字段的前后空白"""
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
        
        return df
    
    def _normalize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化字段值（使用知识库）"""
        normalized_count = 0
        
        # 标准化运营商名称
        if '运营商名称' in df.columns:
            df['运营商名称'] = df['运营商名称'].apply(
                lambda x: KnowledgeBase.normalize_operator(str(x)) if pd.notna(x) else x
            )
            normalized_count += 1
        
        # 注意：不对 区县_中文 进行标准化处理
        # normalize_location 函数是为用户查询设计的（把"北京"转成"北京市"），
        # 不适合用于数据导入，其模糊匹配逻辑会导致区县名被错误转换
        # 例如："南关区" 可能被错误转换为 "吉林省" 等
        
        # 标准化充电桩类型
        if '充电桩类型' in df.columns:
            df['充电桩类型'] = df['充电桩类型'].apply(
                lambda x: KnowledgeBase.get_actual_value('充电桩类型', str(x)) 
                if pd.notna(x) and str(x) in KnowledgeBase.PILE_TYPE_NICKNAMES else x
            )
            normalized_count += 1
        
        # 标准化时间字段
        if '充电开始时间' in df.columns:
            df['充电开始时间'] = pd.to_datetime(
                df['充电开始时间'], 
                errors='coerce',
                infer_datetime_format=True
            )
            normalized_count += 1
        
        self.cleaning_stats['normalized_fields'] = normalized_count
        
        if self.verbose and normalized_count > 0:
            print(f"✅ 标准化了 {normalized_count} 个字段")
        
        return df
    
    def _fix_region_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根据区域代码修正省市区中文名称
        以数字代码为准，如果中文名称与代码不匹配则进行修正
        """
        # 检查是否存在需要处理的字段
        has_province = '省份' in df.columns and '省份_中文' in df.columns
        has_city = '城市' in df.columns and '城市_中文' in df.columns
        has_district = '区县' in df.columns and '区县_中文' in df.columns
        
        if not (has_province or has_city or has_district):
            return df
        
        try:
            converter = get_converter()
        except Exception as e:
            if self.verbose:
                print(f"⚠️  区域代码转换器加载失败: {e}，跳过区域名称修正")
            return df
        
        df_fixed = df.copy()
        province_fixed = 0
        city_fixed = 0
        district_fixed = 0
        
        # 逐行处理
        for idx in df_fixed.index:
            # 修正省份
            if has_province:
                code = df_fixed.at[idx, '省份']
                current_name = df_fixed.at[idx, '省份_中文']
                if pd.notna(code) and str(code).strip():
                    correct_name, fixed = converter.convert_province(str(code), str(current_name) if pd.notna(current_name) else None)
                    if fixed and correct_name:
                        df_fixed.at[idx, '省份_中文'] = correct_name
                        province_fixed += 1
            
            # 修正城市
            if has_city:
                code = df_fixed.at[idx, '城市']
                current_name = df_fixed.at[idx, '城市_中文']
                if pd.notna(code) and str(code).strip():
                    correct_name, fixed = converter.convert_city(str(code), str(current_name) if pd.notna(current_name) else None)
                    if fixed and correct_name:
                        df_fixed.at[idx, '城市_中文'] = correct_name
                        city_fixed += 1
            
            # 修正区县
            if has_district:
                code = df_fixed.at[idx, '区县']
                current_name = df_fixed.at[idx, '区县_中文']
                if pd.notna(code) and str(code).strip():
                    correct_name, fixed = converter.convert_district(str(code), str(current_name) if pd.notna(current_name) else None)
                    if fixed and correct_name:
                        df_fixed.at[idx, '区县_中文'] = correct_name
                        district_fixed += 1
        
        # 更新统计
        self.cleaning_stats['region_fixed']['province_fixed'] = province_fixed
        self.cleaning_stats['region_fixed']['city_fixed'] = city_fixed
        self.cleaning_stats['region_fixed']['district_fixed'] = district_fixed
        
        total_fixed = province_fixed + city_fixed + district_fixed
        if self.verbose and total_fixed > 0:
            print(f"✅ 区域名称修正: 省份 {province_fixed} 条, 城市 {city_fixed} 条, 区县 {district_fixed} 条")
        
        return df_fixed
    
    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理空值"""
        # 处理运营商名称为空的情况：自动填充为"无登记运营商"
        if '运营商名称' in df.columns:
            empty_mask = (
                df['运营商名称'].isna() | 
                (df['运营商名称'].astype(str).str.strip() == '') |
                (df['运营商名称'].astype(str) == 'nan')
            )
            if empty_mask.any():
                df.loc[empty_mask, '运营商名称'] = '无登记运营商'
                filled_count = empty_mask.sum()
                if self.verbose:
                    print(f"✅ 为 {filled_count} 条记录的运营商名称填充了默认值：'无登记运营商'")
        
        # 对于其他字符串字段，用空字符串填充
        # 注意：日期字段标准化后需要保留NULL，避免被填充为''（与文档“日期无效置NULL”一致）
        date_columns = {'充电站投入使用时间', '充电桩生产日期'}
        for col in df.select_dtypes(include=['object']).columns:
            if col == '运营商名称':  # 运营商名称已经处理过了
                continue
            if col in date_columns:
                continue
            df[col] = df[col].fillna('')
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """去除重复数据"""
        # 基于充电桩编号去重（如果存在）
        # 注意：充电桩编号为空的行不算重复数据，应该保留
        if '充电桩编号' in df.columns:
            before_count = len(df)
            
            # 标识充电桩编号为空的记录
            empty_mask = (
                df['充电桩编号'].isna() | 
                (df['充电桩编号'].astype(str).str.strip() == '') |
                (df['充电桩编号'].astype(str) == 'nan')
            )
            
            # 分离空值和非空值
            df_non_empty = df[~empty_mask].copy()  # 非空值的数据
            df_empty = df[empty_mask].copy()  # 空值的数据（全部保留）
            
            # 只对非空值进行去重
            if not df_non_empty.empty:
                df_non_empty = df_non_empty.drop_duplicates(subset=['充电桩编号'], keep='first')
            
            # 合并去重后的非空数据和所有空值数据
            if not df_empty.empty and not df_non_empty.empty:
                df = pd.concat([df_non_empty, df_empty], ignore_index=True)
            elif not df_empty.empty:
                df = df_empty
            else:
                df = df_non_empty
            
            removed = before_count - len(df)
            self.cleaning_stats['duplicates_removed'] = removed
            
            if self.verbose and removed > 0:
                print(f"✅ 删除了 {removed} 条重复数据（已忽略充电桩编号为空的记录）")
        else:
            # 如果没有编号字段，基于所有列去重
            before_count = len(df)
            df = df.drop_duplicates(keep='first')
            removed = before_count - len(df)
            self.cleaning_stats['duplicates_removed'] = removed
            
            if self.verbose and removed > 0:
                print(f"✅ 删除了 {removed} 条重复数据")
        
        return df
    
    def _convert_to_database_types(self, df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
        """根据数据库字段类型转换数据类型"""
        if not self.type_converter:
            return df
            
        try:
            df_converted = self.type_converter.convert_to_database_types(
                df, schema, self.engine
            )
            stats = self.type_converter.get_conversion_stats()
            self.cleaning_stats['columns_type_converted'] = stats['columns_converted']
            
            if stats['errors']:
                if self.verbose:
                    print(f"⚠️  类型转换警告: {len(stats['errors'])} 个字段转换时出现问题")
                    for error in stats['errors'][:3]:  # 只显示前3个错误
                        print(f"   - {error}")
            
            return df_converted
        except Exception as e:
            if self.verbose:
                print(f"⚠️  类型转换失败: {str(e)}，使用原始数据类型")
            return df
    
    def _truncate_string_fields(self, df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
        """
        截断VARCHAR字段的长度，确保不超过数据库定义的最大长度
        :param df: 原始DataFrame
        :param schema: 表结构字典
        :return: 截断后的DataFrame
        """
        # 获取所有VARCHAR字段的长度定义
        varchar_lengths = get_varchar_lengths(schema)
        
        if not varchar_lengths:
            return df
        
        df_truncated = df.copy()
        total_truncated = 0
        
        for col_name, max_length in varchar_lengths.items():
            # 只处理DataFrame中存在的列
            if col_name not in df_truncated.columns:
                continue
            
            # 只处理字符串类型的列
            if df_truncated[col_name].dtype != 'object':
                continue
            
            # 只对非空值进行截断，保持NaN值不变
            # 创建一个掩码来标识非空值
            non_null_mask = df_truncated[col_name].notna()
            
            if non_null_mask.any():
                # 只对非空值转换为字符串并截断
                non_null_values = df_truncated.loc[non_null_mask, col_name].astype(str)
                
                # 统计截断前的长度
                before_truncation = non_null_values.str.len()
                
                # 截断字符串（从头开始截取）
                truncated_values = non_null_values.str[:max_length]
                
                # 更新DataFrame中的值
                df_truncated.loc[non_null_mask, col_name] = truncated_values
                
                # 统计被截断的行数
                truncated_mask = before_truncation > max_length
                
                if truncated_mask.any():
                    truncated_count = truncated_mask.sum()
                    total_truncated += truncated_count
                    
                    # 记录该字段的截断统计
                    if col_name not in self.cleaning_stats['truncation_details']:
                        self.cleaning_stats['truncation_details'][col_name] = {
                            'max_length': max_length,
                            'truncated_count': 0,
                            'max_original_length': 0
                        }
                    
                    self.cleaning_stats['truncation_details'][col_name]['truncated_count'] += truncated_count
                    max_orig_len = before_truncation[truncated_mask].max()
                    current_max = self.cleaning_stats['truncation_details'][col_name]['max_original_length']
                    if max_orig_len > current_max:
                        self.cleaning_stats['truncation_details'][col_name]['max_original_length'] = int(max_orig_len)
                    
                    if self.verbose:
                        print(f"⚠️  字段 '{col_name}': {truncated_count} 个值超出长度限制({max_length})，已截断（原始最大长度: {int(max_orig_len)}）")
        
        self.cleaning_stats['strings_truncated'] = total_truncated
        
        if self.verbose and total_truncated > 0:
            print(f"✅ 共截断了 {total_truncated} 个字符串值")
        
        return df_truncated
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据验证"""
        # 注意：充电桩编号可以为空，运营商名称也会自动填充，所以不再删除空值记录
        # 这里可以添加其他自定义验证逻辑
        
        invalid_rows = []
        
        # 可以添加其他字段的验证逻辑
        # 目前不删除任何记录
        
        if invalid_rows:
            invalid_rows = list(set(invalid_rows))
            df = df.drop(index=invalid_rows)
            self.cleaning_stats['invalid_rows_removed'] = len(invalid_rows)
            
            if self.verbose:
                print(f"⚠️  删除了 {len(invalid_rows)} 行无效数据")
        
        return df
    
    def _print_stats(self):
        """打印清洗统计信息"""
        print("\n" + "="*50)
        print("📊 数据清洗统计")
        print("="*50)
        print(f"清洗前行数: {self.cleaning_stats['rows_before']}")
        print(f"清洗后行数: {self.cleaning_stats['rows_after']}")
        print(f"删除重复: {self.cleaning_stats['duplicates_removed']} 行")
        print(f"删除空值: {self.cleaning_stats['null_rows_removed']} 行")
        print(f"删除无效: {self.cleaning_stats['invalid_rows_removed']} 行")
        print(f"标准化字段: {self.cleaning_stats['normalized_fields']} 个")
        print(f"类型转换字段: {self.cleaning_stats['columns_type_converted']} 个")
        if self.cleaning_stats['strings_truncated'] > 0:
            print(f"字符串截断: {self.cleaning_stats['strings_truncated']} 个值")
            # 显示前5个字段的截断详情
            for col_name, details in list(self.cleaning_stats['truncation_details'].items())[:5]:
                print(f"  - {col_name}: {details['truncated_count']} 个值被截断（限制长度: {details['max_length']}, 原始最大长度: {details['max_original_length']}）")
        
        # 区域名称修正统计
        region_stats = self.cleaning_stats.get('region_fixed', {})
        total_region_fixed = sum(region_stats.values())
        if total_region_fixed > 0:
            print(f"区域名称修正: {total_region_fixed} 条")
            print(f"  - 省份: {region_stats.get('province_fixed', 0)} 条")
            print(f"  - 城市: {region_stats.get('city_fixed', 0)} 条")
            print(f"  - 区县: {region_stats.get('district_fixed', 0)} 条")

        # 区域编码异常标记统计
        region_issues = self.cleaning_stats.get('region_issues', {})
        if region_issues:
            total_region_issues = sum(region_issues.values())
            if total_region_issues > 0:
                print(f"区域编码异常标记: {total_region_issues} 条")
                print(f"  - 城市未修复(data_status=5): {region_issues.get('city_code_unfixed', 0)} 条")
                print(f"  - 区县无效(data_status=6): {region_issues.get('district_code_invalid', 0)} 条")
                print(f"  - 隶属不一致(data_status=8): {region_issues.get('hierarchy_mismatch', 0)} 条")

        # 日期标准化统计
        date_stats = self.cleaning_stats.get('date_standardization', {})
        if date_stats and (date_stats.get('parsed', 0) > 0 or date_stats.get('failed', 0) > 0):
            print(f"日期标准化: 成功 {date_stats.get('parsed', 0)} 条, 失败 {date_stats.get('failed', 0)} 条(data_status=7)")
        
        if self.cleaning_stats['rows_before'] > 0:
            retention_rate = self.cleaning_stats['rows_after']/self.cleaning_stats['rows_before']*100
            print(f"数据保留率: {retention_rate:.2f}%")
        else:
            print(f"数据保留率: N/A (清洗前行数为0)")
        print("="*50 + "\n")
    
    def get_stats(self) -> Dict:
        """获取清洗统计信息"""
        return self.cleaning_stats.copy()

