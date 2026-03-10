# 数据库SQL优化设计文档

> 基于 **充电桩数据管理系统** evdata 表的SQL优化设计方案
> 
> **版本**: v1.0 | **日期**: 2026-01-13 | **作者**: VDBP

---

## 📋 目录

1. [当前数据库现状分析](#1-当前数据库现状分析)
2. [索引优化设计](#2-索引优化设计)
3. [SQL查询优化](#3-sql查询优化)
4. [视图设计](#4-视图设计)
5. [数据类型优化](#5-数据类型优化)
6. [存储过程设计](#6-存储过程设计)
7. [性能监控方案](#7-性能监控方案)
8. [实施计划](#8-实施计划)

---

## 1. 当前数据库现状分析

### 1.1 表结构概述

| 项目 | 说明 |
|------|------|
| **数据库名** | evcipadata |
| **主表名** | evdata |
| **设计模式** | 宽表（扁平化设计） |
| **字段数量** | 58 个字段 |
| **数据特点** | 每条记录 = 1个充电桩 + 关联的充电站/运营商/区域信息 |

### 1.2 高频查询字段分析

根据业务代码分析，以下字段为**高频查询字段**：

| 优先级 | 字段名 | 查询类型 | 使用频率 |
|--------|--------|----------|----------|
| ⭐⭐⭐ | `运营商名称` | WHERE IN / GROUP BY | 极高 |
| ⭐⭐⭐ | `省份_中文` | WHERE = / GROUP BY | 极高 |
| ⭐⭐⭐ | `城市_中文` | WHERE = / GROUP BY | 极高 |
| ⭐⭐⭐ | `区县_中文` | WHERE = / GROUP BY | 极高 |
| ⭐⭐⭐ | `额定功率` | WHERE BETWEEN / GROUP BY | 高 |
| ⭐⭐ | `充电桩类型_转换` | WHERE LIKE / GROUP BY | 高 |
| ⭐⭐ | `所属充电站编号` | COUNT DISTINCT / GROUP BY | 高 |
| ⭐⭐ | `充电桩生产日期` | WHERE >= / <= | 中 |
| ⭐⭐ | `充电站投入使用时间` | WHERE (需转换) | 中 |
| ⭐ | `UID` | 主键/唯一标识 | 中 |
| ⭐ | `充电桩编号` | 业务查询 | 中 |
| ⭐ | `充电桩型号` | 精确查询 | 低 |

### 1.3 当前性能瓶颈

| 问题 | 影响 | 原因 |
|------|------|------|
| 全表扫描 | 查询慢 | 缺乏索引 |
| VARCHAR时间字段 | 无法直接比较 | `充电站投入使用时间` 存储格式不标准 |
| 数据冗余 | 存储浪费 | 宽表设计特点 |
| 无连接池优化 | 连接频繁创建 | ✅ 已优化 |

---

## 2. 索引优化设计

### 2.1 推荐索引方案

#### 主键索引（必需）
```sql
-- 确保 UID 为主键（如果尚未设置）
ALTER TABLE evdata ADD PRIMARY KEY (UID);
```

#### 单列索引（高优先级）
```sql
-- 运营商名称索引（最高频查询字段）
CREATE INDEX idx_operator_name ON evdata (运营商名称);

-- 三级区域索引
CREATE INDEX idx_province ON evdata (省份_中文);
CREATE INDEX idx_city ON evdata (城市_中文);
CREATE INDEX idx_district ON evdata (区县_中文);

-- 额定功率索引（功率区间查询）
CREATE INDEX idx_rated_power ON evdata (额定功率);

-- 充电桩类型索引
CREATE INDEX idx_pile_type ON evdata (充电桩类型_转换);

-- 充电站编号索引（用于 COUNT DISTINCT）
CREATE INDEX idx_station_code ON evdata (所属充电站编号);
```

#### 复合索引（高效组合查询）
```sql
-- 区域+运营商组合查询（最常用）
CREATE INDEX idx_region_operator ON evdata (省份_中文, 城市_中文, 运营商名称);

-- 区域三级联动
CREATE INDEX idx_region_hierarchy ON evdata (省份_中文, 城市_中文, 区县_中文);

-- 运营商+类型组合统计
CREATE INDEX idx_operator_type ON evdata (运营商名称, 充电桩类型_转换);

-- 功率+类型组合筛选
CREATE INDEX idx_power_type ON evdata (额定功率, 充电桩类型_转换);
```

#### 日期索引
```sql
-- 生产日期索引（DATE类型，可直接比较）
CREATE INDEX idx_production_date ON evdata (充电桩生产日期);

-- 入库时间索引
CREATE INDEX idx_import_time ON evdata (入库时间);
```

### 2.2 索引创建脚本

完整的索引创建脚本（一键执行）：

```sql
-- ============================================
-- evdata 表索引优化脚本
-- 执行前请先备份数据库
-- ============================================

-- 1. 单列索引
CREATE INDEX IF NOT EXISTS idx_operator_name ON evdata (运营商名称);
CREATE INDEX IF NOT EXISTS idx_province ON evdata (省份_中文);
CREATE INDEX IF NOT EXISTS idx_city ON evdata (城市_中文);
CREATE INDEX IF NOT EXISTS idx_district ON evdata (区县_中文);
CREATE INDEX IF NOT EXISTS idx_rated_power ON evdata (额定功率);
CREATE INDEX IF NOT EXISTS idx_pile_type ON evdata (充电桩类型_转换);
CREATE INDEX IF NOT EXISTS idx_station_code ON evdata (所属充电站编号);
CREATE INDEX IF NOT EXISTS idx_production_date ON evdata (充电桩生产日期);
CREATE INDEX IF NOT EXISTS idx_import_time ON evdata (入库时间);
CREATE INDEX IF NOT EXISTS idx_pile_code ON evdata (充电桩编号);
CREATE INDEX IF NOT EXISTS idx_pile_model ON evdata (充电桩型号);

-- 2. 复合索引
CREATE INDEX IF NOT EXISTS idx_region_operator ON evdata (省份_中文, 城市_中文, 运营商名称);
CREATE INDEX IF NOT EXISTS idx_region_hierarchy ON evdata (省份_中文, 城市_中文, 区县_中文);
CREATE INDEX IF NOT EXISTS idx_operator_type ON evdata (运营商名称, 充电桩类型_转换);
CREATE INDEX IF NOT EXISTS idx_power_type ON evdata (额定功率, 充电桩类型_转换);

-- 3. 验证索引
SHOW INDEX FROM evdata;
```

### 2.3 索引预期效果

| 查询场景 | 优化前 | 优化后 | 提升 |
|----------|--------|--------|------|
| 运营商统计 | 全表扫描 | 索引扫描 | ~10x |
| 区域筛选 | 全表扫描 | 索引扫描 | ~20x |
| 功率区间查询 | 全表扫描 | 范围扫描 | ~15x |
| 复合条件查询 | 多次扫描 | 覆盖索引 | ~30x |

---

## 3. SQL查询优化

### 3.1 常用查询优化模板

#### 运营商统计（TOP10）

**优化前**：
```sql
SELECT 运营商名称, COUNT(*) as 数量
FROM evdata
WHERE 运营商名称 IS NOT NULL AND 运营商名称 != ''
GROUP BY 运营商名称
ORDER BY 数量 DESC
LIMIT 10;
```

**优化后**（使用索引 + 只读需要的字段）：
```sql
SELECT 运营商名称, COUNT(*) as 数量
FROM evdata
WHERE 运营商名称 IS NOT NULL
GROUP BY 运营商名称
ORDER BY 数量 DESC
LIMIT 10;
-- 注：idx_operator_name 索引会被使用
```

#### 区域三级联动查询

**省份级别**（全国 → 按省统计）：
```sql
SELECT 省份_中文, COUNT(*) as 数量
FROM evdata
WHERE 省份_中文 IS NOT NULL
GROUP BY 省份_中文
ORDER BY 数量 DESC;
-- 使用索引：idx_province
```

**城市级别**（省 → 按城市统计）：
```sql
SELECT 城市_中文, COUNT(*) as 数量
FROM evdata
WHERE 省份_中文 = '山东省'
  AND 城市_中文 IS NOT NULL
GROUP BY 城市_中文
ORDER BY 数量 DESC;
-- 使用索引：idx_region_hierarchy
```

**区县级别**（市 → 按区县统计）：
```sql
SELECT 区县_中文, COUNT(*) as 数量
FROM evdata
WHERE 省份_中文 = '山东省'
  AND 城市_中文 = '济南市'
  AND 区县_中文 IS NOT NULL
GROUP BY 区县_中文
ORDER BY 数量 DESC;
-- 使用索引：idx_region_hierarchy
```

#### 功率区间统计

**优化方案**（避免 CASE 表达式重复计算）：
```sql
SELECT 
  CASE 
    WHEN 额定功率 <= 7 THEN '≤7kW（慢充）'
    WHEN 额定功率 <= 30 THEN '7-30kW（小功率）'
    WHEN 额定功率 <= 60 THEN '30-60kW（中功率）'
    WHEN 额定功率 <= 120 THEN '60-120kW（大功率）'
    ELSE '>120kW（超快充）'
  END as 功率区间,
  COUNT(*) as 数量
FROM evdata
WHERE 额定功率 IS NOT NULL
GROUP BY 功率区间
ORDER BY MIN(额定功率);
-- 使用索引：idx_rated_power
```

#### 综合筛选查询

**优化后**（利用复合索引）：
```sql
SELECT 
  充电桩编号,
  运营商名称,
  省份_中文,
  城市_中文,
  充电桩类型_转换,
  额定功率
FROM evdata
WHERE 省份_中文 = '山东省'      -- 使用 idx_region_operator
  AND 城市_中文 = '济南市'
  AND 运营商名称 IN ('国家电网', '特来电')
  AND 额定功率 > 60
  AND 额定功率 IS NOT NULL
ORDER BY 额定功率 DESC
LIMIT 1000;
```

### 3.2 时间字段查询优化

#### 问题：`充电站投入使用时间` 是 VARCHAR 类型

**当前方案**（低效）：
```sql
-- 需要转换，无法使用索引
WHERE STR_TO_DATE(充电站投入使用时间, '%Y/%m/%d') >= '2025-01-01'
```

**优化方案一**：添加计算列 + 索引
```sql
-- 添加标准化日期列
ALTER TABLE evdata 
ADD COLUMN 充电站投入使用时间_date DATE 
GENERATED ALWAYS AS (
  CASE 
    WHEN 充电站投入使用时间 IS NOT NULL 
         AND 充电站投入使用时间 != '' 
         AND 充电站投入使用时间 REGEXP '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$'
    THEN STR_TO_DATE(充电站投入使用时间, '%Y/%m/%d')
    ELSE NULL
  END
) STORED;

-- 在计算列上创建索引
CREATE INDEX idx_station_use_date ON evdata (充电站投入使用时间_date);
```

**优化方案二**：数据清洗时标准化（推荐）
- 在数据导入时将 VARCHAR 转为 DATE
- 修改 `data_cleaner.py` 中的时间字段处理逻辑

### 3.3 EXPLAIN 分析模板

在优化前后使用 EXPLAIN 分析查询计划：

```sql
-- 分析查询执行计划
EXPLAIN SELECT 运营商名称, COUNT(*) as 数量
FROM evdata
WHERE 省份_中文 = '山东省'
GROUP BY 运营商名称;

-- 详细分析（MySQL 8.0+）
EXPLAIN ANALYZE SELECT ...;

-- 查看索引使用情况
EXPLAIN FORMAT=JSON SELECT ...;
```

**关键指标**：
- `type`: 应为 `ref`、`range` 或 `index`，避免 `ALL`（全表扫描）
- `key`: 应显示使用的索引名
- `rows`: 预估扫描行数应尽量小

---

## 4. 视图设计

### 4.1 常用统计视图

#### 运营商统计视图
```sql
CREATE OR REPLACE VIEW v_operator_stats AS
SELECT 
  运营商名称,
  COUNT(*) as 充电桩数量,
  COUNT(DISTINCT 所属充电站编号) as 充电站数量,
  AVG(额定功率) as 平均功率,
  MAX(额定功率) as 最大功率
FROM evdata
WHERE 运营商名称 IS NOT NULL
GROUP BY 运营商名称;
```

#### 区域统计视图
```sql
CREATE OR REPLACE VIEW v_region_stats AS
SELECT 
  省份_中文,
  城市_中文,
  区县_中文,
  COUNT(*) as 充电桩数量,
  COUNT(DISTINCT 所属充电站编号) as 充电站数量
FROM evdata
WHERE 省份_中文 IS NOT NULL
GROUP BY 省份_中文, 城市_中文, 区县_中文;
```

#### 功率分布视图
```sql
CREATE OR REPLACE VIEW v_power_distribution AS
SELECT 
  CASE 
    WHEN 额定功率 <= 7 THEN '≤7kW（慢充）'
    WHEN 额定功率 <= 30 THEN '7-30kW（小功率）'
    WHEN 额定功率 <= 60 THEN '30-60kW（中功率）'
    WHEN 额定功率 <= 120 THEN '60-120kW（大功率）'
    ELSE '>120kW（超快充）'
  END as 功率区间,
  COUNT(*) as 数量,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM evdata WHERE 额定功率 IS NOT NULL), 2) as 占比
FROM evdata
WHERE 额定功率 IS NOT NULL
GROUP BY 功率区间;
```

#### 充电桩类型视图
```sql
CREATE OR REPLACE VIEW v_pile_type_stats AS
SELECT 
  充电桩类型_转换,
  COUNT(*) as 数量,
  AVG(额定功率) as 平均功率
FROM evdata
WHERE 充电桩类型_转换 IS NOT NULL
GROUP BY 充电桩类型_转换;
```

### 4.2 应用查询时使用视图

```sql
-- 直接查询视图（更简洁）
SELECT * FROM v_operator_stats ORDER BY 充电桩数量 DESC LIMIT 10;

-- 带条件的区域统计
SELECT * FROM v_region_stats WHERE 省份_中文 = '山东省';
```

---

## 5. 数据类型优化

### 5.1 当前问题与优化建议

| 字段 | 当前类型 | 问题 | 建议类型 |
|------|----------|------|----------|
| `充电站投入使用时间` | VARCHAR(255) | 无法直接比较 | DATE + 触发器转换 |
| `区县_中文` | TEXT | 过大 | VARCHAR(100) |
| `充电站联系电话` | INTEGER | 可能溢出 | VARCHAR(20) |
| `序号` | INTEGER | 非必需 | 考虑删除 |

### 5.2 数据类型修改脚本（谨慎执行）

```sql
-- 修改区县_中文类型（减少存储）
ALTER TABLE evdata MODIFY COLUMN 区县_中文 VARCHAR(100);

-- 注：充电站联系电话建议保持原样或在下次数据导入时处理
```

---

## 6. 存储过程设计

### 6.1 运营商统计存储过程

```sql
DELIMITER //
CREATE PROCEDURE sp_operator_stats(
  IN p_province VARCHAR(100),
  IN p_city VARCHAR(100),
  IN p_limit INT
)
BEGIN
  SELECT 
    运营商名称,
    COUNT(*) as 充电桩数量,
    COUNT(DISTINCT 所属充电站编号) as 充电站数量
  FROM evdata
  WHERE (p_province IS NULL OR 省份_中文 = p_province)
    AND (p_city IS NULL OR 城市_中文 = p_city)
    AND 运营商名称 IS NOT NULL
  GROUP BY 运营商名称
  ORDER BY 充电桩数量 DESC
  LIMIT IFNULL(p_limit, 100);
END //
DELIMITER ;

-- 调用示例
CALL sp_operator_stats('山东省', '济南市', 10);
CALL sp_operator_stats('山东省', NULL, 20);  -- 省级统计
CALL sp_operator_stats(NULL, NULL, 10);      -- 全国统计
```

### 6.2 区域统计存储过程

```sql
DELIMITER //
CREATE PROCEDURE sp_region_stats(
  IN p_province VARCHAR(100),
  IN p_city VARCHAR(100),
  IN p_stat_level VARCHAR(20)  -- 'province', 'city', 'district'
)
BEGIN
  IF p_stat_level = 'province' THEN
    SELECT 省份_中文, COUNT(*) as 数量
    FROM evdata
    WHERE 省份_中文 IS NOT NULL
    GROUP BY 省份_中文
    ORDER BY 数量 DESC;
  ELSEIF p_stat_level = 'city' THEN
    SELECT 城市_中文, COUNT(*) as 数量
    FROM evdata
    WHERE (p_province IS NULL OR 省份_中文 = p_province)
      AND 城市_中文 IS NOT NULL
    GROUP BY 城市_中文
    ORDER BY 数量 DESC;
  ELSE
    SELECT 区县_中文, COUNT(*) as 数量
    FROM evdata
    WHERE (p_province IS NULL OR 省份_中文 = p_province)
      AND (p_city IS NULL OR 城市_中文 = p_city)
      AND 区县_中文 IS NOT NULL
    GROUP BY 区县_中文
    ORDER BY 数量 DESC;
  END IF;
END //
DELIMITER ;

-- 调用示例
CALL sp_region_stats(NULL, NULL, 'province');           -- 全国省份统计
CALL sp_region_stats('山东省', NULL, 'city');           -- 山东省城市统计
CALL sp_region_stats('山东省', '济南市', 'district');   -- 济南市区县统计
```

---

## 7. 性能监控方案

### 7.1 慢查询监控

```sql
-- 开启慢查询日志（在 my.cnf 中配置）
-- slow_query_log = 1
-- slow_query_log_file = /var/log/mysql/slow.log
-- long_query_time = 2

-- 查看当前慢查询配置
SHOW VARIABLES LIKE 'slow_query%';
SHOW VARIABLES LIKE 'long_query_time';

-- 查看慢查询日志
-- 或使用 pt-query-digest 分析
```

### 7.2 索引使用监控

```sql
-- 查看索引使用情况
SELECT 
  TABLE_NAME,
  INDEX_NAME,
  SEQ_IN_INDEX,
  COLUMN_NAME,
  CARDINALITY
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'evcipadata'
  AND TABLE_NAME = 'evdata';

-- 查看未使用的索引（MySQL 8.0+ Performance Schema）
SELECT * FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE OBJECT_SCHEMA = 'evcipadata'
  AND OBJECT_NAME = 'evdata'
  AND INDEX_NAME IS NOT NULL
  AND COUNT_STAR = 0;
```

### 7.3 表状态监控

```sql
-- 查看表状态
SHOW TABLE STATUS LIKE 'evdata';

-- 分析表
ANALYZE TABLE evdata;

-- 优化表（重建索引，回收空间）
OPTIMIZE TABLE evdata;
```

### 7.4 连接池监控

```python
# 在 Python 应用中监控连接池
from sqlalchemy import event
from utils.db_utils import create_db_engine

engine = create_db_engine()

@event.listens_for(engine, "checkout")
def checkout(dbapi_conn, connection_record, connection_proxy):
    print(f"[连接池] 获取连接: {id(dbapi_conn)}")

@event.listens_for(engine, "checkin")
def checkin(dbapi_conn, connection_record):
    print(f"[连接池] 归还连接: {id(dbapi_conn)}")
```

---

## 8. 实施计划

### 8.1 阶段一：索引优化（立即执行）

| 步骤 | 操作 | 预计时间 | 风险 |
|------|------|----------|------|
| 1 | 备份数据库 | 10分钟 | 低 |
| 2 | 创建单列索引 | 5-10分钟 | 低 |
| 3 | 创建复合索引 | 5-10分钟 | 低 |
| 4 | 验证索引生效 | 5分钟 | 无 |

**执行命令**：
```bash
# 备份
mysqldump -u root -p evcipadata evdata > evdata_backup.sql

# 执行索引脚本
mysql -u root -p evcipadata < create_indexes.sql
```

### 8.2 阶段二：视图创建（短期）

| 步骤 | 操作 | 预计时间 |
|------|------|----------|
| 1 | 创建统计视图 | 5分钟 |
| 2 | 修改应用代码使用视图 | 1-2小时 |
| 3 | 测试验证 | 30分钟 |

### 8.3 阶段三：存储过程（中期）

| 步骤 | 操作 | 预计时间 |
|------|------|----------|
| 1 | 创建存储过程 | 30分钟 |
| 2 | 修改应用调用方式 | 2-4小时 |
| 3 | 性能测试对比 | 1小时 |

### 8.4 阶段四：数据类型优化（长期，需评估）

| 步骤 | 操作 | 风险 |
|------|------|------|
| 1 | 评估数据类型变更影响 | 需仔细评估 |
| 2 | 在测试环境验证 | 中 |
| 3 | 迁移生产数据 | 高（需停机） |

---

## 9. 附录

### 9.1 完整索引创建脚本

保存为 `create_indexes.sql`：

```sql
-- ============================================
-- evdata 表索引优化脚本 v1.0
-- 执行前请先备份数据库
-- ============================================

USE evcipadata;

-- 单列索引
CREATE INDEX IF NOT EXISTS idx_operator_name ON evdata (运营商名称);
CREATE INDEX IF NOT EXISTS idx_province ON evdata (省份_中文);
CREATE INDEX IF NOT EXISTS idx_city ON evdata (城市_中文);
CREATE INDEX IF NOT EXISTS idx_district ON evdata (区县_中文);
CREATE INDEX IF NOT EXISTS idx_rated_power ON evdata (额定功率);
CREATE INDEX IF NOT EXISTS idx_pile_type ON evdata (充电桩类型_转换);
CREATE INDEX IF NOT EXISTS idx_station_code ON evdata (所属充电站编号);
CREATE INDEX IF NOT EXISTS idx_production_date ON evdata (充电桩生产日期);
CREATE INDEX IF NOT EXISTS idx_import_time ON evdata (入库时间);
CREATE INDEX IF NOT EXISTS idx_pile_code ON evdata (充电桩编号);
CREATE INDEX IF NOT EXISTS idx_pile_model ON evdata (充电桩型号);

-- 复合索引
CREATE INDEX IF NOT EXISTS idx_region_operator ON evdata (省份_中文, 城市_中文, 运营商名称);
CREATE INDEX IF NOT EXISTS idx_region_hierarchy ON evdata (省份_中文, 城市_中文, 区县_中文);
CREATE INDEX IF NOT EXISTS idx_operator_type ON evdata (运营商名称, 充电桩类型_转换);
CREATE INDEX IF NOT EXISTS idx_power_type ON evdata (额定功率, 充电桩类型_转换);

-- 验证
SELECT 
  INDEX_NAME,
  COLUMN_NAME,
  SEQ_IN_INDEX
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'evcipadata'
  AND TABLE_NAME = 'evdata'
ORDER BY INDEX_NAME, SEQ_IN_INDEX;

SELECT '索引创建完成!' AS 状态;
```

### 9.2 视图创建脚本

保存为 `create_views.sql`：

```sql
-- ============================================
-- evdata 统计视图创建脚本 v1.0
-- ============================================

USE evcipadata;

-- 运营商统计视图
CREATE OR REPLACE VIEW v_operator_stats AS
SELECT 
  运营商名称,
  COUNT(*) as 充电桩数量,
  COUNT(DISTINCT 所属充电站编号) as 充电站数量,
  ROUND(AVG(额定功率), 2) as 平均功率
FROM evdata
WHERE 运营商名称 IS NOT NULL
GROUP BY 运营商名称;

-- 区域统计视图
CREATE OR REPLACE VIEW v_region_stats AS
SELECT 
  省份_中文,
  城市_中文,
  区县_中文,
  COUNT(*) as 充电桩数量,
  COUNT(DISTINCT 所属充电站编号) as 充电站数量
FROM evdata
WHERE 省份_中文 IS NOT NULL
GROUP BY 省份_中文, 城市_中文, 区县_中文;

-- 功率分布视图
CREATE OR REPLACE VIEW v_power_distribution AS
SELECT 
  CASE 
    WHEN 额定功率 <= 7 THEN '≤7kW（慢充）'
    WHEN 额定功率 <= 30 THEN '7-30kW（小功率）'
    WHEN 额定功率 <= 60 THEN '30-60kW（中功率）'
    WHEN 额定功率 <= 120 THEN '60-120kW（大功率）'
    ELSE '>120kW（超快充）'
  END as 功率区间,
  COUNT(*) as 数量
FROM evdata
WHERE 额定功率 IS NOT NULL
GROUP BY 功率区间;

-- 类型统计视图
CREATE OR REPLACE VIEW v_pile_type_stats AS
SELECT 
  充电桩类型_转换,
  COUNT(*) as 数量,
  ROUND(AVG(额定功率), 2) as 平均功率
FROM evdata
WHERE 充电桩类型_转换 IS NOT NULL
GROUP BY 充电桩类型_转换;

SELECT '视图创建完成!' AS 状态;
```

---

## 📌 快速参考卡片

### 索引创建（一键执行）
```bash
mysql -u root -p evcipadata < create_indexes.sql
```

### 常用查询优化对照
| 场景 | 使用索引 |
|------|----------|
| 运营商统计 | `idx_operator_name` |
| 省份筛选 | `idx_province` |
| 城市筛选 | `idx_region_hierarchy` |
| 功率筛选 | `idx_rated_power` |
| 复合条件 | `idx_region_operator` |

### 性能检查
```sql
EXPLAIN SELECT ... ;  -- 检查是否使用索引
SHOW INDEX FROM evdata;  -- 查看所有索引
```

---

*📊 BY VDBP | 数据库优化持续迭代*
