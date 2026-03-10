# 充电数据表结构重新设计方案

## 📋 目录
1. [当前表结构分析](#当前表结构分析)
2. [存在的问题](#存在的问题)
3. [重新设计方案](#重新设计方案)
4. [表结构详细设计](#表结构详细设计)

---

## 🔍 当前表结构分析

### 当前evdata表的特点
- **字段数量**: 约58个字段
- **设计模式**: 扁平化单表设计（宽表）
- **数据冗余**: 充电站信息在每个充电桩记录中重复存储
- **主要实体**: 充电桩、充电站、运营商、地区等信息混合在一起

### 字段分类

#### 充电桩相关字段（约25个）
- 充电桩编号、充电桩内部编号
- 充电桩类型、充电桩属性、充电桩型号
- 充电桩生产日期、设备开通时间
- 充电桩厂商编号
- 额定电压、额定电流、额定功率
- 接口数量、接口标准（1-4）
- 服务时间、支付方式

#### 充电站相关字段（约8个）
- 所属充电站编号、充电站内部编号
- 充电站名称、充电站位置
- 充电站投入使用时间
- 充电站所处道路属性
- 充电站联系电话

#### 运营商相关字段（约3个）
- 充电桩所属运营商
- 运营商名称
- 充电桩所属运营商_转换

#### 地区相关字段（约6个）
- 省份、城市、区县（代码）
- 省份_中文、城市_中文、区县_中文

#### 其他字段
- 序号、UID、入库时间、备注等

---

## ⚠️ 存在的问题

### 1. 数据冗余严重
- **问题**: 同一充电站的信息在每条充电桩记录中重复存储
- **影响**: 
  - 存储空间浪费（假设一个充电站有100个充电桩，充电站信息重复存储100次）
  - 数据更新困难（修改充电站信息需要更新所有相关充电桩记录）

### 2. 数据一致性风险
- **问题**: 同一充电站的不同充电桩记录中，充电站信息可能不一致
- **影响**: 数据可信度下降，统计分析结果不准确

### 3. 查询性能问题
- **问题**: 表字段过多，索引效率低
- **影响**: 
  - 全表扫描成本高
  - 索引维护开销大
  - JOIN查询虽然需要，但可以通过索引优化

### 4. 扩展性差
- **问题**: 添加新字段影响整表结构
- **影响**: ALTER TABLE操作成本高，影响业务

### 5. 维护困难
- **问题**: 业务逻辑复杂，难以维护
- **影响**: 代码复杂度高，错误风险大

---

## 🎯 重新设计方案

### 设计原则

1. **范式化设计**: 遵循数据库第三范式（3NF），消除数据冗余
2. **实体分离**: 将不同实体（充电桩、充电站、运营商、地区）分离到独立表
3. **关系设计**: 通过外键建立表间关系
4. **性能平衡**: 在范式化和查询性能之间取得平衡
5. **可扩展性**: 便于后续添加新字段和新功能

### 表结构关系图

```
地区表 (region)
    ↑
    | (1:N)
    |
充电站表 (charging_station)
    ↑                    ↑
    | (1:N)              | (N:1)
    |                    |
充电桩表 (charging_pile)  运营商表 (operator)
    ↑
    | (1:N)
    |
充电桩接口表 (pile_interface)
```

---

## 📊 表结构详细设计

### 1. 地区表 (region)

**用途**: 存储省市区县的地理信息

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| region_id | INT | PK, AUTO_INCREMENT | 地区ID（主键） |
| province_code | INT | NOT NULL, UNIQUE | 省份代码 |
| city_code | INT | NULL | 城市代码（省份级别时为NULL） |
| district_code | INT | NULL | 区县代码（城市级别时为NULL） |
| province_name | VARCHAR(50) | NOT NULL | 省份名称 |
| city_name | VARCHAR(50) | NULL | 城市名称 |
| district_name | VARCHAR(50) | NULL | 区县名称 |
| level | TINYINT | NOT NULL | 级别：1-省份，2-城市，3-区县 |
| parent_region_id | INT | FK(region.region_id) | 父级地区ID |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**:
- PRIMARY KEY (region_id)
- UNIQUE KEY (province_code, city_code, district_code)
- INDEX idx_province (province_code)
- INDEX idx_city (city_code)
- INDEX idx_parent (parent_region_id)

**优势**:
- 支持层级查询（省-市-区县）
- 地区信息只存储一份，避免冗余
- 便于地区维度的统计分析

---

### 2. 运营商表 (operator)

**用途**: 存储运营商信息

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| operator_id | INT | PK, AUTO_INCREMENT | 运营商ID（主键） |
| operator_code | VARCHAR(50) | NOT NULL, UNIQUE | 运营商编码 |
| operator_name | VARCHAR(100) | NOT NULL, UNIQUE | 运营商名称 |
| operator_name_cn | VARCHAR(100) | NULL | 运营商中文名称（转换后） |
| contact_phone | VARCHAR(50) | NULL | 联系电话 |
| description | TEXT | NULL | 运营商描述 |
| is_active | BOOLEAN | DEFAULT TRUE | 是否启用 |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**:
- PRIMARY KEY (operator_id)
- UNIQUE KEY uk_operator_code (operator_code)
- UNIQUE KEY uk_operator_name (operator_name)
- INDEX idx_active (is_active)

**优势**:
- 运营商信息统一管理
- 便于运营商维度的统计分析
- 运营商信息变更只需更新一处

---

### 3. 充电站表 (charging_station)

**用途**: 存储充电站基本信息

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| station_id | INT | PK, AUTO_INCREMENT | 充电站ID（主键） |
| station_code | VARCHAR(100) | NOT NULL, UNIQUE | 充电站编号（业务编号） |
| station_internal_code | VARCHAR(50) | NULL | 充电站内部编号 |
| station_name | VARCHAR(200) | NOT NULL | 充电站名称 |
| station_location | VARCHAR(500) | NULL | 充电站位置 |
| region_id | INT | FK(region.region_id) | 所属地区ID |
| road_type | VARCHAR(50) | NULL | 所处道路属性 |
| contact_phone | VARCHAR(50) | NULL | 联系电话 |
| put_into_service_date | DATE | NULL | 投入使用时间 |
| operator_id | INT | FK(operator.operator_id) | 运营商ID |
| total_piles | INT | DEFAULT 0 | 充电桩总数（冗余字段，用于快速查询） |
| status | TINYINT | DEFAULT 1 | 状态：1-正常，2-停用，3-建设中 |
| latitude | DECIMAL(10,7) | NULL | 纬度 |
| longitude | DECIMAL(10,7) | NULL | 经度 |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**:
- PRIMARY KEY (station_id)
- UNIQUE KEY uk_station_code (station_code)
- INDEX idx_region (region_id)
- INDEX idx_operator (operator_id)
- INDEX idx_status (status)
- INDEX idx_location (latitude, longitude) -- 用于地理查询
- INDEX idx_name (station_name) -- 用于名称搜索

**优势**:
- 充电站信息集中管理，避免重复存储
- 通过region_id关联地区，便于地区维度查询
- 通过operator_id关联运营商，便于运营商维度查询
- total_piles作为冗余字段，提高统计查询效率

---

### 4. 充电桩表 (charging_pile)

**用途**: 存储充电桩详细信息

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| pile_id | INT | PK, AUTO_INCREMENT | 充电桩ID（主键） |
| pile_code | VARCHAR(255) | NULL, UNIQUE | 充电桩编号（业务编号） |
| pile_internal_code | VARCHAR(100) | NULL | 充电桩内部编号 |
| station_id | INT | FK(charging_station.station_id), NOT NULL | 所属充电站ID |
| operator_id | INT | FK(operator.operator_id), NULL | 运营商ID |
| pile_type | VARCHAR(50) | NULL | 充电桩类型代码 |
| pile_type_name | VARCHAR(50) | NULL | 充电桩类型名称（转换后） |
| pile_attribute | VARCHAR(50) | NULL | 充电桩属性代码 |
| pile_attribute_name | VARCHAR(50) | NULL | 充电桩属性名称（转换后） |
| area_category | VARCHAR(50) | NULL | 所属区域分类 |
| manufacturer_code | VARCHAR(100) | NULL | 充电桩厂商编号 |
| manufacturer_name | VARCHAR(200) | NULL | 厂商名称（转换后） |
| pile_model | VARCHAR(100) | NULL | 充电桩型号 |
| production_date | DATE | NULL | 生产日期 |
| service_start_date | DATE | NULL | 设备开通时间 |
| service_time | VARCHAR(50) | NULL | 服务时间 |
| payment_method | VARCHAR(50) | NULL | 支付方式 |
| rated_voltage_max | INT | NULL | 额定电压上限 |
| rated_voltage_min | INT | NULL | 额定电压下限 |
| rated_current_max | INT | NULL | 额定电流上限 |
| rated_current_min | INT | NULL | 额定电流下限 |
| rated_power | INT | NULL | 额定功率 |
| interface_count | TINYINT | DEFAULT 0 | 接口数量 |
| alliance_certified | VARCHAR(10) | NULL | 是否获得联盟标识授权 |
| meter_number | VARCHAR(100) | NULL | 电表号 |
| remark | TEXT | NULL | 备注 |
| status | TINYINT | DEFAULT 1 | 状态：1-正常，2-故障，3-停用 |
| uid | VARCHAR(36) | NOT NULL, UNIQUE | 唯一标识ID（UUID） |
| import_time | DATE | NULL | 入库时间 |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**:
- PRIMARY KEY (pile_id)
- UNIQUE KEY uk_pile_code (pile_code)
- UNIQUE KEY uk_uid (uid)
- INDEX idx_station (station_id)
- INDEX idx_operator (operator_id)
- INDEX idx_pile_type (pile_type)
- INDEX idx_status (status)
- INDEX idx_internal_code (pile_internal_code)
- INDEX idx_manufacturer (manufacturer_code)
- INDEX idx_production_date (production_date)
- INDEX idx_service_start (service_start_date)

**优势**:
- 通过station_id关联充电站，避免重复存储充电站信息
- 通过operator_id关联运营商，便于查询和统计
- 独立的索引设计，提高查询效率
- 支持按充电桩维度的详细查询

---

### 5. 充电桩接口表 (pile_interface)

**用途**: 存储充电桩接口详细信息（如果接口信息需要单独管理）

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| interface_id | INT | PK, AUTO_INCREMENT | 接口ID（主键） |
| pile_id | INT | FK(charging_pile.pile_id), NOT NULL | 充电桩ID |
| interface_number | TINYINT | NOT NULL | 接口序号（1,2,3,4） |
| interface_standard | VARCHAR(100) | NULL | 接口标准 |
| status | TINYINT | DEFAULT 1 | 状态：1-正常，2-故障 |
| created_at | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| updated_at | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**:
- PRIMARY KEY (interface_id)
- INDEX idx_pile (pile_id)
- UNIQUE KEY uk_pile_interface (pile_id, interface_number)

**说明**:
- 此表为可选设计
- 如果接口信息简单且固定，可以直接存储在charging_pile表中
- 如果接口信息复杂或需要单独管理，建议使用此表

---

## 📈 性能优化建议

### 1. 索引策略

#### 单列索引
- **主键索引**: 所有表的主键自动创建聚簇索引
- **唯一索引**: 业务唯一字段（如充电桩编号、充电站编号）
- **外键索引**: 所有外键字段（MySQL自动创建）
- **常用查询字段**: 如status、pile_type等

#### 复合索引
- **地区查询**: (province_code, city_code, district_code)
- **地理查询**: (latitude, longitude) - 用于空间查询
- **统计查询**: (station_id, status) - 用于充电站内充电桩统计

### 2. 查询优化

#### 常见查询场景优化

**场景1: 按地区统计充电站数量**
```sql
-- 优化前（宽表）：需要扫描所有记录
SELECT 区县_中文, COUNT(DISTINCT 所属充电站编号) 
FROM evdata 
GROUP BY 区县_中文;

-- 优化后（分表）：直接JOIN，索引加速
SELECT r.district_name, COUNT(DISTINCT s.station_id)
FROM region r
JOIN charging_station s ON s.region_id = r.region_id
WHERE r.level = 3
GROUP BY r.district_name;
```

**场景2: 按运营商统计充电桩数量**
```sql
-- 优化后：利用外键索引
SELECT o.operator_name, COUNT(p.pile_id) as pile_count
FROM operator o
LEFT JOIN charging_pile p ON p.operator_id = o.operator_id
GROUP BY o.operator_id, o.operator_name;
```

**场景3: 查询某个充电站的所有充电桩**
```sql
-- 优化后：直接通过station_id索引查询
SELECT * 
FROM charging_pile 
WHERE station_id = ? 
AND status = 1;
```

### 3. 冗余字段设计

为了平衡范式化和查询性能，建议在以下场景使用冗余字段：

1. **charging_station.total_piles**: 充电桩总数
   - 用途：快速查询充电站充电桩数量
   - 更新：通过触发器或应用层维护

2. **charging_pile.operator_id**: 运营商ID（虽然可以通过station_id关联）
   - 用途：直接查询充电桩所属运营商
   - 更新：当充电站运营商变更时，需要同步更新

### 4. 分区策略（可选）

如果数据量非常大（百万级以上），可以考虑分区：

- **按地区分区**: 按region_id进行范围分区
- **按时间分区**: 按created_at进行时间分区
- **按运营商分区**: 按operator_id进行列表分区

---
