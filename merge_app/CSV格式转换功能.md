# CSV 格式转换功能说明

> 本文档描述「CSV 格式转换」模块的能力、入口、配置项与实现要点。**后续修改功能时请同步更新本文档。**

---

## 1. 功能概述

将多 Sheet 的 Excel 文件合并为单个 CSV 文件（UTF-8 编码），或按本地路径批量转换多表。各 Sheet 首行视为表头，且假定表头一致，纵向合并为一张表。

- **两种使用方式**：**上传文件**（网页上传单文件，合并后在线预览并下载 CSV）、**输入本地路径**（指定本机 Excel 路径，结果保存到磁盘）。
- **路径方式子模式**：**单表转换**（一个 Excel → 一个 CSV）、**多表同时转换**（多个 Excel → 一个 CSV 或 多个 CSV）。

---

## 2. 入口与页面结构

- **侧栏**：在功能选择中选择「📄 CSV格式转换」进入本模块。
- **主区**：
  1. 选择方式：**上传文件** / **输入本地路径**。
  2. **上传文件**：选择单文件（.xlsx / .xls）→ 点击「转换为 CSV」→ 展示合并后行数、Sheet 数、各 Sheet 行数、下载 CSV 按钮及结果预览。
  3. **输入本地路径**：
     - **单表转换**：输入一个 Excel 路径 → 点击「一键转化」→ 在同目录生成「原文件名_合并.csv」，页面展示保存路径与统计。
     - **多表同时转换**：多行输入多个路径（每行一个）→ 选择「多表转换为一个 CSV」或「多表分别转化为 CSV」→ 点击「一键转化」→ 展示每个文件的处理结果或合并后的单个文件路径。

---

## 3. 上传文件方式

- **支持格式**：.xlsx、.xls（单文件）。
- **规则**：所有 Sheet 第一行为标题行，各 Sheet 表头完全一致；纵向合并为一个 DataFrame。
- **编码**：输出 CSV 为 UTF-8；下载按钮使用 `mime="text/csv; charset=utf-8"`。
- **结果**：合并后 DataFrame 存入 `st.session_state.csv_convert_df`，统计信息存入 `csv_convert_stats`；提供「下载 CSV」及前 N 行预览（如 10 行）。

---

## 4. 输入本地路径方式

### 4.1 单表转换

- **输入**：一个 Excel 文件路径（可带引号，会自动去除首尾空格与引号）。
- **输出**：在与源文件**同目录**下生成 `原文件名_合并.csv`，编码 UTF-8，无 BOM。
- **逻辑**：读取该 Excel 所有 Sheet，按首行表头纵向合并，再写入 CSV；任一 Sheet 读取失败则整体失败。
- **Handler**：`excel_path_to_csv(input_path)`，返回 `(输出路径, 错误信息, 统计)`。

### 4.2 多表同时转换

- **输入**：多行文本，每行一个 Excel 路径；空行、非 .xlsx/.xls 行会被忽略；路径会做 strip 与去引号并转为绝对路径。
- **输出方式二选一**：
  - **多表转换为一个 CSV**：每个 Excel 先各自按 Sheet 合并为一张表，再将多表纵向拼成一张表，保存到**第一个文件所在目录**，文件名为 `多表合并_YYYYMMDD_HHMMSS.csv`。任一文件失败则整体失败。
  - **多表分别转化为 CSV**：对每个路径分别调用与单表相同的逻辑，在每个 Excel 所在目录生成「原文件名_合并.csv」；单表失败不影响其他表，页面按文件展示成功/失败及路径。
- **Handler**：
  - 路径解析：`parse_paths_from_multiline(text)` → 路径列表。
  - 多表→单 CSV：`excel_paths_to_single_csv(paths)` → `(输出路径, 错误信息, 统计)`。
  - 多表→多 CSV：`excel_paths_to_separate_csvs(paths)` → 列表，每项 `(输出路径, 错误信息, 统计)`。

---

## 5. 读取与合并规则

- **Excel 引擎**：.xls 使用 xlrd；.xlsx 优先 calamine，失败则 openpyxl（路径方式）；上传方式由 `csv_convert_handler.excel_sheets_to_csv` 根据扩展名选择 engine。
- **合并方式**：`pd.read_excel(..., sheet_name=None, header=0)` 读入所有 Sheet，再 `pd.concat(dfs, axis=0, ignore_index=True)` 纵向合并；空 Sheet 会计入统计但不出现在合并结果中。
- **统计信息**：通常包含 `sheet_names`、`row_counts`、`total_rows`、`sheet_count`；多表→单 CSV 时还有 `file_count`、`rows_per_file`。

---

## 6. 涉及文件与模块

| 文件 / 位置 | 说明 |
|-------------|------|
| `merge_app/app.py` | 侧栏入口、上传/路径切换、单表/多表子选项、路径输入与一键转化按钮、结果展示与下载、统计展示（含各 Sheet 行数 expander）。 |
| `merge_app/handlers/csv_convert_handler.py` | 上传合并 `excel_sheets_to_csv`；路径单表 `excel_path_to_csv`、内部 `_read_one_file_merged`；路径解析 `parse_paths_from_multiline`；多表→单 CSV `excel_paths_to_single_csv`；多表→多 CSV `excel_paths_to_separate_csvs`。 |
| `merge_app/requirements.txt` | 依赖：pandas、openpyxl、xlrd、python-calamine（.xlsx 可选）。 |

---

## 7. 修改功能时的文档同步建议

- 新增「选择方式」或子模式（如仅多表、仅单表）：更新 **2. 入口与页面结构**、**4. 输入本地路径方式**。
- 修改输出路径规则或文件名：更新 **4.1**、**4.2** 中的路径与文件名说明。
- 修改编码（如改为 UTF-8-BOM）：更新 **3**、**4.1** 及 handler 中 `to_csv(encoding=...)` 说明。
- 修改 Excel 引擎或 Sheet 读取规则：更新 **5. 读取与合并规则** 及 handler 对应函数说明。
- 新增/删除统计字段：更新 **5** 中统计信息说明及 **8** 中 handler 返回值说明。
