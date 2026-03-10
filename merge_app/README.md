# Merge 多表合并

独立网页应用：上传多个运营商 Excel/CSV，按统一表头自动识别并纵向合并，无需数据库与登录。

## 依赖

- Python 3.8+
- 见 `requirements.txt`：streamlit、pandas、openpyxl、xlrd

## 运行方式

### 方式一：双击运行（需已安装 Python）

1. 安装依赖：`pip install -r requirements.txt`
2. 双击 **`双击运行 Merge.bat`**，浏览器会自动打开页面（通常为 http://localhost:8501）。

### 方式二：生成 exe，无需 Python 即可双击运行

1. 在 `merge_app` 目录下双击 **`build_exe.bat`**，等待打包完成。
2. 生成的可执行文件在 `dist\Merge多表合并.exe`。
3. 将 `Merge多表合并.exe` 拷贝到任意位置，双击即可运行（本机无需安装 Python）；运行时会自动打开浏览器。

### 方式三：命令行

在 `merge_app` 目录下执行：

```bash
pip install -r requirements.txt
streamlit run app.py
```

或执行 `python run_merge.py` 亦可。

## 说明

- 支持格式：`.xlsx`、`.xls`、`.csv`
- 表头与多 Sheet 规则见页面内「合并规则说明」
- 本应用不连接数据库，不需要登录或配置
