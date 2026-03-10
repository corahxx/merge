# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller配置文件 - 轻量级启动器
只打包启动器和Python环境，源代码以文件夹形式存在
"""

from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules

block_cipher = None

# 收集 streamlit 的所有依赖（关键！否则会报 No package metadata）
streamlit_datas, streamlit_binaries, streamlit_hiddenimports = collect_all('streamlit')

# 收集其他关键包的数据文件
altair_datas = collect_data_files('altair')
plotly_datas = collect_data_files('plotly')
pandas_datas = collect_data_files('pandas')

a = Analysis(
    ['launcher.py'],
    pathex=[],
    binaries=streamlit_binaries,
    datas=streamlit_datas + altair_datas + plotly_datas + pandas_datas,
    hiddenimports=streamlit_hiddenimports + [
        # Streamlit 额外依赖
        'streamlit.web.cli',
        'streamlit.runtime',
        'streamlit.runtime.scriptrunner',
        'altair',
        'pydeck',
        'validators',
        'gitpython',
        'watchdog',
        'tornado',
        'toml',
        'pyarrow',
        'packaging',
        'importlib_metadata',
        # 数据库相关
        'pymysql',
        'pymysql._auth',  # PyMySQL认证模块
        'cryptography',  # MySQL 8.0+ 新认证方法必需
        'cryptography.hazmat',
        'cryptography.hazmat.backends',
        'cryptography.hazmat.primitives',
        'sqlalchemy',
        'sqlalchemy.engine',
        'sqlalchemy.pool',
        'sqlalchemy.dialects.mysql',
        'sqlalchemy.dialects.mysql.pymysql',
        # 数据处理相关
        'pandas',
        'openpyxl',
        'xlrd',
        'numpy',
        # 可视化相关
        'plotly',
        'plotly.graph_objects',
        'plotly.express',
        # PDF生成
        'reportlab',
        # 账号认证相关
        'bcrypt',
        'bcrypt._bcrypt',
        # GUI相关
        'tkinter',
        'tkinter.ttk',
        'tkinter.messagebox',
        # 其他
        'urllib.request',
        'webbrowser',
        'email.mime.text',
        'email.mime.multipart',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # 排除不需要的模块，减小体积
        'matplotlib',
        'numpy.tests',
        'pandas.tests',
        'scipy',
        'IPython',
        'jupyter',
        'notebook',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='charging-agent-launcher',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,  # 使用UPX压缩
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # 显示控制台（用于调试和查看日志）
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # 可以添加图标文件路径，如: 'icon.ico'
)
