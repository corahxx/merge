# PyInstaller spec: 打包 Merge 多表合并为 exe
# 在 merge_app 目录下执行: pyinstaller merge_exe.spec

import os
from PyInstaller.utils.hooks import collect_all

block_cipher = None

# 将 app.py 和 handlers 打进包，运行时解压到 MEIPASS（PyInstaller 会注入 SPEC）
app_dir = os.path.dirname(os.path.abspath(SPEC or __file__))
datas = [
    (os.path.join(app_dir, 'app.py'), '.'),
    (os.path.join(app_dir, 'handlers', '__init__.py'), 'handlers'),
    (os.path.join(app_dir, 'handlers', 'table_merge_handler.py'), 'handlers'),
    (os.path.join(app_dir, 'handlers', 'station_merge_handler.py'), 'handlers'),
]

# 用 collect_all 把 streamlit 整包打进 exe（若当前环境识别为包）
# 若 streamlit 未被识别为包，PyInstaller 会跳过；依赖仍会通过 run_merge 的 import 被拉取
try:
    st_datas, st_binaries, st_hidden = collect_all('streamlit')
    datas += st_datas
    binaries = list(st_binaries)
    hidden_imports = list(st_hidden)
except Exception:
    binaries = []
    hidden_imports = []
hidden_imports += [
    'pandas', 'openpyxl', 'xlrd',
    'altair', 'blake2b', 'click', 'packaging', 'protobuf', 'tornado', 'pyarrow',
]

a = Analysis(
    [os.path.join(app_dir, 'run_merge.py')],
    pathex=[app_dir],
    binaries=binaries,
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    name='Merge多表合并',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,   # True 方便看到报错；打成 exe 后若希望无黑窗可改为 False
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
