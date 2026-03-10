# run_merge.py - 启动器：直接运行或打包成 exe 后双击运行
# 确保工作目录正确并启动 Streamlit

import os
import sys
import traceback

# 顶层导入供 PyInstaller 收集，避免 exe 内缺 streamlit
import streamlit.web.cli as _stcli

def _get_base_dir():
    """ exe 运行时为解压目录 MEIPASS，否则为脚本所在目录 """
    if getattr(sys, "frozen", False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

def main():
    base = _get_base_dir()
    os.chdir(base)
    # 将 app 所在目录加入 path，便于 app.py 里 import handlers
    if base not in sys.path:
        sys.path.insert(0, base)

    # 使用 app.py 的绝对路径，避免打包后 streamlit 找不到脚本
    app_path = os.path.join(base, "app.py")
    if not os.path.isfile(app_path):
        print("错误：找不到 app.py，路径:", app_path)
        input("按回车键退出...")
        sys.exit(1)

    sys.argv = [
        "streamlit", "run", app_path,
        "--server.port", "8501",
        "--browser.gatherUsageStats", "false",
    ]
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    _stcli.main()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("启动失败:")
        traceback.print_exc()
        input("按回车键退出...")
        sys.exit(1)
