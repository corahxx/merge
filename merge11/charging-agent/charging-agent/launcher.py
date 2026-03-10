#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
充电桩数据管理系统 - 轻量级启动器
只打包启动器和Python环境，源代码动态加载
"""

import sys
import os
import subprocess
import webbrowser
import time
from pathlib import Path

# 修复Windows控制台编码问题
# 必须在所有print语句之前执行
USE_EMOJI = True
if sys.platform == 'win32':
    try:
        # 方法1: 尝试设置控制台代码页为UTF-8
        import subprocess
        subprocess.run(['chcp', '65001'], shell=True, capture_output=True)
        
        # 方法2: 尝试重新配置stdout/stderr
        if hasattr(sys.stdout, 'reconfigure'):
            try:
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
                sys.stderr.reconfigure(encoding='utf-8', errors='replace')
            except:
                pass
        
        # 方法3: 如果还是失败，使用ASCII字符
        try:
            # 测试是否能输出emoji
            test_char = '🔧'
            sys.stdout.buffer.write(test_char.encode('utf-8'))
            sys.stdout.buffer.flush()
        except:
            USE_EMOJI = False
    except:
        USE_EMOJI = False

# 定义安全的打印函数（如果编码失败，使用ASCII）
def safe_print(*args, **kwargs):
    """安全的打印函数，自动处理编码问题"""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        # 如果编码失败，移除emoji后重试
        args_str = ' '.join(str(arg) for arg in args)
        # 移除常见的emoji字符
        args_str = args_str.replace('🔧', '[TOOL]').replace('✅', '[OK]').replace('❌', '[ERROR]')
        args_str = args_str.replace('⚠️', '[WARN]').replace('🚀', '[START]').replace('📁', '[DIR]')
        args_str = args_str.replace('📂', '[FOLDER]').replace('📄', '[FILE]').replace('🐍', '[PYTHON]')
        args_str = args_str.replace('📋', '[CMD]').replace('📝', '[INFO]').replace('💡', '[TIP]')
        args_str = args_str.replace('🔍', '[CHECK]').replace('⏳', '[WAIT]').replace('📱', '[BROWSER]')
        args_str = args_str.replace('🔗', '[LINK]')
        print(args_str, **kwargs)


def setup_python_path():
    """
    设置Python路径，添加源代码目录
    支持打包后的环境和开发环境
    """
    # 获取EXE所在目录或脚本所在目录
    if getattr(sys, 'frozen', False):
        # 打包后的路径
        BASE_DIR = Path(sys.executable).parent.resolve()
        safe_print(f"{'🔧' if USE_EMOJI else '[EXE]'} EXE模式: 基础目录 = {BASE_DIR}")
    else:
        # 开发环境路径
        BASE_DIR = Path(__file__).parent.resolve()
        safe_print(f"{'🔧' if USE_EMOJI else '[DEV]'} 开发模式: 基础目录 = {BASE_DIR}")
    
    # 源代码目录（与启动器同级）
    APP_DIR = BASE_DIR / "app"
    
    # 如果app目录不存在，尝试使用当前目录（开发环境）
    if not APP_DIR.exists():
        safe_print(f"{'⚠️' if USE_EMOJI else '[WARN]'}  app目录不存在: {APP_DIR}")
        APP_DIR = BASE_DIR
        safe_print(f"   使用基础目录: {APP_DIR}")
    else:
        safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 找到app目录: {APP_DIR}")
    
    # 验证目录存在
    if not APP_DIR.exists():
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 错误: 应用目录不存在: {APP_DIR}")
        return None
    
    # 添加到Python路径
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
        safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 已添加Python路径: {APP_DIR}")
    
    # 显示目录内容（用于调试）
    try:
        files = list(APP_DIR.glob("*.py"))[:5]  # 只显示前5个
        if files:
            print(f"   目录中的Python文件: {[f.name for f in files]}")
    except:
        pass
    
    return APP_DIR


def check_config(APP_DIR):
    """检查配置文件是否存在"""
    config_file = APP_DIR / "config.py"
    return config_file.exists()


def load_config(APP_DIR):
    """加载配置"""
    try:
        sys.path.insert(0, str(APP_DIR))
        from config import DB_CONFIG
        return DB_CONFIG
    except Exception as e:
        print(f"⚠️  加载配置失败: {str(e)}")
        return None


def test_connection(APP_DIR):
    """测试数据库连接"""
    try:
        sys.path.insert(0, str(APP_DIR))
        from utils.db_utils import test_connection
        success, message = test_connection()
        return success, message
    except Exception as e:
        return False, f"连接测试异常: {str(e)}"


def show_config_gui(APP_DIR):
    """显示配置界面（tkinter）"""
    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except ImportError:
        print("❌ 错误: 无法导入tkinter，请确保Python已正确安装")
        return False
    
    root = tk.Tk()
    root.title("充电桩数据管理系统 - 配置向导")
    root.geometry("550x550")
    root.resizable(False, False)
    
    # 居中显示
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')
    
    # 配置变量
    host_var = tk.StringVar(value="localhost")
    port_var = tk.StringVar(value="3306")
    user_var = tk.StringVar(value="root")
    password_var = tk.StringVar()
    database_var = tk.StringVar(value="evdata")
    llm_enabled_var = tk.BooleanVar(value=False)  # LLM开关，默认关闭
    
    # 尝试加载现有配置
    try:
        sys.path.insert(0, str(APP_DIR))
        from config import DB_CONFIG
        host_var.set(DB_CONFIG.get('host', 'localhost'))
        port_var.set(str(DB_CONFIG.get('port', 3306)))
        user_var.set(DB_CONFIG.get('user', 'root'))
        password_var.set(DB_CONFIG.get('password', ''))
        database_var.set(DB_CONFIG.get('database', 'evdata'))
    except:
        pass
    
    # 尝试加载LLM配置
    try:
        from config import LLM_CONFIG
        llm_enabled_var.set(LLM_CONFIG.get('enabled', False))
    except:
        pass
    
    # 标题
    title_label = ttk.Label(
        root, 
        text="数据库配置", 
        font=("Arial", 14, "bold")
    )
    title_label.pack(pady=15)
    
    # 主框架
    frame = ttk.Frame(root, padding=20)
    frame.pack(fill=tk.BOTH, expand=True)
    
    # 配置输入框
    ttk.Label(frame, text="数据库主机:", font=("Arial", 10)).grid(
        row=0, column=0, sticky=tk.W, pady=8, padx=5
    )
    host_entry = ttk.Entry(frame, textvariable=host_var, width=35, font=("Arial", 10))
    host_entry.grid(row=0, column=1, pady=8, padx=5)
    
    ttk.Label(frame, text="数据库端口:", font=("Arial", 10)).grid(
        row=1, column=0, sticky=tk.W, pady=8, padx=5
    )
    port_entry = ttk.Entry(frame, textvariable=port_var, width=35, font=("Arial", 10))
    port_entry.grid(row=1, column=1, pady=8, padx=5)
    
    ttk.Label(frame, text="数据库用户:", font=("Arial", 10)).grid(
        row=2, column=0, sticky=tk.W, pady=8, padx=5
    )
    user_entry = ttk.Entry(frame, textvariable=user_var, width=35, font=("Arial", 10))
    user_entry.grid(row=2, column=1, pady=8, padx=5)
    
    ttk.Label(frame, text="数据库密码:", font=("Arial", 10)).grid(
        row=3, column=0, sticky=tk.W, pady=8, padx=5
    )
    password_entry = ttk.Entry(frame, textvariable=password_var, width=35, show="*", font=("Arial", 10))
    password_entry.grid(row=3, column=1, pady=8, padx=5)
    
    ttk.Label(frame, text="数据库名称:", font=("Arial", 10)).grid(
        row=4, column=0, sticky=tk.W, pady=8, padx=5
    )
    database_entry = ttk.Entry(frame, textvariable=database_var, width=35, font=("Arial", 10))
    database_entry.grid(row=4, column=1, pady=8, padx=5)
    
    # 分隔线
    ttk.Separator(frame, orient='horizontal').grid(
        row=5, column=0, columnspan=2, sticky='ew', pady=15
    )
    
    # 高级功能标题
    ttk.Label(frame, text="【高级功能】", font=("Arial", 10, "bold")).grid(
        row=6, column=0, columnspan=2, sticky=tk.W, pady=(0, 5)
    )
    
    # LLM开关
    llm_check = ttk.Checkbutton(
        frame, 
        text="启用AI大模型功能（需要本地Ollama服务）",
        variable=llm_enabled_var
    )
    llm_check.grid(row=7, column=0, columnspan=2, sticky=tk.W, padx=5)
    
    # LLM说明
    ttk.Label(
        frame, 
        text="  ℹ️ 启用后可使用智能对话和AI深度分析",
        foreground="gray",
        font=("Arial", 9)
    ).grid(row=8, column=0, columnspan=2, sticky=tk.W, padx=5)
    
    ttk.Label(
        frame, 
        text="  ℹ️ 不启用时数据统计和图表功能完全正常",
        foreground="gray",
        font=("Arial", 9)
    ).grid(row=9, column=0, columnspan=2, sticky=tk.W, padx=5)
    
    # 状态标签
    status_label = ttk.Label(frame, text="", font=("Arial", 9))
    status_label.grid(row=10, column=0, columnspan=2, pady=10)
    
    def save_and_test():
        """保存配置并测试连接"""
        # 验证输入
        if not host_var.get().strip():
            messagebox.showerror("错误", "请输入数据库主机地址")
            return
        
        if not port_var.get().strip():
            messagebox.showerror("错误", "请输入数据库端口")
            return
        
        try:
            port = int(port_var.get())
        except ValueError:
            messagebox.showerror("错误", "端口必须是数字")
            return
        
        if not user_var.get().strip():
            messagebox.showerror("错误", "请输入数据库用户名")
            return
        
        if not database_var.get().strip():
            messagebox.showerror("错误", "请输入数据库名称")
            return
        
        # 生成配置文件
        config_content = f"""# config.py - 系统配置
# 支持环境变量覆盖，优先级：环境变量 > 配置文件

import os

DB_CONFIG = {{
    'host': os.getenv('DB_HOST', '{host_var.get().strip()}'),
    'port': int(os.getenv('DB_PORT', '{port}')),
    'user': os.getenv('DB_USER', '{user_var.get().strip()}'),
    'password': os.getenv('DB_PASSWORD', '{password_var.get()}'),
    'database': os.getenv('DB_NAME', '{database_var.get().strip()}')
}}

# AI大模型配置
LLM_CONFIG = {{
    'enabled': {str(llm_enabled_var.get())},  # 是否启用AI大模型功能
    'model': 'qwen3:30b',  # 模型名称
}}
"""
        config_path = APP_DIR / "config.py"
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(config_content)
        except Exception as e:
            messagebox.showerror("错误", f"保存配置文件失败: {str(e)}")
            return
        
        # 测试连接
        status_label.config(text="正在测试连接...", foreground="blue")
        root.update()
        
        success, message = test_connection(APP_DIR)
        if success:
            status_label.config(text="✅ 连接成功！", foreground="green")
            messagebox.showinfo("成功", "配置已保存，数据库连接测试成功！")
            root.destroy()
            return True
        else:
            status_label.config(text=f"❌ {message}", foreground="red")
            messagebox.showerror("连接失败", f"数据库连接失败：\n{message}\n\n请检查配置信息是否正确")
            return False
    
    # 按钮框架
    button_frame = ttk.Frame(frame)
    button_frame.grid(row=11, column=0, columnspan=2, pady=15)
    
    test_button = ttk.Button(
        button_frame, 
        text="测试连接并保存", 
        command=save_and_test,
        width=20
    )
    test_button.pack(side=tk.LEFT, padx=5)
    
    cancel_button = ttk.Button(
        button_frame, 
        text="取消", 
        command=root.destroy,
        width=15
    )
    cancel_button.pack(side=tk.LEFT, padx=5)
    
    # 绑定回车键
    root.bind('<Return>', lambda e: save_and_test())
    
    # 运行GUI
    root.mainloop()
    
    # 检查配置是否已保存
    return check_config(APP_DIR)


def check_port_available(port=8501):
    """检查端口是否可用"""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        return result != 0  # 0表示端口被占用
    except:
        return True


def kill_process_on_port(port=8501):
    """杀掉占用指定端口的进程（仅Windows）"""
    import subprocess
    try:
        # 使用 netstat 查找占用端口的进程 PID
        result = subprocess.run(
            ['netstat', '-ano'],
            capture_output=True,
            text=True,
            encoding='gbk',  # Windows 默认编码
            errors='ignore'
        )
        
        pids_to_kill = set()
        for line in result.stdout.splitlines():
            # 查找包含端口号的行，格式如: TCP    127.0.0.1:8501    ...    LISTENING    12345
            if f':{port}' in line and ('LISTENING' in line or 'ESTABLISHED' in line):
                parts = line.split()
                if parts:
                    pid = parts[-1]  # PID 是最后一列
                    if pid.isdigit() and int(pid) > 0:
                        pids_to_kill.add(pid)
        
        if not pids_to_kill:
            return False, "未找到占用端口的进程"
        
        # 杀掉找到的进程
        killed = []
        for pid in pids_to_kill:
            try:
                subprocess.run(
                    ['taskkill', '/PID', pid, '/F'],
                    capture_output=True,
                    text=True,
                    encoding='gbk',
                    errors='ignore'
                )
                killed.append(pid)
            except:
                pass
        
        if killed:
            # 等待进程完全退出
            import time
            time.sleep(1)
            return True, f"已终止进程: {', '.join(killed)}"
        else:
            return False, "无法终止进程"
            
    except Exception as e:
        return False, f"操作失败: {str(e)}"


def _run_streamlit_child():
    """
    子进程模式（给EXE自己用）：
    由父进程通过 subprocess 启动：
        charging-agent-launcher.exe --_run_streamlit <APP_DIR> <ENTRY_PY>
    该模式会在当前进程内直接调用 streamlit.web.cli.main()
    """
    try:
        idx = sys.argv.index("--_run_streamlit")
        app_dir = Path(sys.argv[idx + 1]).resolve()
        entry_py = Path(sys.argv[idx + 2]).resolve()
    except Exception:
        print("[ERROR] invalid args for --_run_streamlit", flush=True)
        print(f"[DEBUG] argv={sys.argv}", flush=True)
        raise SystemExit(2)

    # 尽量保证输出编码一致（避免乱码/emoji问题）
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    print("[DEBUG] streamlit child mode", flush=True)
    print(f"[DEBUG] sys.executable={sys.executable}", flush=True)
    print(f"[DEBUG] app_dir={app_dir}", flush=True)
    print(f"[DEBUG] entry_py={entry_py} exists={entry_py.exists()}", flush=True)

    # 设置cwd与sys.path（确保能import app目录内模块）
    try:
        os.chdir(str(app_dir))
    except Exception as e:
        print(f"[ERROR] chdir failed: {e}", file=sys.stderr, flush=True)
        raise SystemExit(3)

    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))

    # 运行 streamlit
    try:
        import streamlit.web.cli as stcli
    except Exception as e:
        print(f"[ERROR] import streamlit failed: {e}", file=sys.stderr, flush=True)
        raise SystemExit(4)

    # 模拟命令行：streamlit run entry_py ...
    # 注意：必须禁用 developmentMode，否则 server.port 会报错
    sys.argv = [
        "streamlit",
        "run",
        str(entry_py),
        "--global.developmentMode=false",
        "--server.headless=true",
        "--server.port=8501",
        "--server.maxUploadSize=600",
    ]
    print(f"[DEBUG] streamlit argv={sys.argv}", flush=True)

    # stcli.main() 会触发 SystemExit；交给外层处理
    stcli.main()


def start_streamlit(APP_DIR, debug_mode=False):
    """
    重写后的启动逻辑：
    1. 自动适配 app.py 或 main.py
    2. 增强 EXE 模式下的环境注入
    3. 修复编码乱码
    """
    # --- 1. 自动识别入口文件 ---
    # main.py 为主程序入口
    possible_entries = ["main.py", "app.py", "data_manager.py", "agent.py"]
    main_script = None
    
    for entry in possible_entries:
        target = APP_DIR / entry
        if target.exists():
            main_script = target
            break
            
    if not main_script:
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 错误: 在 {APP_DIR} 中未找到任何有效的入口文件 {possible_entries}")
        return None

    # --- 2. 准备启动环境与命令 ---
    # 无论是否是 EXE，我们都通过 sys.executable 调用
    # 在打包后的 EXE 中，sys.executable 就是运行环境本身
    
    log_file = APP_DIR / "streamlit_launcher.log"
    log_handle = None
    try:
        log_handle = open(log_file, "w", encoding="utf-8", buffering=1)
        log_handle.write(f"Streamlit启动日志 - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_handle.write(f"工作目录: {APP_DIR}\n入口文件: {main_script}\n")
        log_handle.write("=" * 60 + "\n")
        log_handle.flush()
    except Exception as e:
        safe_print(f"{'⚠️' if USE_EMOJI else '[WARN]'} 日志文件创建失败: {e}")
        log_handle = None

    if getattr(sys, 'frozen', False):
        # EXE模式：启动“自身”进入子进程模式，由子进程内直接调用 streamlit.web.cli.main()
        cmd = [sys.executable, "--_run_streamlit", str(APP_DIR), str(main_script)]
        safe_print(f"{'📝' if USE_EMOJI else '[INFO]'} EXE模式启动，识别到入口: {main_script.name}")
    else:
        # 开发模式启动
        cmd = [sys.executable, "-m", "streamlit", "run", str(main_script), "--server.headless", "true"]

    # --- 3. 执行并捕获输出 (解决乱码的关键) ---
    safe_print(f"{'🚀' if USE_EMOJI else '[START]'} 正在启动 Streamlit...")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(APP_DIR),
            text=True,
            encoding='utf-8',   # 强制 UTF-8
            errors='replace',
            bufsize=1,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"}
        )

        # 后台持续写入日志（成功启动时也能留下完整输出，便于排查）
        if process.stdout and log_handle:
            import threading

            def _drain_stdout():
                try:
                    for line in iter(process.stdout.readline, ""):
                        if not line:
                            break
                        log_handle.write(line)
                        log_handle.flush()
                        if debug_mode:
                            print(line, end="", flush=True)
                except Exception:
                    pass

            t = threading.Thread(target=_drain_stdout, daemon=True)
            t.start()
            process._log_thread = t
            process._log_handle = log_handle
        elif log_handle:
            # 没有stdout也要保证日志句柄能在外层关闭
            process._log_handle = log_handle
        
        # --- 4. 实时监控启动状态 ---
        time.sleep(3) # 给一点缓冲时间
        
        if process.poll() is not None:
            # 如果进程此时已经结束，说明启动即报错
            try:
                output, _ = process.communicate(timeout=5)
            except Exception:
                try:
                    process.kill()
                    output, _ = process.communicate(timeout=2)
                except Exception:
                    output = ""

            output = output or ""

            if log_handle:
                log_handle.write(output)
                log_handle.flush()

            safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 启动失败，请检查日志: {log_file}")
            print(f"退出码: {process.returncode}")
            print("-" * 30 + " 报错详情 " + "-" * 30)
            print(output)
            print("-" * 70)
            if log_handle:
                log_handle.close()
            return None

        # --- 5. 验证健康检查端点 ---
        import urllib.request
        for i in range(15):  # 最多等待 15 秒
            try:
                # Streamlit 启动后会有健康检查接口
                resp = urllib.request.urlopen("http://localhost:8501/_stcore/health", timeout=1)
                if resp.getcode() == 200:
                    safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 服务已就绪！")
                    webbrowser.open("http://localhost:8501")
                    return process
            except:
                print(f"\r  正在同步环境并启动服务... ({i+1}/15)", end="", flush=True)
                time.sleep(1)
        
        safe_print(f"\n{'⚠️' if USE_EMOJI else '[WARN]'} 服务启动较慢，请稍后手动刷新浏览器。")
        # 日志句柄由main在process结束时关闭（或此处如果未挂到process上也可关闭）
        return process

    except Exception as e:
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 启动器发生异常: {str(e)}")
        if log_handle:
            try:
                log_handle.write(f"\n[ERROR] launcher exception: {e}\n")
                log_handle.flush()
                log_handle.close()
            except:
                pass
        return None


def main():
    """主函数"""
    # 子进程模式：直接运行streamlit（必须在任何防重入逻辑之前）
    if "--_run_streamlit" in sys.argv:
        try:
            _run_streamlit_child()
        except SystemExit:
            raise
        except Exception as e:
            print(f"[ERROR] streamlit child crashed: {e}", flush=True)
            import traceback
            traceback.print_exc()
            raise SystemExit(5)

    # 检查是否已经在运行（防止重复启动）
    if os.getenv('_STREAMLIT_LAUNCHER_') == '1':
        # 这是从临时脚本调用的，不应该执行启动器逻辑
        return
    
    # 设置环境变量，标记启动器已运行
    os.environ['_STREAMLIT_LAUNCHER_'] = '1'
    
    # 检查端口是否被占用
    if not check_port_available(8501):
        safe_print(f"{'⚠️' if USE_EMOJI else '[WARN]'}  端口8501已被占用")
        print("   可能已有Streamlit实例在运行")
        print()
        print("   请选择操作:")
        print("   [Y] 自动关闭旧进程并启动新实例")
        print("   [N] 取消启动")
        print("   [S] 跳过检查，强制尝试启动")
        response = input("\n请选择 (Y/N/S): ").strip().upper()
        
        if response == 'Y':
            safe_print(f"{'🔄' if USE_EMOJI else '[KILL]'} 正在关闭占用端口的进程...")
            success, msg = kill_process_on_port(8501)
            if success:
                safe_print(f"{'✅' if USE_EMOJI else '[OK]'} {msg}")
                # 再次检查端口
                if not check_port_available(8501):
                    safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 端口仍被占用，请手动关闭进程")
                    input("\n按回车键退出...")
                    return
            else:
                safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} {msg}")
                print("   请尝试手动关闭进程：")
                print("   1. 打开任务管理器 (Ctrl+Shift+Esc)")
                print("   2. 找到 Python 或 Streamlit 相关进程")
                print("   3. 右键 → 结束任务")
                input("\n按回车键退出...")
                return
        elif response == 'S':
            safe_print(f"{'⚠️' if USE_EMOJI else '[WARN]'}  跳过端口检查，继续尝试启动...")
        else:
            return
    
    print("=" * 60)
    print("  充电桩数据管理系统 - 启动器")
    print("=" * 60)
    print()
    
    # 1. 设置Python路径
    APP_DIR = setup_python_path()
    if APP_DIR is None:
        input("\n按回车键退出...")
        return
    safe_print(f"{'📁' if USE_EMOJI else '[DIR]'} 应用目录: {APP_DIR}")
    
    # 2. 检查配置
    if not check_config(APP_DIR):
        safe_print(f"{'⚠️' if USE_EMOJI else '[WARN]'}  配置文件不存在，启动配置向导...")
        print()
        if not show_config_gui(APP_DIR):
            safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 配置未完成，退出")
            input("\n按回车键退出...")
            return
        print()
    
    # 3. 加载配置
    config = load_config(APP_DIR)
    if not config:
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 配置文件加载失败")
        response = input("是否重新配置? (Y/N): ")
        if response.upper() == 'Y':
            if not show_config_gui(APP_DIR):
                safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 配置未完成，退出")
                input("\n按回车键退出...")
                return
        else:
            input("\n按回车键退出...")
            return
    
    # 4. 测试连接
    safe_print(f"{'🔍' if USE_EMOJI else '[CHECK]'} 测试数据库连接...")
    success, message = test_connection(APP_DIR)
    if not success:
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} {message}")
        print()
        response = input("是否重新配置? (Y/N): ")
        if response.upper() == 'Y':
            if show_config_gui(APP_DIR):
                success, message = test_connection(APP_DIR)
                if not success:
                    safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 连接失败: {message}")
                    input("\n按回车键退出...")
                    return
            else:
                input("\n按回车键退出...")
                return
        else:
            input("\n按回车键退出...")
            return
    
    safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 数据库连接成功")
    print()
    
    # 5. 启动应用
    safe_print(f"{'🚀' if USE_EMOJI else '[START]'} 正在启动应用...")
    
    # 检查是否有调试参数
    debug_mode = '--debug' in sys.argv or '-d' in sys.argv
    
    if debug_mode:
        safe_print(f"{'🔧' if USE_EMOJI else '[DEBUG]'} 调试模式已启用")
        print("   提示: 如果正常模式无法启动，使用调试模式可以看到详细错误信息")
        print()
    else:
        # 如果正常模式失败，提示使用调试模式
        print("   提示: 如果启动失败，可以修改launcher.py启用调试模式")
        print("   或使用: python launcher.py --debug")
        print()
    
    process = start_streamlit(APP_DIR, debug_mode=debug_mode)
    
    if process is None:
        safe_print(f"{'❌' if USE_EMOJI else '[ERROR]'} 应用启动失败")
        safe_print(f"\n{'💡' if USE_EMOJI else '[TIP]'} 故障排除建议:")
        print("   1. 检查Python环境是否正确")
        print("   2. 检查所有依赖是否已安装: pip install -r requirements.txt")
        print("   3. 检查源代码文件是否存在且可读")
        print("   4. 尝试手动运行: streamlit run main.py")
        print("   5. 查看上面的错误信息")
        input("\n按回车键退出...")
        return
    
    print()
    print("=" * 60)
    safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 应用已启动")
    safe_print(f"{'📱' if USE_EMOJI else '[BROWSER]'} 浏览器将自动打开")
    safe_print(f"{'🔗' if USE_EMOJI else '[LINK]'} 访问地址: http://localhost:8501")
    print()
    safe_print(f"{'💡' if USE_EMOJI else '[TIP]'} 提示: 关闭此窗口将停止应用")
    print("=" * 60)
    print()
    
    try:
        # 等待进程结束
        # 注意：由于stdout和stderr被重定向到PIPE，如果需要实时查看输出，
        # 可以在开发环境中直接运行streamlit命令查看
        process.wait()
        # 关闭日志句柄（如果有）
        try:
            if hasattr(process, "_log_handle") and process._log_handle:
                process._log_handle.close()
        except Exception:
            pass
    except KeyboardInterrupt:
        print("\n\n正在关闭应用...")
        try:
            process.terminate()
            process.wait(timeout=5)
        except:
            try:
                process.kill()
            except:
                pass
        safe_print(f"{'✅' if USE_EMOJI else '[OK]'} 应用已关闭")
    except Exception as e:
        safe_print(f"\n{'⚠️' if USE_EMOJI else '[WARN]'}  应用异常退出: {str(e)}")
        # 尝试获取错误输出
        try:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                if stderr:
                    print("错误输出:")
                    print(stderr[-500:])  # 只显示最后500字符
        except:
            pass
    
    input("\n按回车键退出...")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 使用普通print避免编码问题
        print(f"\n[ERROR] 发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        input("\n按回车键退出...")
