# check_excel_file.py - Excel文件完整性检查工具

import sys
import os
import zipfile
import pandas as pd
from pathlib import Path

def check_excel_file(file_path):
    """检查Excel文件完整性"""
    file_path = Path(file_path)
    
    print("="*60)
    print(f"检查文件: {file_path.name}")
    print("="*60)
    
    # 检查1: 文件是否存在
    if not file_path.exists():
        print("❌ 文件不存在")
        return False
    
    # 检查2: 文件大小
    file_size = file_path.stat().st_size
    file_size_mb = file_size / 1024 / 1024
    print(f"📊 文件大小: {file_size_mb:.2f} MB")
    
    if file_size == 0:
        print("❌ 文件大小为0，文件可能损坏")
        return False
    
    # 检查3: ZIP结构（XLSX）
    if file_path.suffix.lower() == '.xlsx':
        print("\n🔍 检查XLSX文件结构（ZIP格式）...")
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                bad_file = zf.testzip()
                if bad_file:
                    print(f"❌ ZIP文件损坏: {bad_file}")
                    print("   建议：文件可能不完整，请重新下载或复制")
                    return False
                else:
                    print("✅ ZIP结构正常")
                    # 列出主要文件
                    file_list = zf.namelist()
                    if 'xl/workbook.xml' in file_list:
                        print("✅ 找到工作簿文件")
                    if any('xl/worksheets' in f for f in file_list):
                        print("✅ 找到工作表文件")
        except zipfile.BadZipFile:
            print("❌ 不是有效的ZIP文件")
            return False
        except Exception as e:
            print(f"❌ 无法打开ZIP文件: {e}")
            return False
    
    # 检查4: 尝试读取少量数据
    print("\n🔍 尝试读取数据...")
    try:
        # 先尝试读取前10行
        df = pd.read_excel(file_path, nrows=10)
        print(f"✅ 可以读取数据")
        print(f"   - 列数: {len(df.columns)}")
        print(f"   - 列名（前5个）: {list(df.columns[:5])}")
        
        # 尝试获取总行数（对于大文件可能很慢）
        if file_size_mb < 100:  # 小于100MB才尝试读取全部
            print("\n🔍 计算总行数...")
            try:
                df_full = pd.read_excel(file_path)
                print(f"✅ 总行数: {len(df_full):,}")
                print(f"✅ 文件完整性检查通过！")
                return True
            except Exception as e:
                print(f"⚠️  无法读取全部数据: {e}")
                print("   但前10行可以读取，文件可能部分损坏")
                return False
        else:
            print("⚠️  文件较大，跳过总行数计算")
            print("✅ 文件可以读取，建议转换为CSV格式以提高处理速度")
            return True
            
    except EOFError:
        print("❌ EOFError: 文件读取中断")
        print("\n可能原因：")
        print("1. 文件损坏或不完整")
        print("2. 文件正在被其他程序使用")
        print("3. 文件没有完全下载/复制")
        print("4. 磁盘空间不足")
        print("\n建议：")
        print("- 关闭可能打开该文件的Excel程序")
        print("- 检查文件是否完整")
        print("- 尝试将文件另存为新文件")
        print("- 转换为CSV格式（推荐）")
        return False
    except Exception as e:
        error_type = type(e).__name__
        print(f"❌ 读取失败: {error_type}: {e}")
        
        if 'corrupt' in str(e).lower() or 'truncated' in str(e).lower():
            print("\n文件可能损坏，建议：")
            print("1. 在Excel中打开文件检查")
            print("2. 尝试'另存为'新文件")
            print("3. 转换为CSV格式")
        
        return False

def main():
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        result = check_excel_file(file_path)
        
        print("\n" + "="*60)
        if result:
            print("✅ 文件检查通过，可以正常使用")
        else:
            print("❌ 文件存在问题，请根据上述建议处理")
        print("="*60)
        
        return 0 if result else 1
    else:
        print("用法: python check_excel_file.py <Excel文件路径>")
        print("\n示例:")
        print("  python check_excel_file.py data.xlsx")
        return 1

if __name__ == "__main__":
    sys.exit(main())

