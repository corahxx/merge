# -*- coding: utf-8 -*-
# count_code_stats.py - 统计项目代码行数和文件数量

import os
from pathlib import Path

def count_lines(file_path):
    """统计文件行数"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return len(f.readlines())
    except:
        return 0

def should_ignore(path):
    """判断是否应该忽略该路径"""
    ignore_patterns = [
        'venv', '__pycache__', '.git', '.idea', '.vscode', 
        'node_modules', 'backup', '.pytest_cache', '.mypy_cache',
        '*.pyc', '*.pyo', '*.pyd', '*.so', '*.dll', '*.exe',
        '*.egg', '*.whl', '__init__.pyc'
    ]
    
    path_str = str(path)
    for pattern in ignore_patterns:
        if pattern in path_str:
            return True
    return False

def get_file_stats(root_dir='.'):
    """统计项目文件信息"""
    root = Path(root_dir)
    
    # 统计所有代码文件
    code_extensions = {'.py', '.md', '.txt', '.bat', '.sh', '.js', '.html', '.css', '.json', '.yaml', '.yml'}
    
    stats = {
        'total_files': 0,
        'total_lines': 0,
        'py_files': 0,
        'py_lines': 0,
        'other_files': 0,
        'other_lines': 0,
        'file_details': []
    }
    
    for file_path in root.rglob('*'):
        if file_path.is_file() and not should_ignore(file_path):
            ext = file_path.suffix.lower()
            
            # 只统计代码相关文件
            if ext in code_extensions or ext == '':
                lines = count_lines(file_path)
                rel_path = file_path.relative_to(root)
                
                stats['total_files'] += 1
                stats['total_lines'] += lines
                stats['file_details'].append({
                    'path': str(rel_path),
                    'lines': lines,
                    'ext': ext
                })
                
                if ext == '.py':
                    stats['py_files'] += 1
                    stats['py_lines'] += lines
                else:
                    stats['other_files'] += 1
                    stats['other_lines'] += lines
    
    return stats

def main():
    print("=" * 60)
    print("📊 项目代码统计")
    print("=" * 60)
    print()
    
    stats = get_file_stats()
    
    print(f"📁 总文件数: {stats['total_files']}")
    print(f"📝 总代码行数: {stats['total_lines']:,}")
    print()
    
    print(f"🐍 Python文件: {stats['py_files']} 个")
    print(f"   Python代码行数: {stats['py_lines']:,} 行")
    print()
    
    print(f"📄 其他文件: {stats['other_files']} 个")
    print(f"   其他代码行数: {stats['other_lines']:,} 行")
    print()
    
    # 显示Python文件详情（按行数排序）
    py_files = [f for f in stats['file_details'] if f['ext'] == '.py']
    py_files.sort(key=lambda x: x['lines'], reverse=True)
    
    print("=" * 60)
    print("📋 Python文件详情（按行数排序，Top 20）:")
    print("=" * 60)
    for i, file_info in enumerate(py_files[:20], 1):
        print(f"{i:2d}. {file_info['path']:50s} {file_info['lines']:5d} 行")
    
    if len(py_files) > 20:
        print(f"\n... 还有 {len(py_files) - 20} 个Python文件")
    
    print()
    print("=" * 60)
    print("📋 其他文件详情（按行数排序，Top 10）:")
    print("=" * 60)
    other_files = [f for f in stats['file_details'] if f['ext'] != '.py']
    other_files.sort(key=lambda x: x['lines'], reverse=True)
    for i, file_info in enumerate(other_files[:10], 1):
        print(f"{i:2d}. {file_info['path']:50s} {file_info['lines']:5d} 行")
    
    if len(other_files) > 10:
        print(f"\n... 还有 {len(other_files) - 10} 个其他文件")

if __name__ == '__main__':
    main()

