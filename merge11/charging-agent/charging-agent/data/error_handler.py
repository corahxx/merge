# data/error_handler.py - 错误处理和调试工具

import traceback
import sys
import os
from typing import Optional, Dict, Any
from datetime import datetime
import logging

# 配置日志（安全的配置方式，避免重复配置问题）
def setup_logging():
    """配置日志系统"""
    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(__file__))
    log_file = os.path.join(project_root, 'data_processing.log')
    
    # 获取根logger
    root_logger = logging.getLogger()
    
    # 检查是否已经有文件handler指向data_processing.log
    has_data_processing_file_handler = False
    log_file_normalized = os.path.normpath(log_file)
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                handler_file_normalized = os.path.normpath(handler.baseFilename)
                if handler_file_normalized == log_file_normalized or handler_file_normalized.endswith(os.path.join(os.sep, 'data_processing.log')):
                    has_data_processing_file_handler = True
                    break
            except:
                pass
    
    # 如果没有data_processing.log的handler，则添加
    if not has_data_processing_file_handler:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # 确保根logger的级别
    if root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)
    
    # 获取本模块的logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    return logger

# 初始化日志
logger = setup_logging()


class ErrorHandler:
    """错误处理器"""
    
    @staticmethod
    def handle_exception(e: Exception, context: str = "") -> Dict[str, Any]:
        """
        统一处理异常
        :param e: 异常对象
        :param context: 上下文信息
        :return: 错误信息字典
        """
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'context': context,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc()
        }
        
        # 记录日志（确保日志已初始化）
        try:
            logger.error(f"[{context}] {error_info['error_type']}: {error_info['error_message']}")
            logger.debug(traceback.format_exc())
        except Exception as log_error:
            # 如果日志记录失败，至少输出到控制台
            print(f"日志记录失败: {log_error}", file=sys.stderr)
            print(f"[{context}] {error_info['error_type']}: {error_info['error_message']}", file=sys.stderr)
        
        return error_info
    
    @staticmethod
    def get_common_solutions(error_type: str, error_message: str) -> list:
        """
        根据错误类型和消息返回常见解决方案
        :param error_type: 错误类型
        :param error_message: 错误消息
        :return: 解决方案列表
        """
        solutions = []
        
        # 数据库连接错误
        if 'mysql' in error_message.lower() or 'connection' in error_message.lower():
            solutions.extend([
                "1. 检查数据库服务是否启动",
                "2. 检查 config.py 中的数据库配置是否正确",
                "3. 检查数据库用户权限",
                "4. 检查网络连接和防火墙设置"
            ])
        
        # 文件读取错误
        elif 'file' in error_message.lower() or 'not found' in error_message.lower():
            solutions.extend([
                "1. 检查文件路径是否正确",
                "2. 检查文件是否存在",
                "3. 检查文件是否被其他程序占用",
                "4. 检查文件格式是否支持（.xlsx, .xls, .csv）"
            ])
        
        # 编码错误
        elif 'encoding' in error_message.lower() or 'utf' in error_message.lower():
            solutions.extend([
                "1. 检查EXCEL文件的编码格式",
                "2. 尝试将文件另存为UTF-8编码",
                "3. 检查文件中是否有特殊字符",
                "4. 尝试使用不同的编码格式读取"
            ])
        
        # SQL错误
        elif 'sql' in error_message.lower() or 'syntax' in error_message.lower():
            solutions.extend([
                "1. 检查SQL语句语法是否正确",
                "2. 检查字段名是否与数据库表结构匹配",
                "3. 检查数据类型是否正确",
                "4. 检查是否有特殊字符需要转义"
            ])
        
        # 内存错误
        elif 'memory' in error_message.lower() or 'out of memory' in error_message.lower():
            solutions.extend([
                "1. 尝试减小数据文件大小",
                "2. 增加系统内存",
                "3. 使用分批处理（减小chunk_size参数）",
                "4. 关闭其他占用内存的程序"
            ])
        
        # 数据类型错误
        elif 'type' in error_message.lower() or 'dtype' in error_message.lower():
            solutions.extend([
                "1. 检查数据类型是否正确",
                "2. 检查字段映射是否正确",
                "3. 检查数据格式是否符合预期",
                "4. 尝试使用数据类型转换"
            ])
        
        # 表不存在错误
        elif 'table' in error_message.lower() and 'not exist' in error_message.lower():
            solutions.extend([
                "1. 检查表名是否正确（默认: table2509ev）",
                "2. 使用 'replace' 模式自动创建表",
                "3. 手动创建数据库表",
                "4. 检查数据库连接是否指向正确的数据库"
            ])
        
        # 字段不存在错误
        elif 'column' in error_message.lower() or 'field' in error_message.lower():
            solutions.extend([
                "1. 检查字段映射配置是否正确",
                "2. 检查EXCEL文件中的列名",
                "3. 检查数据库表结构",
                "4. 确认字段名拼写是否正确（注意大小写）"
            ])
        
        # 权限错误
        elif 'permission' in error_message.lower() or 'access denied' in error_message.lower():
            solutions.extend([
                "1. 检查数据库用户权限",
                "2. 确认用户有SELECT、INSERT、UPDATE、DELETE权限",
                "3. 检查文件读写权限",
                "4. 以管理员身份运行程序"
            ])
        
        # 如果没有匹配的解决方案
        if not solutions:
            solutions.extend([
                "1. 查看详细的错误日志（data_processing.log）",
                "2. 检查输入数据格式是否正确",
                "3. 尝试使用更小的数据集测试",
                "4. 查看完整的错误堆栈跟踪信息"
            ])
        
        return solutions
    
    @staticmethod
    def format_error_report(error_info: Dict[str, Any]) -> str:
        """
        格式化错误报告
        :param error_info: 错误信息字典
        :return: 格式化的错误报告字符串
        """
        report = f"""
{'='*60}
❌ 错误报告
{'='*60}
时间: {error_info['timestamp']}
上下文: {error_info.get('context', '未知')}
错误类型: {error_info['error_type']}
错误消息: {error_info['error_message']}

📋 可能的解决方案:
{chr(10).join(ErrorHandler.get_common_solutions(error_info['error_type'], error_info['error_message']))}

📝 详细堆栈跟踪:
{error_info['traceback']}
{'='*60}
"""
        return report

