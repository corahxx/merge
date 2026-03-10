# handlers/file_upload_handler.py - 文件上传处理

from pathlib import Path
from typing import Optional, Dict
import streamlit as st


class FileUploadHandler:
    """处理文件上传和保存"""
    
    def __init__(self, upload_dir: str = "uploads"):
        """
        初始化文件上传处理器
        :param upload_dir: 上传文件保存目录
        """
        self.upload_dir = Path(upload_dir)
        self.upload_dir.mkdir(exist_ok=True)
    
    def save_uploaded_file(self, uploaded_file) -> Optional[Path]:
        """
        保存上传的文件
        :param uploaded_file: Streamlit 上传的文件对象
        :return: 保存后的文件路径，失败返回 None
        """
        if uploaded_file is None:
            return None
        
        try:
            file_path = self.upload_dir / uploaded_file.name
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            return file_path
        except Exception as e:
            st.error(f"保存文件失败: {str(e)}")
            return None
    
    def save_multiple_files(self, uploaded_files) -> Dict[str, Optional[Path]]:
        """
        保存多个上传的文件
        :param uploaded_files: 文件列表
        :return: 文件名到路径的映射字典
        """
        result = {}
        for uploaded_file in uploaded_files:
            if uploaded_file:
                file_path = self.save_uploaded_file(uploaded_file)
                result[uploaded_file.name] = file_path
        return result
    
    def get_file_info(self, uploaded_file) -> Dict:
        """
        获取文件信息
        :param uploaded_file: Streamlit 上传的文件对象
        :return: 文件信息字典
        """
        if uploaded_file is None:
            return {}
        
        return {
            'name': uploaded_file.name,
            'size': uploaded_file.size,
            'size_mb': uploaded_file.size / 1024 / 1024,
            'type': uploaded_file.type,
            'is_large': uploaded_file.size > 50 * 1024 * 1024  # 50MB
        }
    
    def is_large_file(self, file_size: int, threshold_mb: int = 50) -> bool:
        """
        判断是否为大文件
        :param file_size: 文件大小（字节）
        :param threshold_mb: 阈值（MB）
        :return: 是否为大文件
        """
        return file_size > threshold_mb * 1024 * 1024


