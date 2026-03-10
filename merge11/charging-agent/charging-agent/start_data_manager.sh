#!/bin/bash
# 启动数据管理系统（支持600MB文件上传）

echo "========================================"
echo "  启动数据管理系统"
echo "  支持文件大小: 600MB"
echo "========================================"
echo ""

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "错误: 未找到虚拟环境"
    echo "请先创建虚拟环境: python3 -m venv venv"
    exit 1
fi

# 激活虚拟环境
source venv/bin/activate

# 启动Streamlit
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600

