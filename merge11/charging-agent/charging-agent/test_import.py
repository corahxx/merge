# test_import.py
print("🔍 正在测试关键库导入...")

try:
    import streamlit as st
    print("✅ streamlit 导入成功")

    from langchain_community.llms import Ollama
    print("✅ langchain-community 导入成功")

    from langchain_community.utilities import SQLDatabase
    print("✅ SQLDatabase 支持正常")

    import pymysql
    print("✅ PyMySQL 安装正常")

    print("\n🎉 恭喜！所有依赖均已正确安装！")
    print("现在可以运行：streamlit run app.py")

except Exception as e:
    print(f"❌ 导入失败：{e}")
