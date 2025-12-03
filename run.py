import streamlit.web.cli as stcli
import os, sys

def resolve_path(path):
    """获取资源绝对路径"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, path)
    return os.path.join(os.path.abspath("."), path)

if __name__ == "__main__":
    # 1. 强制屏蔽“请输入邮箱”的提示 (关键！)
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    
    # 2. 获取 app.py 的真实路径
    app_path = resolve_path("app.py")
    
    # 3. 构造启动命令
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--global.developmentMode=false",
        "--server.headless=true",  # 再次确保无头模式
        "--browser.gatherUsageStats=false", # 禁止收集统计信息
        "--server.address=localhost", 
        "--server.port=8501", 
    ]
    
    print("🚀 正在启动 BiliConsole...")
    print("如果浏览器没有自动弹出，请手动访问: http://localhost:8501")
    
    # 4. 启动
    sys.exit(stcli.main())