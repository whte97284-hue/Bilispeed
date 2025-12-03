import streamlit.web.cli as stcli
import os, sys

def resolve_path(path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, path)
    return os.path.join(os.path.abspath("."), path)

if __name__ == "__main__":
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    
    app_path = resolve_path("app.py")
    
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--global.developmentMode=false",
        "--server.headless=true",  
        "--browser.gatherUsageStats=false", 
        "--server.address=localhost", 
        "--server.port=8501", 
    ]
    
    print("🚀 正在启动 BiliConsole...")
    print("如果浏览器没有自动弹出，请手动访问: http://localhost:8501")
    
    # 4. 启动

    sys.exit(stcli.main())
