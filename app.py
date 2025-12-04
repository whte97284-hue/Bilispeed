import streamlit as st
import asyncio
import urllib.parse
import json
import os
import sys
import re
import httpx
import pandas as pd
import subprocess
import shutil
import time
import threading
import csv
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from bilibili_api import user, video, Credential

# ================= 🎨 UI 系统：V24.2 REI (Stable) =================
st.set_page_config(
    page_title="REI System",
    page_icon="💠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 引入 Plotly 默认主题配置
REI_BLUE = "#29B6F6"
REI_DARK = "#0277BD"
EVA_ORANGE = "#FFA726"
EVA_WARN = "#FF9800"

st.markdown(f"""
<meta name="referrer" content="no-referrer">
<style>
    /* 1. 全局字体与背景 */
    .stApp {{
        background-color: #F8F9FA;
        font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
    }}
    
    /* 2. 侧边栏 */
    [data-testid="stSidebar"] {{
        background-color: #FFFFFF;
        border-right: 1px solid #E0E0E0;
    }}
    
    /* 3. 导航菜单魔改 */
    [data-testid="stSidebar"] [data-testid="stRadio"] > label {{ display: none; }}
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] {{ gap: 12px; }}
    
    [data-testid="stSidebar"] [data-testid="stRadio"] label {{
        background-color: #F0F2F5;
        border: 1px solid transparent;
        border-radius: 12px;
        padding: 16px !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        color: #607D8B !important;
        transition: all 0.2s ease-in-out;
        justify-content: center;
        margin-bottom: 5px;
    }}
    
    [data-testid="stSidebar"] [data-testid="stRadio"] label:hover {{
        background-color: #E3F2FD;
        color: {REI_DARK} !important;
    }}
    
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-checked="true"] {{
        background-color: #FFFFFF !important;
        border: 2px solid {REI_BLUE} !important;
        color: {REI_DARK} !important;
        box-shadow: 0 4px 12px rgba(41, 182, 246, 0.15);
    }}

    /* 4. 按钮整形 - 去红化处理 */
    .stButton > button {{
        border-radius: 8px;
        height: 50px !important;
        font-weight: 600;
        font-size: 15px;
        border: none;
        width: 100%;
        transition: 0.2s;
    }}
    
    /* 主按钮 (蓝) */
    button[kind="primary"], button[type="primary"] {{
        background: linear-gradient(90deg, {REI_BLUE} 0%, #039BE5 100%);
        color: white !important;
        box-shadow: 0 4px 6px rgba(3, 155, 229, 0.2);
        border: none !important;
    }}
    button[kind="primary"]:hover, button[type="primary"]:hover {{
        transform: translateY(-1px);
        box-shadow: 0 6px 12px rgba(3, 155, 229, 0.3);
    }}
    
    /* 次要/停止按钮 (灰/橙 - 替代红色) */
    button[kind="secondary"], button[type="secondary"] {{
        background-color: #F5F5F5;
        color: #546E7A !important;
        border: 1px solid #CFD8DC !important;
    }}
    button[kind="secondary"]:hover, button[type="secondary"]:hover {{
        background-color: #ECEFF1;
        color: {EVA_ORANGE} !important;
        border-color: {EVA_ORANGE} !important;
    }}

    /* 5. 输入框美化 */
    .stTextInput>div>div>input, .stTextArea>div>div>textarea {{
        background-color: #FFFFFF;
        border: 1px solid #CFD8DC;
        border-radius: 8px;
        color: #37474F;
        height: 50px;
    }}
    .stTextInput>div>div>input:focus {{
        border-color: {REI_BLUE};
        box-shadow: 0 0 0 3px rgba(41, 182, 246, 0.1);
    }}

    /* 6. 日志区域 */
    .task-log-box {{
        font-family: 'Consolas', monospace; font-size: 12px;
        padding: 15px; border-radius: 10px;
        border: 1px solid #E0E0E0;
        height: 300px; overflow-y: auto;
        background-color: #FAFAFA; 
        color: #546E7A;
    }}
    .status-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }}
    .dot-green {{ background-color: #00C853; }}
    .dot-warn {{ background-color: {EVA_WARN}; }} /* 警告改为橙色 */
    .dot-blue {{ background-color: {REI_BLUE}; }}

    /* 7. 卡片容器 */
    .stContainer {{
        background: white;
        padding: 25px;
        border-radius: 12px;
        border: 1px solid #ECEFF1;
        box-shadow: 0 2px 10px rgba(0,0,0,0.02);
    }}
    
    /* 8. 指标卡 */
    div[data-testid="stMetric"] {{
        background: #F1F8E9;
        border: 1px solid #DCEDC8;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }}
    div[data-testid="stMetric"] label {{ color: #689F38; }}
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{ color: #33691E; }}
    
    /* 9. 视频卡片 */
    .video-card {{
        display: flex; background: white; border-radius: 12px; overflow: hidden;
        border: 1px solid #E3F2FD; margin-bottom: 15px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
    }}
    .video-cover {{ width: 180px; height: 110px; object-fit: cover; }}
    .video-info {{ padding: 12px 20px; display: flex; flex-direction: column; justify-content: center; }}
    .video-title {{ font-weight: bold; font-size: 16px; color: #263238; margin-bottom: 5px; }}
    .video-meta {{ font-size: 13px; color: #78909C; }}

    /* 10. 哨兵警报 (去红 - 改为橙色警告风格) */
    .sentinel-alert {{
        background-color: #FFF3E0; color: #EF6C00; padding: 15px; border-radius: 8px; 
        border: 1px solid #FFE0B2; margin-bottom: 15px; font-weight: bold;
        border-left: 5px solid {EVA_WARN};
    }}
    .sentinel-ok {{
        background-color: #E8F5E9; color: #2E7D32; padding: 15px; border-radius: 8px; 
        border: 1px solid #C8E6C9; margin-bottom: 15px;
    }}
    
    /* 控制台样式 */
    .console-box {{
        background: #263238; color: #ECEFF1; padding: 10px; border-radius: 8px;
        font-family: monospace; font-size: 12px; line-height: 1.4;
        max-height: 200px; overflow-y: auto; margin-top: 10px;
    }}
</style>
""", unsafe_allow_html=True)

# ================= 💾 基础配置 =================
def get_base_path():
    if getattr(sys, 'frozen', False): return os.path.dirname(sys.executable)
    return os.getcwd()

BASE_DIR = os.path.join(get_base_path(), "history")
CONFIG_FILE = os.path.join(get_base_path(), "config.json")
TASK_LOG_FILE = os.path.join(get_base_path(), "task_log.json")

DATA_DIR = os.path.join(BASE_DIR, "data")     
COVERS_DIR = os.path.join(BASE_DIR, "covers") 
VIDEOS_DIR = os.path.join(BASE_DIR, "videos") 
MONITOR_DIR = os.path.join(BASE_DIR, "monitor")
SENTINEL_DIR = os.path.join(MONITOR_DIR, "sentinel")

for d in [DATA_DIR, COVERS_DIR, VIDEOS_DIR, MONITOR_DIR, SENTINEL_DIR]:
    if not os.path.exists(d): os.makedirs(d)

def load_json(file, default):
    if os.path.exists(file):
        try:
            with open(file, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
    return default

def save_json(file, data):
    folder = os.path.dirname(file)
    if folder: os.makedirs(folder, exist_ok=True)
    with open(file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

def log_task(module, target, status, details=""):
    logs = load_json(TASK_LOG_FILE, [])
    new_log = {
        "time": datetime.now().strftime('%m-%d %H:%M'),
        "mod": module, "tgt": str(target)[:12], "sts": status, "msg": details
    }
    logs.insert(0, new_log)
    save_json(TASK_LOG_FILE, logs[:50])

if 'config_loaded' not in st.session_state:
    saved_config = load_json(CONFIG_FILE, {"sessdata": "", "uids": "551898501"})
    st.session_state['sessdata'] = saved_config.get('sessdata', "")
    st.session_state['uids'] = saved_config.get('uids', "551898501")
    st.session_state['monitor_stop_event'] = threading.Event()
    st.session_state['config_loaded'] = True

def save_settings():
    save_json(CONFIG_FILE, {"sessdata": st.session_state.sessdata, "uids": st.session_state.uids})

# ================= 🛠️ 工具 & 辅助函数 =================
def check_tool(name):
    local_path = os.path.join(get_base_path(), name)
    if os.path.exists(local_path): return local_path
    return shutil.which(name)

def get_ffmpeg_path():
    local_ffmpeg = os.path.join(get_base_path(), "ffmpeg.exe")
    if os.path.exists(local_ffmpeg): return local_ffmpeg
    if shutil.which("ffmpeg"): return "ffmpeg"
    return None

def format_duration(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

# Plotly 图表绘制函数
def draw_dual_axis_chart(df, x_col, y1_col, y2_col, title1, title2):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 轴 1 (通常是播放量)
    fig.add_trace(
        go.Scatter(x=df[x_col], y=df[y1_col], name=title1, mode='lines+markers',
                   line=dict(color=REI_BLUE, width=3), marker=dict(size=6)),
        secondary_y=False
    )
    
    # 轴 2 (通常是硬币/评论)
    fig.add_trace(
        go.Scatter(x=df[x_col], y=df[y2_col], name=title2, mode='lines+markers',
                   line=dict(color=EVA_ORANGE, width=3, dash='dot'), marker=dict(size=6)),
        secondary_y=True
    )

    fig.update_layout(
        title=None,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        hovermode="x unified",
        margin=dict(l=0, r=0, t=20, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_yaxes(title_text=title1, secondary_y=False, gridcolor='#E0E0E0')
    fig.update_yaxes(title_text=title2, secondary_y=True, showgrid=False)
    fig.update_xaxes(gridcolor='#E0E0E0')
    return fig

# ================= 🕸️ 核心业务逻辑 =================
async def get_video_basic_info(bvid, sessdata):
    try:
        cred = Credential(sessdata=urllib.parse.unquote(sessdata.strip()))
        v = video.Video(bvid=bvid, credential=cred)
        info = await v.get_info()
        return {
            "title": info['title'],
            "pic": info['pic'], 
            "proxy_pic": f"https://images.weserv.nl/?url={info['pic']}", 
            "owner": info['owner']['name'],
            "view": info['stat']['view'],
            "reply": info['stat']['reply'],
            "pubdate": datetime.fromtimestamp(info['pubdate']).strftime('%Y-%m-%d %H:%M')
        }
    except Exception as e:
        return None

async def get_details(bvid, title, semaphore, cred, progress_callback):
    async with semaphore:
        try:
            v = video.Video(bvid=bvid, credential=cred)
            info, tags_raw = await asyncio.gather(v.get_info(), v.get_tags(), return_exceptions=True)
            if isinstance(info, Exception): return None
            tags_list = [t['tag_name'] for t in tags_raw] if tags_raw and not isinstance(tags_raw, Exception) else []
            if progress_callback: progress_callback()
            view = info['stat']['view']
            coin = info['stat']['coin']
            coin_ratio = round((coin / view * 100), 2) if view > 0 else 0
            pub_dt = datetime.fromtimestamp(info['pubdate'])
            return {
                "bvid": bvid, "title": title, 
                "date": pub_dt.strftime('%Y-%m-%d'), "datetime": pub_dt, "publish_hour": pub_dt.hour,
                "duration": info['duration'], "duration_str": format_duration(info['duration']),
                "cover": f"https://images.weserv.nl/?url={info['pic']}",
                "play": view, "coins": coin, 
                "favs": info['stat']['favorite'], "shares": info['stat']['share'],
                "reply": info['stat']['reply'],
                "coin_ratio": coin_ratio, "tags": tags_list, "desc": info['desc'].replace('\n', ' ').strip()
            }
        except: return None

async def scan_user_videos(uid, sessdata):
    cred = Credential(sessdata=urllib.parse.unquote(sessdata.strip()))
    u = user.User(int(uid), credential=cred)
    info = await u.get_user_info()
    videos = []
    page = 1
    while True:
        res = await u.get_videos(ps=30, pn=page)
        if not res or 'list' not in res or 'vlist' not in res['list']: break
        vlist = res['list']['vlist']
        if not vlist: break
        for v in vlist: videos.append(v)
        page += 1
        await asyncio.sleep(0.1)
    return info['name'], videos

def run_bbdown_advanced(bbdown_path, bvid, work_dir, sessdata, status_placeholder, log_placeholder, options):
    script_dir = os.getcwd()
    cmd = [bbdown_path, bvid, "--work-dir", work_dir]
    
    # 1. 身份凭证 (V37.0: 全模式强制使用本地凭证)
    # 只要文件存在，就优先用文件，不再手动传 -c，这样兼容性最好
    local_data = os.path.join(script_dir, "BBDown.data")
    if os.path.exists(local_data):
        auth_status = "✅ 本地凭证 (BBDown.data)"
    else:
        # 只有文件不存在时，才降级使用网页字符串
        clean_sess = sessdata.replace("SESSDATA=", "").strip()
        cmd.extend(["-c", f"SESSDATA={clean_sess}"])
        auth_status = "⚠️ 网页 SESSDATA (文件未找到)"

    # 2. 代理设置
    env = os.environ.copy()
    proxy_msg = "🏠 直连"
    raw_proxy = options.get('proxy', '').strip()
    if raw_proxy:
        if not raw_proxy.startswith("http"): fixed_proxy = f"http://{raw_proxy}"
        elif raw_proxy.startswith("http:") and not raw_proxy.startswith("http://"): fixed_proxy = raw_proxy.replace("http:", "http://")
        else: fixed_proxy = raw_proxy
        env["http_proxy"] = fixed_proxy
        env["https_proxy"] = fixed_proxy
        env["all_proxy"] = fixed_proxy
        proxy_msg = f"🌍 {fixed_proxy}"

    # 3. 接口策略 (V37.0: 纯净 Web 模式)
    # 只有用户明确勾选 APP/TV 时才加参数，否则保持纯净，模拟浏览器
    if_msg = "🌐 Web (纯净模式)"
    if options.get('use_app'): 
        cmd.append("-app")
        if_msg = "📱 APP"
    elif options.get('use_tv'): 
        cmd.append("-tv")
        if_msg = "📺 TV"

    # 4. 稳定性参数
    # 强制不使用 Aria2，除非用户在代码里手动改回来
    # 你的网络环境 SSL 报错，单线程是最稳的
    if options.get('use_aria2'):
        if shutil.which("aria2c") or os.path.exists(os.path.join(script_dir, "aria2c.exe")):
            cmd.append("--use-aria2c") 
    
    # 自动画质不传参，让 BBDown 自己选
    res = options.get('resolution')
    if res and res != "自动 (Auto)": cmd.extend(["--dfn-priority", res])
        
    enc = options.get('encoding')
    if enc and enc != "自动 (Auto)": cmd.extend(["--encoding-priority", enc])
        
    if options.get('audio_only'): cmd.append("--audio-only") 
    
    if options.get('p_range'): cmd.extend(["-p", options['p_range']])
    elif options.get('download_all'): cmd.append("-p ALL")

    # 5. 诊断信息
    status_placeholder.info(f"""
    🛠️ V37.0 救砖模式:
    1. 凭证: {auth_status}
    2. 网络: {proxy_msg}
    3. 接口: {if_msg}
    4. 策略: 优先使用 Web 接口绕过区域检测，单线程保稳定。
    """)
    
    time.sleep(0.5)

    # 6. 执行
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding='gbk', errors='replace',
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0, bufsize=1,
        env=env, cwd=script_dir
    )
    
    full_log = ""
    last_update_time = 0 
    log_placeholder.code("🚀 正在连接 Bilibili...", language="text")

    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None: break
        if line:
            full_log += line
            if len(full_log) > 5000: full_log = full_log[-5000:]
            
            current_time = time.time()
            if current_time - last_update_time > 0.3:
                log_placeholder.code(full_log, language="text")
                last_update_time = current_time

    log_placeholder.code(full_log, language="text")
    return process.returncode == 0
    

# ================= 👁️ 监控核心 (Sentinel) =================

def monitor_worker(bvid, interval_min, duration_hours, sentinel_enabled, sentinel_interval_min, stop_event, sessdata):
    """后台监控线程：哨兵逻辑"""
    cred = Credential(sessdata=urllib.parse.unquote(sessdata.strip()))
    v = video.Video(bvid=bvid, credential=cred)
    
    csv_file = os.path.join(MONITOR_DIR, f"{bvid}_monitor.csv")
    sentinel_file = os.path.join(SENTINEL_DIR, f"{bvid}_sentinel.csv")
    
    # 初始化 CSV
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["timestamp", "time_str", "view", "like", "coin", "fav", "reply", "share"])
            
    if not os.path.exists(sentinel_file):
        with open(sentinel_file, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["timestamp", "time_str", "old_reply", "new_reply", "diff", "msg"])

    end_time = datetime.now() + timedelta(hours=duration_hours)
    interval_sec = interval_min * 60
    
    # 哨兵状态
    last_reply_check_time = datetime.now()
    last_reply_count = None
    
    log_task("实时监控", bvid, "🟢 启动", f"哨兵: {'ON' if sentinel_enabled else 'OFF'}")
    
    while not stop_event.is_set():
        if datetime.now() > end_time:
            log_task("实时监控", bvid, "⏹️ 结束", "达到设定时长")
            break
            
        try:
            # 获取数据
            info = asyncio.run(v.get_info())
            stat = info['stat']
            now = datetime.now()
            
            # 记录常规数据
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([
                    now.timestamp(), now.strftime('%Y-%m-%d %H:%M:%S'), 
                    stat['view'], stat['like'], stat['coin'], stat['favorite'], stat['reply'], stat['share']
                ])
            
            # 哨兵检测逻辑
            if sentinel_enabled:
                if last_reply_count is None: last_reply_count = stat['reply']
                elif (now - last_reply_check_time).total_seconds() / 60 >= sentinel_interval_min:
                    diff = stat['reply'] - last_reply_count
                    if diff < 0:
                        with open(sentinel_file, 'a', newline='', encoding='utf-8') as f:
                            csv.writer(f).writerow([now.timestamp(), now.strftime('%H:%M'), last_reply_count, stat['reply'], diff, "疑似删评"])
                        log_task("哨兵警告", bvid, "⚠️ 异常", f"评论减少 {diff}")
                    last_reply_count = stat['reply']
                    last_reply_check_time = now

        except Exception as e:
            log_task("实时监控", bvid, "⚠️ 异常", str(e)[:20])
            
        # 智能睡眠 (响应 Stop 信号)
        for _ in range(int(interval_sec)):
            if stop_event.is_set(): break
            time.sleep(1)

# ================= 🖥️ 界面布局 =================

# ================= 🖥️ 侧边栏 (V30.0: 新增扫码登录功能) =================

with st.sidebar:
    st.markdown("### 💠 REI SYSTEM")
    
    mode = st.radio(
        "导航", 
        ["数据洞察", "视频下载", "封面提取", "实时监控"], 
        label_visibility="collapsed"
    )
    
    st.write("") 
    
    with st.expander("🔑 账号凭证 (Token)", expanded=True):
        # SESSDATA 输入框
        st.text_area("SESSDATA", key="sessdata", height=80, on_change=save_settings, help="手动填入，或点击下方按钮扫码自动获取")
        
# === 🟢 V30.6 完整性修复版：BBDown 扫码登录 ===
        if st.button("📱 扫码登录 (自动获取)", use_container_width=True):
            bbdown_exe = check_tool("BBDown.exe")
            if not bbdown_exe:
                st.error("未找到 BBDown.exe")
            else:
                status_text = st.empty()
                qr_placeholder = st.empty()
                log_area = st.empty()
                full_logs = ""
                
                try:
                    # 1. 清理旧文件
                    if os.path.exists("BBDown.data"): os.remove("BBDown.data")
                    if os.path.exists("qrcode.png"): os.remove("qrcode.png")
                    
                    status_text.info("⏳ 正在启动登录进程...")
                    
                    # 启动进程
                    proc = subprocess.Popen(
                        [bbdown_exe, "login"], 
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                        text=True, encoding='gbk', errors='ignore',
                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                        bufsize=1
                    )
                    
                    qr_shown = False
                    
                    while True:
                        line = proc.stdout.readline()
                        if not line and proc.poll() is not None: break
                        
                        if line:
                            clean_line = line.strip()
                            if "██" not in clean_line:
                                full_logs += clean_line + "\n"
                                log_area.code(full_logs[-300:], language="text")

                            # 显示本地二维码
                            if not qr_shown and (os.path.exists("qrcode.png") or "qrcode.png" in clean_line):
                                time.sleep(0.5)
                                if os.path.exists("qrcode.png"):
                                    status_text.success("📸 请使用 B站 App 扫码")
                                    qr_placeholder.image("qrcode.png", width=200)
                                    qr_shown = True
                            
                            # === 🛡️ 核心修复：防止 0kb 文件 ===
                            if "Login successful" in line or "登录成功" in line:
                                status_text.success("✅ 登录成功！正在写入凭证 (请勿操作)...")
                                
                                # 🛑 关键：强制等待 3 秒，确保数据写入硬盘
                                time.sleep(3)
                                proc.terminate()
                                
                                # 🛑 校验：检查文件是否存在且大于 0 字节
                                if os.path.exists("BBDown.data"):
                                    file_size = os.path.getsize("BBDown.data")
                                    if file_size > 0:
                                        status_text.success(f"✅ 凭证保存成功 ({file_size} bytes)！正在重载...")
                                        
                                        # 清理临时图
                                        if os.path.exists("qrcode.png"): os.remove("qrcode.png")
                                        
                                        # 提取 SESSDATA 更新界面
                                        with open("BBDown.data", "r", encoding='utf-8') as f:
                                            cookie_str = f.read()
                                            if "SESSDATA=" in cookie_str:
                                                start = cookie_str.find("SESSDATA=") + 9
                                                end = cookie_str.find(";", start)
                                                if end == -1: end = len(cookie_str)
                                                new_sess = cookie_str[start:end]
                                                st.session_state.sessdata = new_sess
                                                save_settings()
                                        
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        status_text.error("❌ 严重错误：凭证文件为空 (0kb)！写入失败。")
                                        st.error("请尝试方案 B：在黑框终端手动运行 'BBDown login'")
                                else:
                                    status_text.error("❌ 未找到 BBDown.data 文件")
                                break
                except Exception as e:
                    st.error(f"出错: {e}")

    with st.expander("⚙️ 引擎参数"):
        concurrency = st.slider("并发线程", 1, 10, 5)

    st.divider()
    st.markdown("**📋 系统日志**")
    logs = load_json(TASK_LOG_FILE, [])
    log_html = ""
    for l in logs:
        sts = l.get('sts', 'UNK')
        dot_class = "dot-green" if any(x in sts for x in ["成功","完成","启动"]) else "dot-warn" if any(x in sts for x in ["失败","异常","停止"]) else "dot-blue"
        log_html += f"<div><span class='status-dot {dot_class}'></span><span style='color:#78909C'>[{l['time'][-5:]}]</span> {l['tgt']}: {sts}</div>"
    st.markdown(f'<div class="task-log-box">{log_html}</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown(f"""<div style="text-align:center;color:#B0BEC5;font-size:12px;">BiliCommander V30.0<br>Rei Edition</div>""", unsafe_allow_html=True)

# === 模块 1: 数据洞察 (Plotly版) ===
if mode == "数据洞察":
    st.title("📊 数据洞察")
    with st.container():
        c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
        with c1:
            st.text_area("目标 UID 矩阵", key="uids", height=100, on_change=save_settings, placeholder="每行一个UID", label_visibility="visible")
        with c2:
            if st.button("🚀 执行分析", type="primary", use_container_width=True):
                if not st.session_state.sessdata: st.warning("缺 SESSDATA")
                else:
                    async def quick_mine():
                        uids = [x.strip() for x in st.session_state.uids.replace('\n',',').split(',') if x.strip()]
                        res_list = []
                        cred = Credential(sessdata=urllib.parse.unquote(st.session_state.sessdata))
                        status_text = st.empty()
                        for i, uid in enumerate(uids):
                            try:
                                u = user.User(int(uid), credential=cred)
                                info = await u.get_user_info()
                                status_text.info(f"扫描: {info['name']}...")
                                videos = []
                                page = 1
                                while True:
                                    r = await u.get_videos(ps=30, pn=page)
                                    if not r or not r['list']['vlist']: break
                                    videos.extend(r['list']['vlist'])
                                    page += 1
                                    await asyncio.sleep(0.1)
                                sema = asyncio.Semaphore(concurrency)
                                tasks = [get_details(v['bvid'], v['title'], sema, cred, None) for v in videos]
                                details = await asyncio.gather(*tasks)
                                valid = [d for d in details if d]
                                
                                summ = {"UP主": info['name'], "UID": uid, "视频数": len(valid), "总播放量": sum(d['play'] for d in valid), "总硬币": sum(d['coins'] for d in valid), "总收藏": sum(d['favs'] for d in valid)}
                                full = {"summary": summ, "videos": valid}
                                res_list.append(full)
                                
                                path = os.path.join(DATA_DIR, f"{uid}_{info['name']}")
                                if not os.path.exists(path): os.makedirs(path)
                                save_json(f"{path}/{datetime.now().strftime('%Y%m%d_%H%M')}.json", full)
                                log_task("数据分析", info['name'], "成功", f"{len(valid)}条")
                            except Exception as e: log_task("数据分析", uid, "异常", str(e))
                        status_text.success("任务完成")
                        return res_list
                    with st.spinner("数据链路连接中..."): st.session_state['mining_results'] = asyncio.run(quick_mine())

    if os.path.exists(DATA_DIR):
        with st.expander("📂 历史档案归档"):
            uid_folders = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
            if uid_folders:
                col_h1, col_h2, col_h3 = st.columns([2, 2, 1], vertical_alignment="bottom")
                sel_uid = col_h1.selectbox("UP主", ["-- 请选择 --"] + uid_folders)
                if sel_uid != "-- 请选择 --":
                    target_path = os.path.join(DATA_DIR, sel_uid)
                    files = sorted([f for f in os.listdir(target_path) if f.endswith('.json')], reverse=True)
                    sel_file = col_h2.selectbox("时间点", files)
                    if col_h3.button("加载", use_container_width=True):
                        with open(os.path.join(target_path, sel_file), 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            st.session_state['mining_results'] = [data] if isinstance(data, dict) else data
                            st.rerun()

    if 'mining_results' in st.session_state:
        st.write("---")
        for res in st.session_state['mining_results']:
            summ = res['summary']
            vids = res['videos']
            df = pd.DataFrame(vids)
            st.markdown(f"### 👤 {summ['UP主']}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("总播放量", f"{summ['总播放量']:,}")
            m2.metric("总硬币", f"{summ['总硬币']:,}")
            m3.metric("总收藏", f"{summ['总收藏']:,}")
            m4.metric("视频数", summ['视频数'])
            
            tab1, tab2 = st.tabs(["📄 列表", "📈 趋势 (Interactive)"])
            with tab1: 
                st.dataframe(df, column_config={
                    "cover": st.column_config.ImageColumn("封面"), 
                    "play": st.column_config.NumberColumn("播放", format="%d"),
                    "coin_ratio": st.column_config.NumberColumn("币/播比", format="%.2f%%")
                }, use_container_width=True, height=400)
            with tab2: 
                # [升级] 使用 Plotly 绘制双轴图
                if not df.empty:
                    fig = draw_dual_axis_chart(df.sort_values("datetime"), "date", "play", "coins", "播放量", "硬币数")
                    st.plotly_chart(fig, use_container_width=True)

# === 模块 2: 视频下载 (V31.0: 修复港澳台崩溃/支持本地凭证) ===
elif mode == "视频下载":
    st.title("🎥 视频下载")
    bbdown_path = check_tool("BBDown.exe")
    ffmpeg_path = check_tool("ffmpeg.exe")
    
    if not bbdown_path: 
        st.error("🚫 严重错误: 未检测到 BBDown.exe")
    else: 
        if ffmpeg_path: st.success("✅ 引擎就绪 (BBDown + FFmpeg)")
        else: st.warning("⚠️ 警告: 未检测到 FFmpeg")

    tab_up, tab_bangumi = st.tabs(["批量下载 (扫描UP主)", "直链/番剧下载 (URL)"])

    # --- 🟢 Tab 1: UP主批量下载 ---
    with tab_up:
        st.caption("输入 UP 主的 UID，批量选择视频下载")
        with st.container():
            c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
            with c1: 
                dl_uid = st.text_input("输入 UID 获取列表", value="551898501", key="input_uid_scan")
            with c2:
                if st.button("🔍 扫描列表", type="primary", use_container_width=True, key="btn_scan_uid"):
                    if not st.session_state.sessdata: st.warning("请先在左侧配置 SESSDATA")
                    else:
                        with st.spinner("正在扫描接口..."):
                            name, vlist = asyncio.run(scan_user_videos(dl_uid, st.session_state.sessdata))
                            st.session_state['dl_list'] = {"name": name, "uid": dl_uid, "v": vlist}
        
        if 'dl_list' in st.session_state:
            data = st.session_state['dl_list']
            st.info(f"👤 {data['name']} (UID: {data['uid']}) - 共扫描到 {len(data['v'])} 个视频")
            
            df = pd.DataFrame(data['v'])
            df['selected'] = False
            edited = st.data_editor(
                df, 
                column_config={
                    "selected": st.column_config.CheckboxColumn("选", width="small"), 
                    "pic": st.column_config.ImageColumn("封面"), 
                    "title": st.column_config.TextColumn("标题", disabled=True)
                }, 
                column_order=["selected", "pic", "title", "bvid"], 
                use_container_width=True, 
                height=400,
                key="editor_video_list"
            )
            
            with st.expander("⚙️ 批量下载参数", expanded=True):
                o1, o2, o3, o4 = st.columns(4)
                opt_res = o1.selectbox("画质", ["8K", "4K", "1080P60", "1080P+", "1080P", "720P"], index=1, key="sel_res_up")
                opt_code = o2.selectbox("编码", ["hevc", "av1", "avc"], index=0, key="sel_code_up")
                use_tv = o3.checkbox("TV端接口", value=True, help="推荐开启！通常无水印且画质更高", key="chk_tv_up")
                check_danmaku = o4.checkbox("下载弹幕", value=True, key="chk_dm_up")

            to_dl = edited[edited['selected']==True]
            if st.button(f"🚀 批量下载选中的 {len(to_dl)} 个视频", type="primary", disabled=len(to_dl)==0, use_container_width=True, key="btn_start_batch"):
                dl_folder = os.path.join(VIDEOS_DIR, f"{data['name']}_{data['uid']}")
                if not os.path.exists(dl_folder): os.makedirs(dl_folder)
                
                console = st.empty()
                prog = st.progress(0)
                status = st.empty()
                
                for idx, row in enumerate(to_dl.itertuples()):
                    status.info(f"下载中 ({idx+1}/{len(to_dl)}): {row.title}")
                    opts = {'resolution': opt_res, 'encoding': opt_code, 'danmaku': check_danmaku, 'use_tv': use_tv}
                    sess_clean = urllib.parse.unquote(st.session_state.sessdata.strip())
                    run_bbdown_advanced(bbdown_path, row.bvid, dl_folder, sess_clean, console, opts)
                    prog.progress((idx+1)/len(to_dl))
                
                status.success(f"全部完成！保存至: {dl_folder}")
                try: 
                    if os.name == 'nt': os.startfile(dl_folder)
                except: pass

    # --- 🔵 Tab 2: 番剧/直链/下载 (V31.0 核心升级版) ---
    with tab_bangumi:
        st.caption("支持解析：番剧 Season (ss)、番剧 Episode (ep)、多P视频 (BV)")
        
        # 1. 输入与解析区
        with st.container():
            col_in, col_btn = st.columns([4, 1], vertical_alignment="bottom")
            with col_in:
                url_input = st.text_input(
                    "资源链接 / ID", 
                    placeholder="例如: https://www.bilibili.com/bangumi/play/ss28420 或 BV1xx...",
                    key="input_url_parse"
                )
            with col_btn:
                # 解析按钮
                if st.button("🔍 解析目录", type="primary", use_container_width=True, key="btn_parse_url"):
                    if not url_input:
                        st.warning("请先输入链接")
                    elif not st.session_state.sessdata:
                        st.error("请先配置 SESSDATA")
                    else:
                        st.session_state['parsed_episodes'] = None 
                        with st.spinner("正在获取分集列表..."):
                            async def parse_content(url, sess):
                                try:
                                    cred = Credential(sessdata=urllib.parse.unquote(sess.strip()))
                                    import re
                                    target_id = ""
                                    mode = "video"
                                    if "ss" in url: 
                                        target_id = re.search(r"ss(\d+)", url).group(1); mode = "season"
                                    elif "ep" in url:
                                        target_id = re.search(r"ep(\d+)", url).group(1); mode = "ep"
                                    elif "BV" in url:
                                        target_id = re.search(r"(BV\w+)", url).group(1); mode = "video"
                                    
                                    data_list = []
                                    if mode in ["season", "ep"]:
                                        return {"type": "bangumi", "title": "番剧/电影资源", "list": []}
                                    else:
                                        v = video.Video(bvid=target_id, credential=cred)
                                        info = await v.get_info()
                                        pages = info.get('pages', [])
                                        for p in pages:
                                            data_list.append({"index": p['page'], "title": p['part'], "duration": format_duration(p['duration']), "cid": p['cid']})
                                        return {"type": "video", "title": info['title'], "list": data_list, "bvid": target_id}
                                except Exception as e: return {"error": str(e)}

                            res = asyncio.run(parse_content(url_input, st.session_state.sessdata))
                            if "error" in res: st.error(f"解析失败: {res['error']}")
                            elif res['type'] == 'bangumi':
                                st.info("🎬 已识别番剧链接。")
                                st.session_state['parsed_bangumi'] = True
                                st.session_state['parsed_video'] = None
                            else:
                                st.success(f"解析成功: {res['title']}")
                                st.session_state['parsed_video'] = res
                                st.session_state['parsed_bangumi'] = False

        # 2. 选集逻辑
        selected_indices = []
        if st.session_state.get('parsed_video'):
            pv = st.session_state['parsed_video']
            st.write(f"📺 **{pv['title']}**")
            df_p = pd.DataFrame(pv['list'])
            df_p.insert(0, "Select", False)
            edited_p = st.data_editor(df_p, column_config={"Select": st.column_config.CheckboxColumn("下", width="small"), "index": st.column_config.NumberColumn("P", width="small"), "title": st.column_config.TextColumn("标题")}, hide_index=True, use_container_width=True, height=250, key="editor_mp")
            selected_indices = edited_p[edited_p["Select"] == True]["index"].tolist()
            st.caption(f"已选 {len(selected_indices)} 个")

        elif st.session_state.get('parsed_bangumi'):
            st.write("🎬 **番剧模式**")
            col_bg1, col_bg2 = st.columns(2)
            with col_bg1: dl_mode = st.radio("模式", ["下载全集 (ALL)", "指定集数"], horizontal=True)
            with col_bg2: ep_range = st.text_input("集数 (如 1,2,5-10)", disabled=(dl_mode=="下载全集 (ALL)"))
            selected_indices = "ALL" if dl_mode == "下载全集 (ALL)" else ep_range

        # 3. 下载参数区
        if st.session_state.get('parsed_video') or st.session_state.get('parsed_bangumi'):
            st.divider()
            
            with st.expander("🌍 港澳台解锁 / 网络加速", expanded=True):
                c_net1, c_net2 = st.columns([3, 1])
                with c_net1:
                    proxy_input = st.text_input("HTTP代理地址", placeholder="例如 http://127.0.0.1:7890", key="proxy_input_fix")
                with c_net2:
                    st.write("")
                    st.write("")
                    # 默认关闭 Aria2，解决 net_http_ssl 报错
                    use_aria2 = st.checkbox("Aria2 加速", value=False, help="网络不稳定请关闭此项", key="aria2_fix")

            with st.expander("⚙️ 画质与接口策略 (V37.0)", expanded=True):
                b1, b2, b3, b4 = st.columns(4)
                # 默认自动
                res_bg = b1.selectbox("画质", ["自动 (Auto)", "1080P", "1080P+", "4K"], index=0, key="rf_fix")
                code_bg = b2.selectbox("编码", ["自动 (Auto)", "avc", "hevc", "av1"], index=0, key="cf_fix")
                
                # === 🛠️ 关键：默认选中 Web 接口 ===
                # 你的日志证明只有 Web 接口能获取到视频流
                api_mode = b3.radio("接口模式", ["Web接口 (推荐)", "APP接口", "TV接口"], index=0, key="api_mode_sel")
                use_web = True if "Web" in api_mode else False
                use_app = True if "APP" in api_mode else False
                use_tv = True if "TV" in api_mode else False
                
                # 永远使用本地凭证
                st.caption("✅ 默认使用本地扫码凭证 (BBDown.data)")

            if st.button("🚀 开始下载", type="primary", use_container_width=True, key="btn_dl_final_fix"):
                # 还是传一下，作为备用
                raw = st.session_state.sessdata
                clean_sess = raw.strip().replace('\n', '').replace('\r', '').replace('"', '').replace("'", "")
                if clean_sess.startswith("SESSDATA="): clean_sess = clean_sess.replace("SESSDATA=", "")
                
                bg_folder = os.path.join(VIDEOS_DIR, "Downloads_Direct")
                if not os.path.exists(bg_folder): os.makedirs(bg_folder)
                
                status_box = st.empty()
                log_box = st.empty()
                
                p_arg = ""
                if selected_indices == "ALL": p_arg = "ALL"
                elif isinstance(selected_indices, list): p_arg = ",".join(map(str, selected_indices))
                elif isinstance(selected_indices, str): p_arg = selected_indices
                
                final_opts = {
                    'resolution': res_bg, 'encoding': code_bg, 
                    'use_app': use_app, 
                    'use_tv': use_tv,
                    'use_web_api': use_web,
                    'p_range': p_arg,
                    'proxy': proxy_input.strip(),
                    'use_aria2': use_aria2,
                    'use_local_auth': True # 强制开启
                }

                success = run_bbdown_advanced(bbdown_path, url_input.strip(), bg_folder, clean_sess, status_box, log_box, final_opts)
                
                if success:
                    st.success("✅ 下载成功！")
                    try: 
                        if os.name == 'nt': os.startfile(bg_folder)
                    except: pass
                else:
                    st.error("❌ 下载失败")
                    
# === 模块 3: 封面提取 ===
elif mode == "封面提取":
    st.title("🖼️ 封面提取")
    with st.container():
        c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
        with c1: c_uid = st.text_input("目标 UID", value="551898501", key="cover_uid_input")
        with c2:
            if st.button("📡 扫描封面", type="primary", use_container_width=True):
                 with st.spinner("扫描中..."):
                     name, videos = asyncio.run(scan_user_videos(c_uid, st.session_state.sessdata))
                     for v in videos: v['proxy_pic'] = f"https://images.weserv.nl/?url={v['pic']}"
                     st.session_state['cv_data'] = {"name": name, "uid": c_uid, "v": videos}
    
    if 'cv_data' in st.session_state:
        cd = st.session_state['cv_data']
        cc1, cc2, cc3 = st.columns([1,1,4])
        if cc1.button("全选"): 
            for v in cd['v']: v['selected'] = True 
            st.rerun()
        if cc2.button("清空", type="secondary"): 
            for v in cd['v']: v['selected'] = False
            st.rerun()
        edit_cv = st.data_editor(pd.DataFrame(cd['v']), column_config={"selected": st.column_config.CheckboxColumn("✅"), "proxy_pic": st.column_config.ImageColumn("预览")}, column_order=["selected", "proxy_pic", "title"], use_container_width=True, height=500)
        dl_cv = edit_cv[edit_cv['selected']==True]
        
        if cc3.button(f"📥 下载封面 ({len(dl_cv)})", type="primary", disabled=len(dl_cv)==0, use_container_width=True):
            f_path = os.path.join(COVERS_DIR, f"{cd['name']}_{cd['uid']}")
            if not os.path.exists(f_path): os.makedirs(f_path)
            async def download_imgs_v14(items, folder):
                async with httpx.AsyncClient() as client:
                    async def dl_one(v):
                        try:
                            n = re.sub(r'[\\/*?:"<>|]', "", v['title'])[:80]
                            r = await client.get(v['pic'])
                            with open(f"{folder}/{n}_{v['bvid']}.jpg", "wb") as f: f.write(r.content)
                            return True
                        except: return False
                    await asyncio.gather(*[dl_one(i) for i in items])
            asyncio.run(download_imgs_v14(dl_cv.to_dict('records'), f_path))
            st.success(f"已保存至: {f_path}")
            if os.name == 'nt': os.startfile(f_path)

# === 模块 4: 实时监控 (V24.3 Final Stable) ===
elif mode == "实时监控":
    st.title("🔴 实时监控")
    
    # --- 1. 历史档案归档区 ---
    if os.path.exists(MONITOR_DIR):
        with st.expander("📂 监控档案室 (历史记录)"):
            csvs = [f for f in os.listdir(MONITOR_DIR) if f.endswith('_monitor.csv')]
            if csvs:
                sel_csv = st.selectbox("选择历史记录", ["-- 请选择 --"] + csvs)
                if sel_csv != "-- 请选择 --":
                    try:
                        df_hist = pd.read_csv(os.path.join(MONITOR_DIR, sel_csv))
                        # 修复：默认显示 view 和 reply，确保列存在
                        y2_col = "reply" if "reply" in df_hist.columns else df_hist.columns[-1]
                        fig = draw_dual_axis_chart(df_hist, "time_str", "view", y2_col, "播放趋势", "互动趋势")
                        st.plotly_chart(fig, use_container_width=True)
                        
                        s_file = os.path.join(SENTINEL_DIR, sel_csv.replace("_monitor", "_sentinel"))
                        if os.path.exists(s_file):
                            err_df = pd.read_csv(s_file)
                            if not err_df.empty:
                                st.markdown(f'<div class="sentinel-alert">⚠️ 警报：检测到 {len(err_df)} 次异常删评行为！</div>', unsafe_allow_html=True)
                            else:
                                st.markdown('<div class="sentinel-ok">🛡️ 哨兵检测正常：暂无删评</div>', unsafe_allow_html=True)
                    except: st.error("文件损坏或格式不兼容")

    # --- 2. 核心控制台 (Inputs) ---
    st.write("### ⚙️ 监控配置")
    with st.container():
        c1, c2, c3, c4 = st.columns([3, 1, 1, 1], vertical_alignment="bottom")
        
        # 状态判断
        active = st.session_state.get('monitor_thread_active', False)
        
        with c1: mon_bvid = st.text_input("BVID", placeholder="BVxxxx...", disabled=active)
        with c2: mon_int = st.number_input("频率(分)", 1, value=5, disabled=active)
        with c3: mon_dur = st.number_input("时长(时)", 1, value=24, disabled=active)
        with c4:
            # 停止按钮 (仅运行时显示)
            if active:
                if st.button("⏹️ 停止", type="secondary", use_container_width=True, key="stop_btn_main"):
                    st.session_state['monitor_stop_event'].set()
                    st.session_state['monitor_thread_active'] = False
                    log_task("监控", st.session_state.get('monitor_target'), "停止")
                    time.sleep(1)
                    st.rerun()
            else:
                st.write("") # 占位

    # --- 3. 哨兵设置 (仅未运行时显示) ---
    if not active:
        with st.expander("🛡️ 哨兵防御设置 (Sentinel)", expanded=True):
            col_s1, col_s2 = st.columns([1, 4])
            sentinel_on = col_s1.toggle("启用删评检测", value=True)
            sentinel_freq = col_s2.slider("检测周期 (分钟)", 5, 60, 30)

    # --- 4. 启动逻辑区 ---
    # 确定目标
    target = st.session_state.get('monitor_target', '') if active else (mon_bvid.split('?')[0].split('/')[-1] if mon_bvid else "")
    exists = os.path.exists(os.path.join(MONITOR_DIR, f"{target}_monitor.csv")) if target else False

    if not active and target:
        def safe_launch(clean_history=False):
            if not st.session_state.sessdata: 
                st.error("🚫 请先在左侧填写 SESSDATA")
                return

            if clean_history:
                try: 
                    p1 = os.path.join(MONITOR_DIR, f"{target}_monitor.csv")
                    p2 = os.path.join(SENTINEL_DIR, f"{target}_sentinel.csv")
                    if os.path.exists(p1): os.remove(p1)
                    if os.path.exists(p2): os.remove(p2)
                except: pass

            with st.spinner("🔄 初始化监控链路..."):
                try:
                    if 'monitor_stop_event' not in st.session_state:
                        st.session_state['monitor_stop_event'] = threading.Event()
                    st.session_state['monitor_stop_event'].clear()
                    
                    info = asyncio.run(get_video_basic_info(target, st.session_state.sessdata))
                    
                    if info:
                        st.session_state.update({
                            'monitor_info': info, 'monitor_target': target, 
                            'monitor_start': datetime.now(), 'monitor_hours': mon_dur, 
                            'monitor_thread_active': True
                        })
                        
                        t = threading.Thread(
                            target=monitor_worker, 
                            args=(target, mon_int, mon_dur, sentinel_on, sentinel_freq, st.session_state['monitor_stop_event'], st.session_state.sessdata), 
                            daemon=True
                        )
                        t.start()
                        
                        # 🛠️ 修复点：增加等待时间，防止白屏
                        time.sleep(2) 
                        st.success("✅ 启动成功！")
                        st.rerun()
                    else:
                        st.error("❌ 无法获取信息，请检查 SESSDATA 或网络")
                except Exception as e: st.error(f"启动异常: {e}")

        if exists:
            st.warning(f"检测到 {target} 的历史存档！")
            cc1, cc2 = st.columns(2)
            if cc1.button("🔗 继续监控", type="primary", use_container_width=True, key="btn_resume"): 
                safe_launch(clean_history=False)
            if cc2.button("🆕 覆盖重录", type="secondary", use_container_width=True, key="btn_overwrite"): 
                safe_launch(clean_history=True)
        else:
            if st.button("▶️ 启动新监控", type="primary", use_container_width=True, key="btn_start"):
                safe_launch(clean_history=True)

    # --- 5. 监控面板 (数据显示) ---
    st.write("---")
    
    # A. 视频卡片 (优先显示内存中的信息)
    info_mem = st.session_state.get('monitor_info')
    if info_mem:
        st.markdown(f"""<div class="video-card"><img src="{info_mem['proxy_pic']}" class="video-cover"><div class="video-info"><div class="video-title">{info_mem['title']}</div><div class="video-meta"><span>UP: {info_mem['owner']}</span> <span>发布: {info_mem['pubdate']}</span></div></div></div>""", unsafe_allow_html=True)

    # B. 数据与图表
    c_path = os.path.join(MONITOR_DIR, f"{target}_monitor.csv")
    
    if target and os.path.exists(c_path):
        # 1. 导出区
        with st.expander("📂 数据导出 (Excel/JSON/TXT)", expanded=False):
            em1, em2, em3 = st.columns(3)
            if em1.button("📂 打开目录", key="open_dir"):
                try: 
                    if os.name == 'nt': os.startfile(MONITOR_DIR)
                    else: subprocess.call(['open', MONITOR_DIR])
                except: st.error("无法打开目录")
            
            try:
                df_exp = pd.read_csv(c_path)
                em2.download_button("⬇️ 导出 JSON", df_exp.to_json(orient='records', force_ascii=False), f"{target}.json", "application/json")
                
                txt_rpt = f"=== 监控日志: {target} ===\n\n"
                for _, r in df_exp.iterrows():
                    txt_rpt += f"[{r['time_str']}] 播放:{r['view']} 评论:{r['reply']} 点赞:{r['like']}\n"
                em3.download_button("⬇️ 导出 TXT", txt_rpt, f"{target}.txt", "text/plain")
            except: pass

        # 2. 运行时进度条
        if active:
            start_t = st.session_state.get('monitor_start')
            if start_t:
                elapsed = datetime.now() - start_t
                total_s = st.session_state['monitor_hours'] * 3600
                st.progress(min(elapsed.total_seconds() / total_s, 1.0), text=f"运行中: {str(elapsed).split('.')[0]}")
            if st.button("🔄 刷新最新数据", use_container_width=True): st.rerun()

        # 3. 核心图表
        try:
            df = pd.read_csv(c_path)
            if not df.empty:
                last = df.iloc[-1]
                first = df.iloc[0]

                # === 🛠️ 修复点：正确的列名映射 (fav vs favorite) ===
                c_sel1, c_sel2 = st.columns([1, 6])
                with c_sel1: st.markdown("**📉 维度:**")
                with c_sel2:
                    # 这里的 value 必须对应 CSV 的列头 (timestamp,time_str,view,like,coin,fav,reply,share)
                    metric_map = {"评论": "reply", "点赞": "like", "收藏": "fav", "硬币": "coin", "分享": "share"}
                    sel_label = st.radio("选择副轴", list(metric_map.keys()), 0, horizontal=True, label_visibility="collapsed")
                    sel_col = metric_map[sel_label]

                # 计算动态增量
                val_last = last[sel_col] if sel_col in last else 0
                val_first = first[sel_col] if sel_col in first else 0
                
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("播放增量", f"+{int(last['view'] - first['view'])}")
                m2.metric(f"{sel_label}增量", f"+{int(val_last - val_first)}")
                m3.metric("当前播放", f"{int(last['view']):,}")
                m4.metric(f"当前{sel_label}", f"{int(val_last):,}")
                
                tab1, tab2 = st.tabs(["增量趋势", "总量趋势"])
                with tab1:
                    d_df = df.copy()
                    d_df['d_view'] = d_df['view'].diff().fillna(0)
                    if sel_col in d_df: d_df[f'd_{sel_col}'] = d_df[sel_col].diff().fillna(0)
                    else: d_df[f'd_{sel_col}'] = 0
                    fig1 = draw_dual_axis_chart(d_df, "time_str", "d_view", f"d_{sel_col}", "播放增量", f"{sel_label}增量")
                    st.plotly_chart(fig1, use_container_width=True)
                with tab2: 
                    if sel_col in df:
                        fig2 = draw_dual_axis_chart(df, "time_str", "view", sel_col, "播放总量", f"{sel_label}总量")
                        st.plotly_chart(fig2, use_container_width=True)

        except Exception as e: st.error(f"图表渲染异常: {e}")

    # C. 正在初始化状态处理 (防止白屏)
    elif active and not os.path.exists(c_path):
        st.warning("⏳ 正在等待数据回传... (请稍等 5 秒后点击刷新)")
        if st.button("🔄 手动刷新", key="loading_refresh"): st.rerun()
