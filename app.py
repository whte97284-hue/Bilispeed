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
import sqlite3
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from bilibili_api import user, video, comment, Credential  # 👈 加上 comment
from database import db
import jieba
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from collections import Counter
from archiver import archiver
# 解决 Matplotlib 中文乱码 (Windows)
plt.rcParams['font.sans-serif'] = ['SimHei'] 
plt.rcParams['axes.unicode_minus'] = False

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
    # 直接调用数据库写入
    db.log_system_event(module, target, status, details)

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
            # 并发获取 详情 和 Tag
            info, tags_raw = await asyncio.gather(v.get_info(), v.get_tags(), return_exceptions=True)
            
            if isinstance(info, Exception): return None
            
            # 处理 Tags (列表转逗号分隔字符串)
            tags_list = []
            if not isinstance(tags_raw, Exception) and tags_raw:
                tags_list = [t['tag_name'] for t in tags_raw]
            tags_str = ",".join(tags_list)

            if progress_callback: progress_callback()
            
            stat = info['stat']
            view = stat['view']
            coin = stat['coin']
            coin_ratio = round((coin / view * 100), 2) if view > 0 else 0
            pub_dt = datetime.fromtimestamp(info['pubdate'])
            
            return {
                "bvid": bvid, "title": info['title'], 
                "desc": info.get('desc', '').strip(),
                "tname": info.get('tname', '未知分区'),
                "tags": tags_str,  # 🟢 抓取到了标签
                "date": pub_dt.strftime('%Y-%m-%d'), 
                "datetime": pub_dt, 
                "publish_hour": pub_dt.hour,
                "duration": info['duration'], 
                "duration_str": format_duration(info['duration']),
                "cover": info['pic'],
                "play": view, 
                "coins": coin, 
                "danmaku": stat.get('danmaku', 0), # 🟢 抓取到了弹幕
                "favs": stat['favorite'], 
                "shares": stat['share'],
                "reply": stat['reply'], # 🟢 评论数
                "coin_ratio": coin_ratio
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

def run_bbdown_advanced(bbdown_path, bvid, work_dir, sessdata, status_placeholder, log_placeholder, options, progress_bar=None):
    script_dir = os.getcwd()
    cmd = [bbdown_path, bvid, "--work-dir", work_dir]
    
    # 1. 身份凭证
    local_data = os.path.join(script_dir, "BBDown.data")
    if options.get('use_local_auth') and os.path.exists(local_data):
        auth_status = "✅ 本地凭证"
    else:
        clean_sess = sessdata.replace("SESSDATA=", "").strip()
        cmd.extend(["-c", f"SESSDATA={clean_sess}"])
        auth_status = "⚠️ 网页SESSDATA"

    # 2. 代理处理
    env = os.environ.copy()
    raw_proxy = options.get('proxy', '').strip()
    if raw_proxy:
        if not raw_proxy.startswith("http"): fixed_proxy = f"http://{raw_proxy}"
        elif raw_proxy.startswith("http:") and not raw_proxy.startswith("http://"): fixed_proxy = raw_proxy.replace("http:", "http://")
        else: fixed_proxy = raw_proxy
        env["http_proxy"] = fixed_proxy
        env["https_proxy"] = fixed_proxy
        env["all_proxy"] = fixed_proxy
    
    # 3. 接口与参数
    if options.get('use_app'): cmd.append("-app")
    elif options.get('use_tv'): cmd.append("-tv")
    
    # 4. 容量预估模式 (Info Mode)
    if options.get('info_only'):
        cmd.append("-info")
    else:
        # 下载模式参数
        if options.get('use_aria2'):
            if shutil.which("aria2c") or os.path.exists(os.path.join(script_dir, "aria2c.exe")):
                cmd.append("--use-aria2c") 
        
        res = options.get('resolution')
        if res and res != "自动 (Auto)": cmd.extend(["--dfn-priority", res])
        
        enc = options.get('encoding')
        if enc and enc != "自动 (Auto)": cmd.extend(["--encoding-priority", enc])
        
        if options.get('audio_only'): cmd.append("--audio-only") 
        if options.get('p_range'): cmd.extend(["-p", options['p_range']])
        elif options.get('download_all'): cmd.append("-p ALL")

    # 5. 启动进程
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding='gbk', errors='replace',
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0, bufsize=1,
        env=env, cwd=script_dir
    )
    
    full_log = ""
    last_update_time = 0 
    
    if not options.get('info_only'):
        log_placeholder.code("🚀 引擎预热中...", language="text")

    # 6. 实时日志解析循环
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None: break
        if line:
            clean_line = line.strip()
            full_log += line
            if len(full_log) > 5000: full_log = full_log[-5000:]
            
            current_time = time.time()
            
            # === 非阻塞进度反馈 (Regex 解析) ===
            if progress_bar and not options.get('info_only'):
                try:
                    # 抓取百分比 (例如: 15.5%)
                    match_percent = re.search(r'(\d{1,3}\.\d{1,2})%', clean_line)
                    if match_percent:
                        pct = float(match_percent.group(1))
                        progress_bar.progress(min(pct / 100, 1.0))
                    
                    # 抓取速度 (例如: 2.3 MiB/s)
                    match_speed = re.search(r'(\d+\.\d+ [KMGT]iB/s)', clean_line)
                    if match_speed:
                        speed = match_speed.group(1)
                        status_placeholder.info(f"🚀 下载中... 速度: {speed} | 身份: {auth_status}")
                except: pass

            # === 日志限速刷新 (0.2s) ===
            if current_time - last_update_time > 0.2:
                if not options.get('info_only'):
                    log_placeholder.code(full_log, language="text")
                last_update_time = current_time

    if not options.get('info_only'):
        log_placeholder.code(full_log, language="text")
    
    # 如果是 info 模式，返回完整日志供解析
    if options.get('info_only'):
        return full_log
        
    return process.returncode == 0
    
# ================= 👁️ 监控核心 (Sentinel) =================

def monitor_worker(bvid, interval_min, duration_hours, sentinel_enabled, sentinel_interval_min, stop_event, sessdata):
    """后台监控线程：数据库版"""
    cred = Credential(sessdata=urllib.parse.unquote(sessdata.strip()))
    v = video.Video(bvid=bvid, credential=cred)
    
    # 🔴 删除旧的 CSV 初始化代码 ...
    
    end_time = datetime.now() + timedelta(hours=duration_hours)
    interval_sec = interval_min * 60
    
    # 哨兵状态
    last_reply_check_time = datetime.now()
    last_reply_count = None
    
    log_task("实时监控", bvid, "🟢 启动", f"哨兵: {'ON' if sentinel_enabled else 'OFF'}")
    
    # 1. 先存入视频基础信息 (确保外键关联)
    try:
        base_info = asyncio.run(v.get_info())
        db.upsert_video_info(
            bvid=bvid, 
            title=base_info['title'], 
            cover=base_info['pic'],
            owner_name=base_info['owner']['name'],
            owner_uid=base_info['owner']['mid'],
            pubdate=datetime.fromtimestamp(base_info['pubdate'])
        )
    except: pass # 忽略初始化错误

    while not stop_event.is_set():
        if datetime.now() > end_time:
            log_task("实时监控", bvid, "⏹️ 结束", "达到设定时长")
            break
            
        try:
            # 获取数据
            info = asyncio.run(v.get_info())
            stat = info['stat']
            now = datetime.now()
            
            # 🟢 新代码：写入数据库监控表
            db.insert_monitor_data(bvid, stat)
            
            # 哨兵检测逻辑
            if sentinel_enabled:
                if last_reply_count is None: last_reply_count = stat['reply']
                elif (now - last_reply_check_time).total_seconds() / 60 >= sentinel_interval_min:
                    diff = stat['reply'] - last_reply_count
                    if diff < 0:
                        # 🟢 新代码：写入哨兵日志表
                        db.insert_sentinel_alert(bvid, last_reply_count, stat['reply'], diff, "疑似删评")
                        log_task("哨兵警告", bvid, "⚠️ 异常", f"评论减少 {diff}")
                    last_reply_count = stat['reply']
                    last_reply_check_time = now

        except Exception as e:
            log_task("实时监控", bvid, "⚠️ 异常", str(e)[:20])
            
        for _ in range(int(interval_sec)):
            if stop_event.is_set(): break
            time.sleep(1)

# ================= 🖥️ 界面布局 =================

# ================= 🖥️ 侧边栏 (V50.3: 会员状态精准识别版) =================

with st.sidebar:
    st.markdown("### 💠 REI SYSTEM")
    
    mode = st.radio(
        "导航", 
        ["数据洞察", "视频下载", "智能归档", "实时监控", "舆情分析"], 
        label_visibility="collapsed"
    )
    
    st.write("") 
    
    # === 🟢 核心升级：智能账号看板 (V50.3) ===
    with st.expander("👤 账号状态", expanded=True):
        # 1. 尝试自动同步
        bbdown_file = "BBDown.data"
        if os.path.exists(bbdown_file):
            try:
                with open(bbdown_file, "r", encoding='utf-8') as f:
                    content = f.read()
                    if "SESSDATA=" in content:
                        start = content.find("SESSDATA=") + 9
                        end = content.find(";", start)
                        if end == -1: end = len(content)
                        file_sess = content[start:end].strip()
                        
                        if file_sess and file_sess != st.session_state.get('sessdata', ''):
                            st.session_state.sessdata = file_sess
                            save_settings()
                            time.sleep(0.1)
            except: pass

        # 2. 验证逻辑
        current_sess = st.session_state.get('sessdata', '')
        user_info = None
        is_valid = False
        err_msg = ""
        
        if current_sess:
            try:
                cred = Credential(sessdata=current_sess)
                # 使用 user.get_self_info 模块函数
                user_info = asyncio.run(asyncio.wait_for(user.get_self_info(cred), timeout=5))
                is_valid = True
            except asyncio.TimeoutError:
                err_msg = "验证超时 (网络不通)"
            except Exception as e:
                err_msg = str(e)
                if "401" in err_msg: err_msg = "SESSDATA 无效/已过期"

        # 3. UI 展示
        if is_valid and user_info:
            # === A. 登录成功 ===
            c_ava, c_info = st.columns([1, 2.5])
            with c_ava:
                try: st.image(user_info['face'], use_container_width=True)
                except: st.text("🖼️")
            
            # 🟢 修复：精准解析 VIP 状态
            with c_info:
                vip_data = user_info.get('vip', {})
                vip_status = vip_data.get('status') # 1: 活跃, 0: 过期
                vip_type = vip_data.get('type')     # 2: 年度, 1: 月度
                
                vip_label = "普通用户"
                status_color = "⚪"
                
                if vip_status == 1:
                    status_color = "🟢"
                    if vip_type == 2:
                        vip_label = "年度大会员"
                    elif vip_type == 1:
                        vip_label = "大会员"
                else:
                    status_color = "⚪"
                
                st.markdown(f"**{user_info['name']}**")
                st.caption(f"{status_color} {vip_label}")
            
            if st.button("🚪 退出 / 换号"):
                st.session_state.sessdata = ""
                if os.path.exists(bbdown_file): os.remove(bbdown_file)
                save_settings()
                st.rerun()

        else:
            # === B. 未登录或验证失败 ===
            if current_sess:
                st.error(f"🔴 验证失败: {err_msg}")
                if os.path.exists(bbdown_file):
                    st.success("✅ 本地凭证文件存在")
                    if st.button("📂 强制读取 BBDown.data"):
                        with open(bbdown_file, "r", encoding='utf-8') as f:
                            c_str = f.read()
                            start = c_str.find("SESSDATA=") + 9
                            end = c_str.find(";", start)
                            if end == -1: end = len(c_str)
                            st.session_state.sessdata = c_str[start:end]
                            save_settings()
                        st.rerun()
            else:
                st.info("⚪ 请先扫码登录")
            
            # 扫码按钮
            if st.button("📱 扫码登录 (BBDown)", use_container_width=True, type="primary"):
                bbdown_exe = check_tool("BBDown.exe")
                if not bbdown_exe:
                    st.error("未找到 BBDown.exe")
                else:
                    st_status = st.empty()
                    st_qr = st.empty()
                    
                    try:
                        if os.path.exists(bbdown_file): os.remove(bbdown_file)
                        if os.path.exists("qrcode.png"): os.remove("qrcode.png")
                        
                        st_status.info("⏳ 正在获取二维码...")
                        
                        proc = subprocess.Popen(
                            [bbdown_exe, "login"], 
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                            text=True, encoding='gbk', errors='ignore',
                            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                            bufsize=1
                        )
                        
                        while True:
                            line = proc.stdout.readline()
                            if not line and proc.poll() is not None: break
                            
                            if line:
                                if "qrcode.png" in line or os.path.exists("qrcode.png"):
                                    time.sleep(0.5)
                                    if os.path.exists("qrcode.png"):
                                        st_status.success("📸 请使用 B站 App 扫码")
                                        st_qr.image("qrcode.png", width=180)
                                
                                if "Login successful" in line or "登录成功" in line:
                                    st_status.success("✅ 登录成功！")
                                    time.sleep(3) 
                                    proc.terminate()
                                    st.rerun() 
                                    break
                    except Exception as e:
                        st.error(f"错误: {e}")

    with st.expander("⚙️ 引擎参数"):
        concurrency = st.slider("并发线程", 1, 10, 5)

    st.divider()
    st.markdown("**📋 系统日志**")
    try:
        logs = db.get_system_logs(20)
        log_html = ""
        for l in logs:
            sts = l.get('status', 'UNK')
            dot_class = "dot-green" if any(x in sts for x in ["成功","完成","启动"]) else "dot-warn" if any(x in sts for x in ["失败","异常","停止"]) else "dot-blue"
            log_html += f"<div><span class='status-dot {dot_class}'></span><span style='color:#78909C'>[{l['time_str'][-5:]}]</span> {l['target'][:8]}: {sts}</div>"
        st.markdown(f'<div class="task-log-box" style="height:200px;">{log_html}</div>', unsafe_allow_html=True)
    except: st.caption("日志服务暂不可用")
    
    st.markdown("---")
    st.markdown(f"""<div style="text-align:center;color:#B0BEC5;font-size:12px;">BiliCommander V50.3<br>Ultimate Edition</div>""", unsafe_allow_html=True)

# === 模块 1: 数据洞察 (V45.0: 字段全修复版) ===
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
                        cred = Credential(sessdata=urllib.parse.unquote(st.session_state.sessdata))
                        status_text = st.empty()
                        st.session_state['current_uid_view'] = None 
                        
                        for i, uid in enumerate(uids):
                            try:
                                u = user.User(int(uid), credential=cred)
                                info = await u.get_user_info()
                                db.upsert_uploader(info['mid'], info['name'], info['face'])
                                
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
                                
                                for v in valid:
                                    last_stat = db.get_latest_stat(v['bvid'])
                                    
                                    # 🟢 写入：映射 desc -> description
                                    video_meta = {
                                        'bvid': v['bvid'], 'mid': info['mid'], 
                                        'title': v['title'], 'pic': v['cover'], 
                                        'description': v['desc'], # 👈 关键点
                                        'tname': v['tname'], 
                                        'tags': v['tags'],
                                        'pubdate': v['datetime'], 'duration': v['duration']
                                    }
                                    db.upsert_video(video_meta)
                                    
                                    stat_data = {
                                        'view': v['play'], 'like': 0, 'coin': v['coins'], 
                                        'danmaku': v['danmaku'],
                                        'favorite': v['favs'], 'reply': v['reply'], 'share': v['shares']
                                    }
                                    db.insert_stats(v['bvid'], stat_data, source="insight_scan")
                                
                                log_task("数据分析", info['name'], "成功", f"{len(valid)}条")
                                st.session_state['current_uid_view'] = str(uid)
                                
                            except Exception as e: 
                                log_task("数据分析", uid, "异常", str(e))
                                st.error(f"抓取 {uid} 失败: {e}")
                                
                        status_text.success("✅ 抓取完成")
                        time.sleep(1)
                        st.rerun()

                    with st.spinner("数据链路连接中..."): 
                        asyncio.run(quick_mine())

    # 查看区
    all_uploaders = db.get_all_uploaders()
    
    if all_uploaders:
        st.write("---")
        up_options = [f"{u[1]} ({u[0]})" for u in all_uploaders]
        
        default_idx = 0
        if st.session_state.get('current_uid_view'):
            for idx, opt in enumerate(up_options):
                if str(st.session_state['current_uid_view']) in opt:
                    default_idx = idx
                    break
        
        sel_up = st.selectbox("📂 选择已归档的 UP 主", up_options, index=default_idx)
        
        if sel_up:
            target_uid = sel_up.split('(')[-1].replace(')', '')
            df = db.get_uploader_videos_snapshot(target_uid)
            
            if not df.empty:
                total_view = df['view'].sum()
                total_coin = df['coin'].sum()
                total_reply = df['reply'].sum()
                
                st.markdown(f"### 📊 {sel_up.split('(')[0]}")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("总播放", f"{total_view:,}")
                m2.metric("总硬币", f"{total_coin:,}")
                m3.metric("总评论", f"{total_reply:,}")
                m4.metric("视频数", len(df))
                
                df['url'] = df['bvid'].apply(lambda x: f"https://www.bilibili.com/video/{x}")
                df['coin_ratio'] = df.apply(lambda x: x['coin']/x['view'] if x['view']>0 else 0, axis=1)
                
                tab1, tab2 = st.tabs(["📄 详细列表", "📈 趋势图"])
                
                with tab1:
                    st.dataframe(
                        df,
                        column_config={
                            "cover": st.column_config.ImageColumn("封面", width="small"),
                            "title": st.column_config.TextColumn("标题", width="medium"),
                            "url": st.column_config.LinkColumn("链接", display_text="点击观看", width="small"),
                            "tname": st.column_config.TextColumn("分区", width="small"),
                            "tags": st.column_config.TextColumn("标签", width="medium"),
                            # 🟢 显示：description
                            "description": st.column_config.TextColumn("简介", width="large", help="视频简介"),
                            
                            "view": st.column_config.NumberColumn("播放", format="%d"),
                            "danmaku": st.column_config.NumberColumn("弹幕", format="%d"),
                            "reply": st.column_config.NumberColumn("评论", format="%d"),
                            "coin": st.column_config.NumberColumn("硬币", format="%d"),
                            "coin_ratio": st.column_config.NumberColumn("币/播", format="%.2f%%"),
                            "record_time": st.column_config.DatetimeColumn("抓取时间", format="MM-DD HH:mm")
                        },
                        column_order=[
                            "cover", "title", "url", "tname", "tags", 
                            "view", "danmaku", "reply", "coin", "coin_ratio", "description", "record_time"
                        ],
                        use_container_width=True,
                        height=600
                    )
                
                with tab2:
                    fig = draw_dual_axis_chart(df.sort_values("pubdate"), "pubdate", "view", "coin", "发布时间-播放量", "发布时间-硬币")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("无数据")
    else:
        st.info("数据库为空，请在上方抓取。")

# === 模块 2: 视频下载 (V42.0: 容量预估 + 进度条 + 批量修复) ===
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
                    if not st.session_state.sessdata: st.warning("请先配置 SESSDATA")
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
                use_tv = o3.checkbox("TV端接口", value=True, help="推荐开启！", key="chk_tv_up")
                check_danmaku = o4.checkbox("下载弹幕", value=True, key="chk_dm_up")

            to_dl = edited[edited['selected']==True]
            if st.button(f"🚀 批量下载选中的 {len(to_dl)} 个视频", type="primary", disabled=len(to_dl)==0, use_container_width=True, key="btn_start_batch"):
                dl_folder = os.path.join(VIDEOS_DIR, f"{data['name']}_{data['uid']}")
                if not os.path.exists(dl_folder): os.makedirs(dl_folder)
                
                # 🟢 批量下载也分离 UI
                prog = st.progress(0)
                status_box = st.empty()
                log_box = st.empty()
                
                for idx, row in enumerate(to_dl.itertuples()):
                    status_box.info(f"🔄 [{idx+1}/{len(to_dl)}] 正在处理: {row.title}")
                    
                    opts = {
                        'resolution': opt_res, 'encoding': opt_code, 'danmaku': check_danmaku, 
                        'use_tv': use_tv, 'use_web_api': not use_tv, 
                        'use_local_auth': True, 'use_aria2': False
                    }
                    sess_clean = urllib.parse.unquote(st.session_state.sessdata.strip())
                    
                    # 调用 updated 函数
                    run_bbdown_advanced(bbdown_path, row.bvid, dl_folder, sess_clean, status_box, log_box, opts)
                    prog.progress((idx+1)/len(to_dl))
                
                status_box.success(f"✅ 全部完成！保存至: {dl_folder}")
                log_box.empty()
                try: 
                    if os.name == 'nt': os.startfile(dl_folder)
                except: pass

    # --- 🔵 Tab 2: 番剧/直链/下载 (V42.0: 容量预估 + 进度条) ---
    with tab_bangumi:
        st.caption("支持解析：番剧 Season (ss)、番剧 Episode (ep)、多P视频 (BV)")
        
        with st.container():
            col_in, col_btn = st.columns([4, 1], vertical_alignment="bottom")
            with col_in:
                url_input = st.text_input("资源链接 / ID", placeholder="https://... 或 BV...", key="input_url_parse")
            with col_btn:
                if st.button("🔍 解析目录", type="primary", use_container_width=True, key="btn_parse_url"):
                    if not url_input: st.warning("请先输入链接")
                    elif not st.session_state.sessdata: st.error("请先配置 SESSDATA")
                    else:
                        st.session_state['parsed_episodes'] = None 
                        with st.spinner("正在获取分集列表..."):
                            async def parse_content(url, sess):
                                try:
                                    cred = Credential(sessdata=urllib.parse.unquote(sess.strip()))
                                    import re
                                    target_id = ""
                                    mode = "video"
                                    if "ss" in url: target_id = re.search(r"ss(\d+)", url).group(1); mode = "season"
                                    elif "ep" in url: target_id = re.search(r"ep(\d+)", url).group(1); mode = "ep"
                                    elif "BV" in url: target_id = re.search(r"(BV\w+)", url).group(1); mode = "video"
                                    
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

        if st.session_state.get('parsed_video') or st.session_state.get('parsed_bangumi'):
            st.divider()
            with st.expander("🌍 港澳台解锁 / 网络加速", expanded=True):
                c_net1, c_net2 = st.columns([3, 1])
                with c_net1: proxy_input = st.text_input("HTTP代理地址", placeholder="例如 http://127.0.0.1:7890", key="proxy_input_fix")
                with c_net2:
                    st.write(""); st.write("")
                    use_aria2 = st.checkbox("Aria2 加速", value=False, key="aria2_fix")

            with st.expander("⚙️ 画质与接口策略", expanded=True):
                b1, b2, b3, b4 = st.columns(4)
                res_bg = b1.selectbox("画质", ["自动 (Auto)", "1080P", "1080P+", "4K"], index=0, key="rf_fix")
                code_bg = b2.selectbox("编码", ["自动 (Auto)", "avc", "hevc", "av1"], index=0, key="cf_fix")
                api_mode = b3.radio("接口模式", ["Web接口 (推荐)", "APP接口", "TV接口"], index=0, key="api_mode_sel")
                use_local_auth = b4.checkbox("使用扫码凭证", value=True, key="use_local_auth")
                
                use_web = "Web" in api_mode
                use_app = "APP" in api_mode
                use_tv = "TV" in api_mode

            # === 🟢 新增：功能按钮组 ===
            c_act1, c_act2 = st.columns(2)
            
            # 按钮 1: 容量预估
            with c_act1:
                if st.button("📏 预估容量 / 获取流信息", use_container_width=True):
                    raw = st.session_state.sessdata
                    clean_sess = raw.replace("SESSDATA=", "").strip()
                    bg_folder = os.path.join(VIDEOS_DIR, "Downloads_Direct")
                    
                    info_opts = {
                        'use_app': use_app, 'use_tv': use_tv, 'use_web_api': use_web,
                        'proxy': proxy_input.strip(), 'use_local_auth': use_local_auth,
                        'info_only': True # 👈 开启 Info 模式
                    }
                    
                    status_box = st.empty()
                    log_box = st.empty()
                    
                    with st.spinner("正在探测视频流信息..."):
                        info_log = run_bbdown_advanced(bbdown_path, url_input.strip(), bg_folder, clean_sess, status_box, log_box, info_opts)
                        if "Title:" in info_log:
                            st.success("获取成功！请查看下方详情：")
                            st.text_area("视频流信息", info_log, height=300)
                        else: st.error("获取失败，请检查网络或代理。")

            # 按钮 2: 开始下载 (带进度条)
            with c_act2:
                if st.button("🚀 开始下载", type="primary", use_container_width=True, key="btn_dl_final_fix"):
                    raw = st.session_state.sessdata
                    clean_sess = raw.replace("SESSDATA=", "").strip()
                    bg_folder = os.path.join(VIDEOS_DIR, "Downloads_Direct")
                    if not os.path.exists(bg_folder): os.makedirs(bg_folder)
                    
                    status_box = st.empty()
                    prog_bar = st.progress(0, text="准备开始...")
                    log_box = st.empty()
                    
                    p_arg = ""
                    if selected_indices == "ALL": p_arg = "ALL"
                    elif isinstance(selected_indices, list): p_arg = ",".join(map(str, selected_indices))
                    elif isinstance(selected_indices, str): p_arg = selected_indices
                    
                    final_opts = {
                        'resolution': res_bg, 'encoding': code_bg, 
                        'use_app': use_app, 'use_tv': use_tv, 'use_web_api': use_web,
                        'p_range': p_arg, 'proxy': proxy_input.strip(),
                        'use_aria2': use_aria2, 'use_local_auth': use_local_auth
                    }

                    success = run_bbdown_advanced(bbdown_path, url_input.strip(), bg_folder, clean_sess, status_box, log_box, final_opts, progress_bar=prog_bar)
                    
                    if success:
                        prog_bar.progress(1.0, text="✅ 下载完成")
                        try: 
                            if os.name == 'nt': os.startfile(bg_folder)
                        except: pass
                    else:
                        prog_bar.progress(0, text="❌ 下载失败")
                    
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

# === 模块 4: 实时监控 (V38.0 SQL-DB 适配版) ===
elif mode == "实时监控":
    st.title("🔴 实时监控 (SQL版)")
    
    # --- 1. 历史档案归档区 (从数据库读取) ---
    # 使用 try-except 防止数据库未初始化时报错
    try:
        # 获取所有有监控记录的视频列表
        with sqlite3.connect("history/bili_data.db") as conn:
            cursor = conn.cursor()
            # 关联查询：从 stats 表反查 videos 表获取标题
            cursor.execute("""
                SELECT DISTINCT v.bvid, v.title 
                FROM video_stats s
                JOIN videos v ON s.bvid = v.bvid
                ORDER BY s.record_time DESC
            """)
            video_list = cursor.fetchall()
            
        if video_list:
            with st.expander("📂 监控档案室 (历史记录)", expanded=False):
                # 格式化选项: "标题 (BVID)"
                options = [f"{v[1]} ({v[0]})" for v in video_list]
                sel_opt = st.selectbox("选择历史记录", ["-- 查看旧数据 --"] + options)
                
                if sel_opt != "-- 查看旧数据 --":
                    # 解析 BVID
                    hist_bvid = sel_opt.split('(')[-1].replace(')', '')
                    
                    # 从 DB 获取数据
                    df_hist = db.get_monitor_history(hist_bvid)
                    
                    if not df_hist.empty:
                        st.caption(f"📅 记录时间: {df_hist.iloc[0]['time_str']} ~ {df_hist.iloc[-1]['time_str']} | 总数据点: {len(df_hist)}")
                        
                        # 绘图
                        y2_col = "reply" if "reply" in df_hist.columns else df_hist.columns[-1]
                        fig = draw_dual_axis_chart(df_hist, "time_str", "view", y2_col, "播放趋势", "互动趋势")
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # 哨兵记录
                        df_sentinel = db.get_sentinel_logs(hist_bvid)
                        if not df_sentinel.empty:
                            st.markdown(f'<div class="sentinel-alert">⚠️ 历史警报：检测到 {len(df_sentinel)} 次异常！</div>', unsafe_allow_html=True)
                            st.dataframe(df_sentinel, use_container_width=True)
                    else:
                        st.info("该视频暂无详细数据点")
    except Exception as e:
        # 刚开始运行时数据库可能为空，忽略此错误
        if "no such table" not in str(e):
            st.warning(f"历史记录读取暂不可用: {e}")

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
                    # 写入数据库日志
                    db.log_system_event("监控", st.session_state.get('monitor_target'), "停止", "用户手动停止")
                    time.sleep(1)
                    st.rerun()
            else:
                st.write("") # 占位

    # --- 3. 哨兵设置 ---
    if not active:
        with st.expander("🛡️ 哨兵防御设置 (Sentinel)", expanded=True):
            col_s1, col_s2 = st.columns([1, 4])
            sentinel_on = col_s1.toggle("启用删评检测", value=True)
            sentinel_freq = col_s2.slider("检测周期 (分钟)", 5, 60, 30)

    # --- 4. 启动逻辑区 ---
    # 确定目标 BVID
    target = st.session_state.get('monitor_target', '') if active else (mon_bvid.split('?')[0].split('/')[-1] if mon_bvid else "")
    
    # 检查数据库中是否已有该视频的数据
    has_history = False
    if target:
        try:
            temp_df = db.get_monitor_history(target)
            if not temp_df.empty: has_history = True
        except: pass

    if not active and target:
        def safe_launch():
            if not st.session_state.sessdata: 
                st.error("🚫 请先在左侧填写 SESSDATA")
                return

            with st.spinner("🔄 初始化监控链路..."):
                try:
                    if 'monitor_stop_event' not in st.session_state:
                        st.session_state['monitor_stop_event'] = threading.Event()
                    st.session_state['monitor_stop_event'].clear()
                    
                    # 获取基础信息用于更新 Session 和 DB 基础表
                    info = asyncio.run(get_video_basic_info(target, st.session_state.sessdata))
                    
                    if info:
                        # 存入 Session 用于 UI 显示
                        st.session_state.update({
                            'monitor_info': info, 'monitor_target': target, 
                            'monitor_start': datetime.now(), 'monitor_hours': mon_dur, 
                            'monitor_thread_active': True
                        })
                        
                        # 启动线程 (monitor_worker 内部已经改为写 DB 了)
                        t = threading.Thread(
                            target=monitor_worker, 
                            args=(target, mon_int, mon_dur, sentinel_on, sentinel_freq, st.session_state['monitor_stop_event'], st.session_state.sessdata), 
                            daemon=True
                        )
                        t.start()
                        
                        time.sleep(2) 
                        st.success("✅ 启动成功！")
                        st.rerun()
                    else:
                        st.error("❌ 无法获取信息，请检查 SESSDATA")
                except Exception as e: st.error(f"启动异常: {e}")

        # 按钮逻辑
        if has_history:
            st.warning(f"数据库中已存在 {target} 的记录")
            cc1, cc2 = st.columns(2)
            if cc1.button("🔗 继续监控 (追加)", type="primary", use_container_width=True, key="btn_resume"): 
                safe_launch()
            if cc2.button("🗑️ 清空旧数据并重录", type="secondary", use_container_width=True, key="btn_overwrite"): 
                # 清空数据库中该 BVID 的数据
                try:
                    with db._get_conn() as conn:
                        conn.execute("DELETE FROM video_stats WHERE bvid = ?", (target,))
                        conn.execute("DELETE FROM sentinel_logs WHERE bvid = ?", (target,))
                        conn.commit()
                    safe_launch()
                except Exception as e: st.error(f"清理失败: {e}")
        else:
            if st.button("▶️ 启动新监控", type="primary", use_container_width=True, key="btn_start"):
                safe_launch()

    # --- 5. 监控面板 (数据显示) ---
    st.write("---")
    
    # A. 视频卡片 (优先显示内存中的信息)
    info_mem = st.session_state.get('monitor_info')
    if info_mem:
        st.markdown(f"""<div class="video-card"><img src="{info_mem['proxy_pic']}" class="video-cover"><div class="video-info"><div class="video-title">{info_mem['title']}</div><div class="video-meta"><span>UP: {info_mem['owner']}</span> <span>发布: {info_mem['pubdate']}</span></div></div></div>""", unsafe_allow_html=True)

    # B. 数据与图表 (从数据库读取)
    if target:
        try:
            # 🟢 从数据库获取实时数据 DataFrame
            df = db.get_monitor_history(target)
            
            if not df.empty:
                # 1. 导出区
                with st.expander("📂 数据导出 (Excel/JSON/CSV)", expanded=False):
                    em1, em2, em3 = st.columns(3)
                    if em1.button("📂 打开目录", key="open_dir"):
                        os.startfile(os.getcwd())
                    
                    em2.download_button("⬇️ 导出 JSON", df.to_json(orient='records', force_ascii=False), f"{target}.json", "application/json")
                    em3.download_button("⬇️ 导出 CSV", df.to_csv(index=False).encode('utf-8-sig'), f"{target}.csv", "text/csv")

                # 2. 运行时进度条
                if active:
                    start_t = st.session_state.get('monitor_start')
                    if start_t:
                        elapsed = datetime.now() - start_t
                        total_s = st.session_state['monitor_hours'] * 3600
                        st.progress(min(elapsed.total_seconds() / total_s, 1.0), text=f"运行中... ({len(df)} 条记录)")
                    if st.button("🔄 刷新最新数据", use_container_width=True): st.rerun()

                # 3. 核心图表逻辑
                last = df.iloc[-1]
                first = df.iloc[0]

                c_sel1, c_sel2 = st.columns([1, 6])
                with c_sel1: st.markdown("**📉 维度:**")
                with c_sel2:
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

    # C. 正在初始化状态处理
    elif active:
        st.info("⏳ 正在等待第一条数据入库... (约需 5-10 秒)")
        if st.button("🔄 手动刷新", key="loading_refresh"): st.rerun()
        
# === 模块 5: 舆情分析 (V49.0: 极简稳定回退版) ===
elif mode == "舆情分析":
    st.title("🧠 舆情与粉丝画像")

    # 1. 视频选择
    bvid_options = []
    try:
        with sqlite3.connect("history/bili_data.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT bvid, title FROM videos ORDER BY pubdate DESC")
            bvid_options = [f"{row[1]} ({row[0]})" for row in cursor.fetchall()]
    except: pass

    c_ctrl, c_disp = st.columns([1, 3])
    
    with c_ctrl:
        st.info("第一步：确定目标视频")
        input_method = st.radio("数据来源", ["选择已有记录", "手动输入 BVID"], index=0)
        
        target_bvid = ""
        if input_method == "选择已有记录":
            if bvid_options:
                sel_video = st.selectbox("选择库中视频", bvid_options)
                target_bvid = sel_video.split('(')[-1].replace(')', '')
            else:
                st.warning("数据库为空，请切换到手动输入。")
        else:
            target_bvid = st.text_input("输入 BVID", placeholder="BV1xxxx...")

        st.write("---")
        st.markdown("**🕷️ 抓取设置**")
        fetch_limit = st.slider("抓取页数 (每页20条)", 1, 50, 5)
        # 🔴 移除深度抓取开关，回归纯净
        
        # 调试区
        debug_box = st.empty()
        
        btn_disabled = not target_bvid
        if st.button("🚀 抓取/更新评论", type="primary", use_container_width=True, disabled=btn_disabled):
            if not st.session_state.sessdata:
                st.error("请先配置 SESSDATA")
            else:
                async def fetch_comments():
                    try:
                        cred = Credential(sessdata=urllib.parse.unquote(st.session_state.sessdata))
                        v = video.Video(bvid=target_bvid, credential=cred)
                        
                        # 1. 获取基础信息
                        try:
                            base_info = await v.get_info()
                            aid = base_info['aid']
                            debug_box.info(f"✅ 锁定 AID: {aid}")
                            
                            db.upsert_video_info(
                                bvid=target_bvid, title=base_info['title'], cover=base_info['pic'],
                                owner_name=base_info['owner']['name'], owner_uid=base_info['owner']['mid'],
                                pubdate=datetime.fromtimestamp(base_info['pubdate'])
                            )
                        except Exception as e:
                            st.error(f"❌ 视频信息获取失败: {e}")
                            return 0
                        
                        # === 2. 核心适配逻辑 (保留最稳的伪装术) ===
                        try:
                            from bilibili_api.comment import ResourceType
                            type_val = ResourceType.VIDEO
                        except ImportError:
                            class MagicType(int):
                                @property
                                def value(self): return 1
                            type_val = MagicType(1)

                        # 参数探测 (保留这个，因为它确实能解决 page/next 问题)
                        valid_key = None
                        start_idx = 1
                        
                        candidates = [("next", 0), ("page", 1), ("pn", 1), ("page_index", 1)]
                        
                        debug_box.info("🔍 正在连接评论接口...")
                        for k, s in candidates:
                            try:
                                # 试探性抓取
                                res = await comment.get_comments(oid=aid, type_=type_val, credential=cred, **{k: s})
                                if res and 'replies' in res:
                                    valid_key = k
                                    start_idx = s
                                    debug_box.success(f"✅ 接口连接成功 ({k})")
                                    break
                            except: continue
                        
                        if not valid_key:
                            # 兜底
                            valid_key = "next"
                            start_idx = 0

                        # === 3. 极简抓取循环 (无递归，无子楼层) ===
                        all_comments = []
                        prog = st.progress(0, text="正在获取数据...")
                        
                        for i in range(fetch_limit):
                            # 计算页码
                            current_val = start_idx + i
                            kwargs = {"oid": aid, "type_": type_val, "credential": cred, valid_key: current_val}
                            
                            try:
                                c_data = await comment.get_comments(**kwargs)
                            except Exception as e:
                                debug_box.warning(f"⚠️ 第 {i+1} 页获取中断: {e}")
                                break
                                
                            roots = c_data.get('replies', [])
                            if not roots: 
                                debug_box.caption("✅ 已抓取所有可用评论")
                                break 
                            
                            # 🟢 扁平化处理：只存主楼 + 自带的热评
                            for root in roots:
                                all_comments.append(root)
                                # 这里的 replies 是 B 站默认送的前3条，直接拿走，不深入请求
                                if root.get('replies'):
                                    all_comments.extend(root['replies'])
                            
                            prog.progress((i + 1) / fetch_limit)
                            debug_box.text(f"📥 已入库: {len(all_comments)} 条...")
                            await asyncio.sleep(0.3) # 稍微快一点，因为只抓主楼
                        
                        if all_comments:
                            db.insert_comments_batch(target_bvid, all_comments)
                            return len(all_comments)
                        return 0
                        
                    except Exception as e:
                        st.error(f"未知错误: {str(e)}")
                        return 0

                with st.spinner("正在同步评论数据..."):
                    count = asyncio.run(fetch_comments())
                    if count > 0:
                        st.success(f"✅ 成功同步 {count} 条评论！")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning("未获取到新评论。")

    # 右侧分析面板 (保持不变，功能完好)
    with c_disp:
        if not target_bvid:
            st.info("👈 请在左侧选择或输入视频 BVID")
        else:
            df_comments = db.get_comments_data(target_bvid)
            
            if df_comments.empty:
                st.info(f"暂无数据，请点击【抓取评论】。")
            else:
                st.write(f"📊 **分析样本：{len(df_comments)} 条评论**")
                
                tab1, tab2 = st.tabs(["☁️ 词云透视", "👥 粉丝画像"])
                
                with tab1:
                    with st.expander("⚙️ 排除词设置", expanded=False):
                        stop_words_input = st.text_area("排除词", "的 了 是 我 你 视频 这个 觉得 还是 哈哈 哈哈哈 UP up 回复", height=60)
                        stop_words = set(stop_words_input.split())

                    if st.button("🎨 生成词云", use_container_width=True):
                        try:
                            text_content = " ".join(df_comments['content'].astype(str).tolist())
                            words = jieba.cut(text_content)
                            filtered_words = [w for w in words if len(w) > 1 and w not in stop_words]
                            
                            if not filtered_words:
                                st.warning("内容太少")
                            else:
                                word_counts = Counter(filtered_words)
                                font_path = "C:\\Windows\\Fonts\\msyh.ttc"
                                if not os.path.exists(font_path): font_path = "simhei.ttf"
                                
                                wc = WordCloud(
                                    font_path=font_path, width=800, height=400,
                                    background_color='white', max_words=100, colormap='viridis'
                                ).generate_from_frequencies(word_counts)
                                
                                st.image(wc.to_array(), use_container_width=True)
                                
                                st.write("🔥 **高频热词**")
                                top10 = pd.DataFrame(word_counts.most_common(10), columns=["词汇", "频率"])
                                st.dataframe(top10, use_container_width=True)
                        except Exception as e: st.error(f"生成失败: {e}")

                with tab2:
                    col_p1, col_p2 = st.columns(2)
                    with col_p1:
                        st.markdown("**👫 性别分布**")
                        if 'sex' in df_comments.columns:
                            gender_counts = df_comments['sex'].value_counts()
                            if not gender_counts.empty:
                                fig_gender = go.Figure(data=[go.Pie(labels=gender_counts.index, values=gender_counts.values, hole=.4)])
                                fig_gender.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300)
                                st.plotly_chart(fig_gender, use_container_width=True)
                    with col_p2:
                        st.markdown("**🎓 等级分布**")
                        if 'level' in df_comments.columns:
                            level_counts = df_comments['level'].value_counts().sort_index()
                            if not level_counts.empty:
                                fig_level = go.Figure(data=[go.Bar(x=[f"LV {i}" for i in level_counts.index], y=level_counts.values, marker_color='#29B6F6')])
                                fig_level.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300)
                                st.plotly_chart(fig_level, use_container_width=True)
                    
                    if 'location' in df_comments.columns:
                        st.markdown("**🌍 IP 属地 TOP 10**")
                        loc_counts = df_comments['location'].value_counts().head(10)
                        if not loc_counts.empty: st.bar_chart(loc_counts)

# === 模块 6: 智能归档 (V41.0 新增) ===
elif mode == "智能归档":
    st.title("🗂️ 智能归档中心")
    
    # 获取待处理文件
    inbox_files = archiver.scan_inbox()
    
    tab1, tab2 = st.tabs(["📥 待归档区 (Inbox)", "📚 媒体库 (Library)"])
    
    # --- Tab 1: 归档操作 ---
    with tab1:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.info(f"扫描到 {len(inbox_files)} 个散乱文件 (位于 history/videos 根目录)")
        with c2:
            if st.button("📂 打开源目录", use_container_width=True):
                os.startfile(os.path.join(os.getcwd(), "history", "videos"))

        if not inbox_files:
            st.success("🎉 暂无待处理文件，你的媒体库很整洁！")
        else:
            # 文件列表展示
            df_files = pd.DataFrame(inbox_files)
            st.dataframe(
                df_files, 
                column_config={
                    "name": "文件名", 
                    "size": "大小",
                    "path": None # 隐藏完整路径
                },
                use_container_width=True, 
                height=300
            )
            
            st.write("---")
            st.subheader("🛠️ 执行操作")
            
            col_act1, col_act2 = st.columns([2, 1])
            with col_act1:
                rename_on = st.checkbox("启用智能重命名", value=True, help="将文件名修改为: [发布日期] 视频标题.mp4")
            
            with col_act2:
                if st.button("🚀 一键智能整理", type="primary", use_container_width=True):
                    with st.spinner("正在搬运与整理..."):
                        res = archiver.execute_archive(inbox_files, rename_fmt=rename_on)
                        
                    if res['fail'] == 0:
                        st.balloons()
                        st.success(f"成功归档 {res['success']} 个文件！")
                    else:
                        st.warning(f"完成，但有 {res['fail']} 个文件处理失败。")
                        
                    # 显示日志
                    with st.expander("查看详细日志", expanded=True):
                        st.text("\n".join(res['logs']))
                    
                    time.sleep(2)
                    st.rerun()

    # --- Tab 2: 已归档查看 ---
    with tab2:
        st.caption("文件存储位置: history/videos/_Archived")
        
        # 打开归档目录按钮
        if st.button("📂 打开归档总目录"):
            archive_path = os.path.join(os.getcwd(), "history", "videos", "_Archived")
            if os.path.exists(archive_path):
                os.startfile(archive_path)
            else:
                st.error("归档目录尚未创建")

        # 树状结构展示
        tree = archiver.get_archive_tree()
        if not tree:
            st.info("暂无归档记录")
        else:
            for owner, years in tree.items():
                with st.expander(f"👤 {owner}", expanded=False):
                    cols = st.columns(len(years) + 1) if len(years) < 4 else st.columns(4)
                    for i, year_info in enumerate(years):
                        year_dir = year_info.split(' ')[0]
                        # 每个年份一个小按钮，点击打开文件夹
                        if cols[i % 4].button(f"📂 {year_info}", key=f"open_{owner}_{year_dir}"):
                            target = os.path.join(os.getcwd(), "history", "videos", "_Archived", owner, year_dir)
                            os.startfile(target)
