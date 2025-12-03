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

def run_bbdown_advanced(bbdown_path, bvid, work_dir, sessdata, console_placeholder, options):
    cmd = [bbdown_path, bvid, "--work-dir", work_dir, "-c", f"SESSDATA={sessdata}"]
    if options.get('resolution'):
        res_map = {"8K": "8K", "4K": "4K", "1080P60": "1080P60", "1080P+": "1080P+", "1080P": "1080P", "720P": "720P"}
        res_val = res_map.get(options['resolution'], "")
        if res_val: cmd.extend(["--dfn-priority", res_val])
    if options.get('encoding'): cmd.extend(["--encoding-priority", options['encoding']])
    if options.get('danmaku'): cmd.append("-dd") 
    if options.get('audio_only'): cmd.append("--audio-only") 

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding='gbk', errors='replace',
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0, bufsize=1
    )
    full_log = ""
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None: break
        if line:
            full_log += line
            if len(full_log) > 5000: full_log = full_log[-5000:]
            # 实时更新控制台，不使用红色
            console_placeholder.markdown(f'<div class="console-box">{full_log}</div>', unsafe_allow_html=True)
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

with st.sidebar:
    st.markdown("### 💠 REI SYSTEM")
    
    mode = st.radio(
        "导航", 
        ["数据洞察", "视频下载", "封面提取", "实时监控"], 
        label_visibility="collapsed"
    )
    
    st.write("") 
    
    with st.expander("🔑 账号凭证 (Token)", expanded=True):
        st.text_area("SESSDATA", key="sessdata", height=80, on_change=save_settings)
    with st.expander("⚙️ 引擎参数"):
        concurrency = st.slider("并发线程", 1, 10, 5)

    st.divider()
    st.markdown("**📋 系统日志**")
    logs = load_json(TASK_LOG_FILE, [])
    log_html = ""
    for l in logs:
        sts = l.get('sts', 'UNK')
        # 逻辑修改：失败不再是红色，而是橙色(warn)
        dot_class = "dot-green" if any(x in sts for x in ["成功","完成","启动"]) else "dot-warn" if any(x in sts for x in ["失败","异常","停止"]) else "dot-blue"
        log_html += f"<div><span class='status-dot {dot_class}'></span><span style='color:#78909C'>[{l['time'][-5:]}]</span> {l['tgt']}: {sts}</div>"
    st.markdown(f'<div class="task-log-box">{log_html}</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown(f"""<div style="text-align:center;color:#B0BEC5;font-size:12px;">BiliCommander V24.2<br>Rei Edition</div>""", unsafe_allow_html=True)

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

# === 模块 2: 视频下载 ===
elif mode == "视频下载":
    st.title("🎥 视频下载")
    bbdown_path = check_tool("BBDown.exe")
    ffmpeg_path = check_tool("ffmpeg.exe")
    
    if not bbdown_path: st.warning("⚠️ 缺少 BBDown.exe，核心功能无法使用")
    else: st.info("✅ 引擎就绪: BBDown + FFmpeg" if ffmpeg_path else "⚠️ 警告: 无 FFmpeg，无法合并视频，仅能下载分离流")

    with st.container():
        c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
        with c1: dl_uid = st.text_input("输入 UID 获取列表", value="551898501")
        with c2:
            if st.button("🔍 获取列表", type="primary", use_container_width=True):
                if not st.session_state.sessdata: st.warning("缺 SESSDATA")
                else:
                    with st.spinner("获取中..."):
                        name, vlist = asyncio.run(scan_user_videos(dl_uid, st.session_state.sessdata))
                        st.session_state['dl_list'] = {"name": name, "uid": dl_uid, "v": vlist}
    
    if 'dl_list' in st.session_state:
        data = st.session_state['dl_list']
        st.success(f"已加载: {data['name']}，共 {len(data['v'])} 个视频")
        with st.expander("🎛️ 高级选项", expanded=True):
            col_opt1, col_opt2, col_opt3 = st.columns(3)
            with col_opt1: opt_res = st.selectbox("📺 画质", ["8K", "4K", "1080P60", "1080P+", "1080P", "720P"], index=1)
            with col_opt2: opt_code = st.selectbox("🎞️ 编码", ["hevc", "av1", "avc"], index=0)
            with col_opt3:
                st.write("") 
                check_danmaku = st.checkbox("下载弹幕", value=True)
                check_audio = st.checkbox("仅音频", value=False)

        df = pd.DataFrame(data['v'])
        df['selected'] = False
        edited = st.data_editor(df, column_config={"selected": st.column_config.CheckboxColumn("下载"), "pic": st.column_config.ImageColumn("封面"), "title": st.column_config.TextColumn("标题", disabled=True)}, column_order=["selected", "pic", "title", "bvid"], use_container_width=True, height=500)
        to_dl = edited[edited['selected']==True]
        
        if st.button(f"📥 启动下载 ({len(to_dl)})", type="primary", disabled=len(to_dl)==0, use_container_width=True):
            if not bbdown_path: st.error("缺少 BBDown")
            else:
                dl_folder = os.path.join(VIDEOS_DIR, f"{data['name']}_{data['uid']}")
                if not os.path.exists(dl_folder): os.makedirs(dl_folder)
                console = st.empty()
                status_text = st.empty()
                prog_bar = st.progress(0)
                dl_options = {'resolution': opt_res, 'encoding': opt_code, 'danmaku': check_danmaku, 'audio_only': check_audio}
                for idx, row in enumerate(to_dl.itertuples()):
                    status_text.info(f"[{idx+1}/{len(to_dl)}] BBDown: {row.title} ...")
                    sessdata_clean = urllib.parse.unquote(st.session_state.sessdata.strip())
                    is_ok = run_bbdown_advanced(bbdown_path, row.bvid, dl_folder, sessdata_clean, console, dl_options)
                    if is_ok: log_task("视频下载", row.title, "成功", f"Q:{opt_res}")
                    else: log_task("视频下载", row.title, "失败", "Check Console")
                    prog_bar.progress((idx+1)/len(to_dl))
                status_text.success(f"完成！保存至: {dl_folder}")
                if os.name == 'nt': os.startfile(dl_folder)

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

# === 模块 4: 实时监控 (Plotly & 去红版) ===
elif mode == "实时监控":
    st.title("🔴 实时监控")
    
    # 历史归档区
    if os.path.exists(MONITOR_DIR):
        with st.expander("📂 监控档案室"):
            csvs = [f for f in os.listdir(MONITOR_DIR) if f.endswith('_monitor.csv')]
            if csvs:
                sel_csv = st.selectbox("历史记录", ["-- 查看旧数据 --"] + csvs)
                if sel_csv != "-- 查看旧数据 --":
                    try:
                        df = pd.read_csv(os.path.join(MONITOR_DIR, sel_csv))
                        # 使用 Plotly
                        fig = draw_dual_axis_chart(df, "time_str", "view", "reply", "播放趋势", "评论趋势")
                        st.plotly_chart(fig, use_container_width=True)
                        
                        s_file = os.path.join(SENTINEL_DIR, sel_csv.replace("_monitor", "_sentinel"))
                        if os.path.exists(s_file):
                            err_df = pd.read_csv(s_file)
                            if not err_df.empty:
                                st.markdown(f'<div class="sentinel-alert">⚠️ 警报：检测到 {len(err_df)} 次异常删评行为！</div>', unsafe_allow_html=True)
                                st.dataframe(err_df)
                            else:
                                st.markdown('<div class="sentinel-ok">🛡️ 哨兵检测正常：暂无删评</div>', unsafe_allow_html=True)
                    except: st.error("文件损坏")

    with st.container():
        c1, c2, c3, c4 = st.columns([3, 1, 1, 1], vertical_alignment="bottom")
        with c1: mon_bvid = st.text_input("BVID", placeholder="BVxxxx...")
        with c2: mon_int = st.number_input("频率(分)", 1, value=5)
        with c3: mon_dur = st.number_input("时长(时)", 1, value=24)
        with c4:
            active = st.session_state.get('monitor_thread_active')
            target = mon_bvid.split('?')[0].split('/')[-1] if mon_bvid else ""
            exists = os.path.exists(os.path.join(MONITOR_DIR, f"{target}_monitor.csv"))
            
            if not active:
                if target and exists: st.write("")
                else:
                    if st.button("▶️ 启动", type="primary", use_container_width=True): pass 
            else:
                # 停止按钮不再是红色
                if st.button("⏹️ 停止", type="secondary", use_container_width=True):
                    st.session_state['monitor_stop_event'].set()
                    st.session_state['monitor_thread_active'] = False
                    log_task("监控", st.session_state.get('monitor_target'), "停止")
                    st.rerun()

    if not active:
        with st.expander("🛡️ 哨兵防御设置 (Sentinel)", expanded=True):
            col_s1, col_s2 = st.columns([1, 4])
            sentinel_on = col_s1.toggle("启用删评检测", value=True)
            sentinel_freq = col_s2.slider("检测周期 (分钟)", 5, 60, 30)

    # 逻辑启动区
    if not active and target and exists:
        st.warning(f"检测到 {target} 的历史存档！")
        cc1, cc2 = st.columns(2)
        def launch(clean=False):
            if not st.session_state.sessdata: st.warning("缺 SESSDATA"); return
            if clean:
                try: 
                    os.remove(os.path.join(MONITOR_DIR, f"{target}_monitor.csv"))
                    os.remove(os.path.join(SENTINEL_DIR, f"{target}_sentinel.csv"))
                except: pass
            with st.spinner("连接中..."):
                info = asyncio.run(get_video_basic_info(target, st.session_state.sessdata))
                if info:
                    st.session_state.update({'monitor_info': info, 'monitor_target': target, 'monitor_start': datetime.now(), 'monitor_hours': mon_dur, 'monitor_thread_active': True})
                    st.session_state['monitor_stop_event'].clear()
                    threading.Thread(target=monitor_worker, args=(target, mon_int, mon_dur, sentinel_on, sentinel_freq, st.session_state['monitor_stop_event'], st.session_state.sessdata), daemon=True).start()
                    st.rerun()
        
        if cc1.button("🔗 继续监控", type="primary", use_container_width=True): launch(False)
        if cc2.button("🆕 覆盖重录", type="secondary", use_container_width=True): launch(True)
    
    elif not active and target and not exists and st.button("▶️ 启动", key="new_run", type="primary", use_container_width=True):
         if not st.session_state.sessdata: st.warning("缺 SESSDATA")
         else:
             with st.spinner("连接中..."):
                 info = asyncio.run(get_video_basic_info(target, st.session_state.sessdata))
                 if info:
                     st.session_state.update({'monitor_info': info, 'monitor_target': target, 'monitor_start': datetime.now(), 'monitor_hours': mon_dur, 'monitor_thread_active': True})
                     st.session_state['monitor_stop_event'].clear()
                     threading.Thread(target=monitor_worker, args=(target, mon_int, mon_dur, sentinel_on, sentinel_freq, st.session_state['monitor_stop_event'], st.session_state.sessdata), daemon=True).start()
                     st.rerun()

    # 监控面板
    if 'monitor_info' in st.session_state and st.session_state['monitor_info']:
        info = st.session_state['monitor_info']
        st.markdown(f"""<div class="video-card"><img src="{info['proxy_pic']}" class="video-cover"><div class="video-info"><div class="video-title">{info['title']}</div><div class="video-meta"><span>UP: {info['owner']}</span> <span>发布: {info['pubdate']}</span></div></div></div>""", unsafe_allow_html=True)
        
        if active:
            start = st.session_state.get('monitor_start')
            if start:
                elapsed = datetime.now() - start
                total = st.session_state['monitor_hours'] * 3600
                st.progress(min(elapsed.total_seconds() / total, 1.0), text=f"运行时长: {str(elapsed).split('.')[0]}")
            if st.button("🔄 刷新数据", use_container_width=True): st.rerun()

        # 图表 (Plotly)
        t_bv = st.session_state.get('monitor_target', '')
        if not t_bv and mon_bvid: t_bv = mon_bvid.split('?')[0].split('/')[-1]
        c_path = os.path.join(MONITOR_DIR, f"{t_bv}_monitor.csv")
        s_path = os.path.join(SENTINEL_DIR, f"{t_bv}_sentinel.csv")
        
        if t_bv and os.path.exists(c_path):
            try:
                df = pd.read_csv(c_path)
                if not df.empty:
                    last = df.iloc[-1]
                    first = df.iloc[0]
                    
                    if os.path.exists(s_path):
                        err_df = pd.read_csv(s_path)
                        if not err_df.empty:
                            st.markdown(f"""<div class="sentinel-alert">⚠️ 警报：检测到 {len(err_df)} 次异常掉评！</div>""", unsafe_allow_html=True)
                        else:
                            st.markdown('<div class="sentinel-ok">🛡️ 哨兵检测正常：暂无删评</div>', unsafe_allow_html=True)

                    st.write("---")

                    # ==========================================
                    # 🆕 新增：数据维度切换控制台
                    # ==========================================
                    c_sel1, c_sel2 = st.columns([1, 3])
                    with c_sel1:
                        st.markdown("**📉 副轴指标选择:**")
                    with c_sel2:
                        # 映射中文选项到 CSV 列名
                        metric_map = {"评论": "reply", "点赞": "like", "收藏": "fav", "硬币": "coin"}
                        target_label = st.radio(
                            "选择指标", 
                            options=list(metric_map.keys()), 
                            index=0, 
                            horizontal=True, 
                            label_visibility="collapsed"
                        )
                        target_col = metric_map[target_label]

                    # 计算动态增量
                    view_diff = int(last['view'] - first['view'])
                    target_diff = int(last[target_col] - first[target_col])
                    
                    # 动态指标卡 (m2 和 m4 会随选择变化)
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("播放增量", f"+{view_diff}")
                    m2.metric(f"{target_label}增量", f"+{target_diff}", delta_color="normal") # 动态显示选中的指标增量
                    m3.metric("当前播放", f"{int(last['view']):,}")
                    m4.metric(f"当前{target_label}", f"{int(last[target_col]):,}") # 动态显示选中的指标总量
                    
                    tab1, tab2 = st.tabs([f"增量趋势 (Plotly)", f"总量趋势 (Plotly)"])
                    
                    with tab1:
                        delta = df.copy()
                        # 计算差值
                        delta['delta_view'] = df['view'].diff().fillna(0)
                        delta[f'delta_{target_col}'] = df[target_col].diff().fillna(0)
                        
                        # 动态绘制：传入选中的列
                        fig_delta = draw_dual_axis_chart(
                            delta, "time_str", "delta_view", f"delta_{target_col}", 
                            "播放增量", f"{target_label}增量"
                        )
                        st.plotly_chart(fig_delta, use_container_width=True)
                        
                    with tab2: 
                        # 动态绘制：传入选中的列
                        fig_total = draw_dual_axis_chart(
                            df, "time_str", "view", target_col, 
                            "播放总量", f"{target_label}总量"
                        )
                        st.plotly_chart(fig_total, use_container_width=True)
            except Exception as e:
                st.error(f"数据读取错误: {str(e)}")