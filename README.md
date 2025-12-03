解压压缩包（必须解压，不可在压缩包内直接运行）。

确保文件夹内包含 Bili_Final.exe 以及 config.json BBdown.exe ffmpeg.exe（首次运行会生成和压缩包自带）。

双击 Bili_Final.exe，稍等片刻，浏览器将自动弹出控制台界面。

**获取 SESSDATA(必读)**

SESSDATA 是您的 B站“通行证”，不填或填错将无法获取任何数据。

获取步骤（以 Chrome/Edge 浏览器为例）：

在浏览器中打开 Bilibili 并登录账号。

按键盘 F12 键打开“开发者工具”。

在开发者工具顶部菜单栏点击 Application（应用）。

注：如果没看到，点菜单栏右侧的 >> 箭头。

在左侧栏展开 Cookies，点击 https://www.bilibili.com。

在右侧列表中找到 SESSDATA 一栏。

复制 Value（值）下方的那串字符（通常以数字或字母开头，很长）。

回到软件侧边栏，粘贴到 “账号凭证 (SESSDATA)” 输入框中，系统会自动保存。

**功能模块**

视频数据获取：输入目标 UID（支持多行），设置并发数，点击“开始抓取”。结束后可导出包含封面的csv文件，json文件在data文件夹里

封面提取：输入目标 UID，扫描所有视频封面，支持预览、全选及一键批量高清下载。

任务日志：侧边栏实时记录操作状态，方便回溯。

视频监测：可以自定义时间对单个视频实行流量数据监测，包含了哨兵删评监测机制。

视频下载：突破反爬后使用BBdown开源框架进行不限速下载视频、弹幕、音频，支持三种编码，使用ffmpeg开源软件合成视频。

<img width="3840" height="1947" alt="42a23b25246a222777014d504ed54512" src="https://github.com/user-attachments/assets/7bb9d65f-20ed-42d7-9ef8-d5aeac66613b" />
<img width="3840" height="1944" alt="f1f29f0713ed103ebf270ff4ad7606c6" src="https://github.com/user-attachments/assets/77a43271-896a-4716-95a5-8f7dde1ba7b0" />
<img width="3840" height="1944" alt="bd62cf34bcc10683fc602f2a63655880" src="https://github.com/user-attachments/assets/294d59a2-6d7f-470c-9b62-8436a8d7a96f" />
