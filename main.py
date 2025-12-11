"""
Pornhub Downloader
核心流程：页面解析 → m3u8 提取 → 分片下载 → 合并 → 转码 → 输出文件
"""

import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from time import sleep
from urllib.parse import urljoin

import requests
from tqdm import tqdm

# =============================
# 全局配置
# =============================
TIMEOUT = 10            # HTTP请求超时
RETRY_MAX = 4           # 最大重试次数
MAX_WORKERS = 10        # 并发下载线程
DL_DIR = "downloads"    # 文件下载目录
FFMPEG_REL = os.path.join("ffmpeg", "bin", "ffmpeg.exe")


# =============================
# 通用工具函数
# =============================
def base_dir():
    """返回程序运行目录（兼容打包后的路径）"""
    return os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.abspath(
        os.path.dirname(__file__)
    )


def downloads_root():
    """返回并确保下载目录已创建"""
    root = os.path.join(base_dir(), DL_DIR)
    os.makedirs(root, exist_ok=True)
    return root


def normalize_viewkey(text: str):
    """将 viewkey 或 URL 规范化为可访问的视频地址"""
    if text.startswith("http"):
        return text
    if text.startswith("viewkey="):
        return f"https://cn.pornhub.com/view_video.php?{text}"
    return f"https://cn.pornhub.com/view_video.php?viewkey={text}"


def open_folder(path: str):
    """打开系统文件管理器并定位目标文件"""
    try:
        path = os.path.abspath(path)
        if os.name == "nt":
            subprocess.run(["explorer", "/select,", path])
        elif sys.platform == "darwin":
            subprocess.run(["open", os.path.dirname(path)])
        else:
            subprocess.run(["xdg-open", os.path.dirname(path)])
    except:
        pass


# =============================
# 下载器主类
# =============================
class Downloader:
    def __init__(self):
        self.root = downloads_root()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 Chrome/125"
        })
        self.temp_roots = set()
        self.lock = threading.Lock()
        self.cdn_url = ""

    # -------------------------
    # 页面解析 → 获取媒体源
    # -------------------------
    def get_video_sources(self, url: str):
        """解析页面并提取 mediaDefinitions JSON 数据"""
        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            html_text = resp.text
        except Exception:
            return None

        # 尝试匹配完整 JSON 区块
        pattern = re.compile(r'(?P<json>\{[^{}]*"mediaDefinitions"[^{}]*\})', re.S)
        match = pattern.search(html_text)
        if match:
            try:
                js = json.loads(match.group("json"))
                arr = js.get("mediaDefinitions") or []
                for i in arr:
                    i.setdefault("title", js.get("video_title", "unknown"))
                return arr
            except:
                pass

        # 备用方案：逐行查找
        for line in html_text.splitlines():
            if '"isVR"' in line and "mediaDefinitions" in line:
                try:
                    start = line.find("{")
                    j = json.loads(line[start:].replace("};", "}"))
                    return j.get("mediaDefinitions")
                except:
                    continue

        return None

    # -------------------------
    # m3u8 解析
    # -------------------------
    def parse_m3u8(self, text: str):
        """解析 m3u8 文本，过滤注释并转为完整 URL 列表"""
        res = []
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                res.append(line if line.startswith("http") else urljoin(self.cdn_url + "/", line))
        return res

    # -------------------------
    # 下载单个片段
    # -------------------------
    def download_one(self, url: str, fn: str):
        """下载单个 ts 分片（具备重试机制与临时文件写入）"""
        if os.path.exists(fn):
            return True

        tmp = fn + f".part.{os.getpid()}.{threading.get_ident()}.{int(time.time() * 1000)}"
        backoff = 1.0

        for _ in range(RETRY_MAX):
            try:
                with self.session.get(url, stream=True, timeout=TIMEOUT) as r:
                    r.raise_for_status()
                    with open(tmp, "wb") as w:
                        for c in r.iter_content(8192):
                            if c:
                                w.write(c)

                os.replace(tmp, fn)
                return True

            except:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except:
                        pass
                time.sleep(backoff)
                backoff *= 2

        return False

    # -------------------------
    # 多线程下载
    # -------------------------
    def download_all(self, urls: list, d: str):
        """并行下载所有分片"""
        os.makedirs(d, exist_ok=True)
        urls = list(dict.fromkeys(urls))
        total = len(urls)

        ok = 0
        with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as ex, \
                tqdm(total=total, unit="m3u8", desc="下载分片", ncols=80) as bar:

            futures = {ex.submit(self.download_one, u, os.path.join(d, f"{i}.ts")): i
                       for i, u in enumerate(urls)}

            for f in concurrent.futures.as_completed(futures):
                if f.result():
                    ok += 1
                bar.update(1)

        return ok == total

    # -------------------------
    # 合并 ts 分片
    # -------------------------
    def merge_ts(self, d: str, out: str):
        """按序号合并所有 ts 为一个完整 ts 文件"""
        files = sorted(
            [f for f in os.listdir(d) if f.endswith(".ts")],
            key=lambda x: int(re.findall(r"(\d+)\.ts$", x)[0])
        )

        if os.path.exists(out):
            return True

        with open(out, "ab") as w, tqdm(total=len(files), desc="合并片段", unit="m3u8", ncols=80) as bar:
            for f in files:
                try:
                    with open(os.path.join(d, f), "rb") as r:
                        shutil.copyfileobj(r, w)
                except:
                    pass
                bar.update(1)

        return True

    # -------------------------
    # ffmpeg 转码
    # -------------------------
    def ffmpeg_path(self):
        """定位 ffmpeg 可执行文件，兼容开发与打包环境"""
        # 相对路径
        relative_path = os.path.join("ffmpeg", "bin", "ffmpeg.exe")
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            temp_path = os.path.join(sys._MEIPASS, relative_path)
            if os.path.exists(temp_path):
                return temp_path

        # 程序主目录
        local_path = os.path.join(base_dir(), relative_path)
        if os.path.exists(local_path):
            return local_path

        # 系统环境
        path_ffmpeg = shutil.which("ffmpeg")
        if path_ffmpeg:
            return path_ffmpeg

        raise FileNotFoundError("未找到 ffmpeg")

    def convert(self, ts: str):
        """将合并 ts 转为 mp4（同编码拷贝）"""
        mp4 = ts.replace(".ts", ".mp4")
        if os.path.exists(mp4):
            return mp4

        cmd = [self.ffmpeg_path(), "-y", "-i", ts, "-c", "copy", mp4]
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if r.returncode == 0 and os.path.exists(mp4):
            try:
                os.remove(ts)
            except:
                pass
            return mp4

        return None

    # -------------------------
    # 单视频下载主流程
    # -------------------------
    def download_single(self, m3u8: str, vid: str):
        """执行 m3u8 下载 → 合并 → 转码 → 输出"""
        self.cdn_url = m3u8.rsplit("/", 1)[0]

        r = self.session.get(m3u8, timeout=TIMEOUT)
        m3u8_urls = self.parse_m3u8(r.text)
        if not m3u8_urls:
            return None

        temp = os.path.join(self.root, vid)
        ts_dir = os.path.join(temp, "m3u8")
        os.makedirs(ts_dir, exist_ok=True)

        with self.lock:
            self.temp_roots.add(temp)

        # 查找真实分片列表（主 m3u8 -> 子 m3u8）
        for variant in m3u8_urls:
            try:
                ts_text = self.session.get(variant, timeout=TIMEOUT).text
            except:
                continue

            ts_urls = self.parse_m3u8(ts_text)
            if not ts_urls:
                continue

            if not self.download_all(ts_urls, ts_dir):
                return None

        # 合并 ts
        merged = os.path.join(temp, f"{vid}.ts")
        if not self.merge_ts(ts_dir, merged):
            return None

        # 转 mp4
        mp4 = self.convert(merged)
        if not mp4:
            return None

        # 重名处理
        dest = os.path.join(self.root, os.path.basename(mp4))
        if os.path.exists(dest):
            b, e = os.path.splitext(dest)
            i = 1
            while os.path.exists(f"{b}_{i}{e}"):
                i += 1
            dest = f"{b}_{i}{e}"

        shutil.move(mp4, dest)

        # 清理临时目录
        shutil.rmtree(temp, ignore_errors=True)
        with self.lock:
            self.temp_roots.discard(temp)

        return dest

    # -------------------------
    # 统一入口
    # -------------------------
    def process(self, key_or_url: str):
        """执行完整下载流程（URL/viewkey → 视频输出）"""
        url = normalize_viewkey(key_or_url)
        arr = self.get_video_sources(url)
        if not arr:
            return None

        def safe_quality(x):
            """解析质量字段为可排序整数"""
            try:
                return int(x.get("quality"))
            except:
                return 0

        best = max(arr, key=safe_quality)
        vurl = best.get("videoUrl")
        if not vurl:
            return None

        vid = re.search(r"viewkey=([^&]+)", url)
        vid = vid.group(1) if vid else str(int(time.time()))

        return self.download_single(vurl, vid)


# =============================
# CLI
# =============================
def main():
    try:
        # 检查网站状态
        response = requests.head("https://cn.pornhub.com", timeout=5)
        if response.status_code != 200:
            print("请检查网络环境或VPN代理")
            return

        key = input("请输入完整 URL 或 viewkey: ").strip()
        if not key:
            print("无效输入")
            return
    except KeyboardInterrupt:
        return
    except requests.RequestException as e:
        print("请检查网络环境或VPN代理")
        return

    d = Downloader()
    start = time.time()

    try:
        out = d.process(key)
    except KeyboardInterrupt:
        return

    if out:
        elapsed = time.time() - start
        print(f"下载完成: {out}")
        print(f"任务耗时: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}")
        sleep(1)
        open_folder(out)


if __name__ == "__main__":
    main()
