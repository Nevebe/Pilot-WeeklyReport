
# -*- coding: utf-8 -*-
import re, requests
from datetime import datetime
from dateutil import parser as dtparser
from .text_utils import text_from_html
from ..config_loader import tzinfo

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "weekly-report-bot/1.0 (+local)"})

def http_get(url, max_retry=3, timeout=25):
    for _ in range(max_retry):
        try:
            resp = SESSION.get(url, timeout=timeout)
            if resp.status_code == 200 and resp.content:
                return resp.content
        except Exception:
            pass
    return None

def md_date(dt):
    if not dt:
        return ""
    try:
        return dt.strftime("%-m月%-d日")
    except Exception:
        return dt.strftime("%m月%d日")

WECHAT_HOST = "mp.weixin.qq.com"
def normalize_url(url):
    if not url:
        return ""
    url = url.strip()
    try:
        if WECHAT_HOST in url:
            m = re.search(r"(https?://mp\.weixin\.qq\.com)/s/([A-Za-z0-9_-]+)", url)
            if m:
                return f"{m.group(1)}/s/{m.group(2)}"
        url = re.sub(r"[?#].*$", "", url)
        return url
    except Exception:
        return url

def parse_date(s):
    try:
        return dtparser.parse(s).astimezone(tzinfo)
    except Exception:
        return None
