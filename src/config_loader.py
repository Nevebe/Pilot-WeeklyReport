
# -*- coding: utf-8 -*-
"""
集中管理配置、路径、时间窗口与先验权重工具。
"""
import os, pathlib
from datetime import datetime, timedelta
from dateutil import tz
import yaml
from dotenv import load_dotenv

# 项目根目录（src 的上一层）
ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

# ---- 环境变量读取 ----
def getenv(name, default=None):
    v = os.environ.get(name)
    return v if (v is not None and v != "") else default

# ---- 基本配置 ----
TIMEZONE   = getenv("TIMEZONE", "Asia/Shanghai")
SITE_TITLE = getenv("SITE_TITLE", "行业周报")
MODEL      = getenv("MODEL", "deepseek-chat")

BASE_FEED  = getenv("BASE_FEED", getenv("BASE_RSS", "http://111.229.56.169:8001/feed"))
IDS_FILE   = getenv("IDS_FILE", "data/ids.txt")
DAYS_BACK  = int(getenv("DAYS_BACK", "7"))
USE_LLM    = getenv("USE_LLM", "true").lower() in ("1","true","yes")
TEXT_MAXLEN = int(getenv("TEXT_MAXLEN", "1600"))

# 缓存
CACHE_ENABLED   = getenv("CACHE_ENABLED", "false").lower() in ("1","true","yes")
CACHE_DIR       = ROOT / getenv("CACHE_DIR", ".cache")
CACHE_TTL_HOURS = int(getenv("CACHE_TTL_HOURS", "24"))

# 三道闸与阈值
ENABLE_NEAR_DUP_DROP = getenv("ENABLE_NEAR_DUP_DROP", "true").lower() in ("1","true","yes")
SIMHASH_HAMMING_THRESHOLD = int(getenv("SIMHASH_HAMMING_THRESHOLD", "4"))
ENABLE_AD_SCORE_FILTER = getenv("ENABLE_AD_SCORE_FILTER", "true").lower() in ("1","true","yes")
AD_SCORE_THRESHOLD = int(getenv("AD_SCORE_THRESHOLD", "5"))
ENABLE_LLM_IGNORE = getenv("ENABLE_LLM_IGNORE", "true").lower() in ("1","true","yes")
MIN_TEXT_LENGTH = int(getenv("MIN_TEXT_LENGTH", "200"))
HARD_WEIGHT = float(getenv("HARD_WEIGHT", "3.0"))

# 路径
DOCS      = ROOT / "docs"
TEMPLATES = ROOT / "src" / "templates"
CONFIG_DIR = ROOT / "config"

# 时间窗口
tzinfo = tz.gettz(TIMEZONE)
now    = datetime.now(tzinfo)
start  = (now - timedelta(days=DAYS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
end    = now

def load_sources_cfg():
    p = CONFIG_DIR / "sources.yml"
    if not p.exists():
        return {}
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def priors_for_source(cfg, source_id):
    w = 1.0
    expert = []
    if not cfg:
        return w, expert
    try:
        w = float(cfg.get("weights", {}).get(source_id, {}).get("weight", 1.0))
    except Exception:
        w = 1.0
    expert = cfg.get("weights", {}).get(source_id, {}).get("expertise", []) or []
    return w, expert
