
# -*- coding: utf-8 -*-
# 该模块提供一个最小可用的本地数仓（SQLite）实现，包含四层：
# - ODS（原始层）：保存抓取脚本原始输出，不做过滤；保留重复，方便追溯。
# - DWD（明细层）：写入清洗/去重/过滤后的数据，并用 valid 编码记录过滤原因。
# - DWS（汇总层）：按 周×来源×分类 聚合，服务周报统计。
# - ADS（应用层）：给每周做一个简单的二次精排和打分排序，供展示使用。
#
# 你只需要在 generate_report.py 中：
#   1) main() 开头调用 init_db()
#   2) 在 fetch_items_from_feed() 里每条 append 前调用 ods_insert_raw()
#   3) 在“文本过短/广告过滤/LLM ignore”分支里调用 dwd_upsert(..., valid=2/3/4)
#   4) 在最终入桶成功时，调用 dwd_upsert(..., valid=1)
#   5) main() 末尾调用 refresh_dws() / refresh_ads()
# 其它代码逻辑不变。

import sqlite3, hashlib, json, os
from pathlib import Path
from datetime import datetime

DB_PATH = Path(os.environ.get("DW_DB_PATH", "data/news_dw.sqlite"))

def _ensure_dir():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def _to_dtstr(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        return dt
    try:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)

def init_db():
    """创建表/索引/视图（存在则跳过）。"""
    _ensure_dir()
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        -- ODS：原始层（允许重复）
        CREATE TABLE IF NOT EXISTS ods_raw (
          rid         INTEGER PRIMARY KEY AUTOINCREMENT,
          source_id   TEXT,
          title       TEXT,
          link        TEXT,
          url_norm    TEXT,
          date        TEXT,
          text        TEXT,
          summary_raw TEXT,
          created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_ods_date ON ods_raw(date);
        CREATE INDEX IF NOT EXISTS idx_ods_url  ON ods_raw(url_norm);

        -- DWD：明细层（清洗+过滤结果；valid 说明状态/原因）
        CREATE TABLE IF NOT EXISTS dwd_news (
        uid           TEXT PRIMARY KEY,
        wid           TEXT,
        title         TEXT NOT NULL,
        summary       TEXT,
        text          TEXT,
        news_word1    TEXT,
        url           TEXT NOT NULL,
        source_id     TEXT,
        category      TEXT,
        region        TEXT,
        tags          TEXT,
        published_at  TEXT,
        valid         INTEGER NOT NULL,
        created_at    TEXT DEFAULT (datetime('now')),
        -- 新增字段：
        week_tag      TEXT,
        llm_confidence TEXT,   -- JSON 字符串
        llm_reason     TEXT,
        platform_type  INTEGER DEFAULT 0,  -- 0未知 / 1手游 / 2PC / 3主机
        game_type      TEXT   
        );
        CREATE INDEX IF NOT EXISTS idx_dwd_published ON dwd_news(published_at);
        CREATE INDEX IF NOT EXISTS idx_dwd_source    ON dwd_news(source_id);
        CREATE INDEX IF NOT EXISTS idx_dwd_category  ON dwd_news(category);
        CREATE INDEX IF NOT EXISTS idx_dwd_valid     ON dwd_news(valid);
        CREATE INDEX IF NOT EXISTS idx_dwd_week      ON dwd_news(week_tag);
        CREATE INDEX IF NOT EXISTS idx_dwd_platform  ON dwd_news(platform_type);


        -- DWS：汇总层（周×来源×分类 计数）
        CREATE TABLE IF NOT EXISTS dws_weekly (
          year_week  TEXT,
          source_id  TEXT,
          category   TEXT,
          cnt        INTEGER,
          PRIMARY KEY(year_week, source_id, category)
        );

        -- ADS：应用层（每周精排后的条目）
        CREATE TABLE IF NOT EXISTS ads_items (
          year_week    TEXT,
          uid          TEXT,
          title        TEXT,
          url          TEXT,
          summary      TEXT,
          category     TEXT,
          source_id    TEXT,
          published_at TEXT,
          score        REAL,
          rank         INTEGER,
          PRIMARY KEY(year_week, uid)
        );
        """)
    return DB_PATH

def ods_insert_raw(it: dict):
    """写入 ODS 原始层（不做去重/过滤）。"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO ods_raw(source_id, title, link, url_norm, date, text, summary_raw)
        VALUES(?,?,?,?,?,?,?)
        """, (
            it.get("source_id"),
            it.get("title"),
            it.get("link"),
            it.get("url_norm"),
            _to_dtstr(it.get("date")),
            it.get("text"),
            it.get("summary_raw")
        ))

def dwd_upsert(it: dict, valid: int = 1):
    """写入/更新 DWD 明细层。"""
    url = it.get("url_norm") or it.get("link") or ""
    uid = _sha1(url)
    wid = f"{(it.get('source_id') or '')}-{uid[:8]}"
    summary = it.get("summary") or it.get("summary_nodate") or ""
    tags = it.get("tags")
    if isinstance(tags, list):
        tags = json.dumps(tags, ensure_ascii=False)
    llm_conf = it.get("llm_confidence")
    if isinstance(llm_conf, (dict, list)):
        llm_conf = json.dumps(llm_conf, ensure_ascii=False)

    llm_reason = it.get("llm_reason") or None
    game_type = it.get("game_type") or ""




    week_tag = None
    dt = it.get("date")
    # 如果上游已算好 week_tag 就用 it['week_tag']；否则根据 date 算
    if it.get("week_tag"):
        week_tag = it["week_tag"]
    elif dt:
        try:
            # dt 可能是 datetime 或字符串
            if hasattr(dt, "strftime"):
                week_tag = dt.strftime("%Y-W%W")
            else:
                from dateutil import parser as _p
                week_tag = _p.parse(dt).strftime("%Y-W%W")
        except Exception:
            week_tag = None
    platform_type = int(it.get("platform_type") or 0)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO dwd_news(uid, wid, title, summary, text, news_word1, url, source_id,
                            category, region, tags, published_at, valid, week_tag,
                            llm_confidence, llm_reason, platform_type, game_type)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(uid) DO UPDATE SET
            title=excluded.title,
            summary=excluded.summary,
            text=excluded.text,
            news_word1=excluded.news_word1,
            url=excluded.url,
            source_id=excluded.source_id,
            category=excluded.category,
            region=excluded.region,
            tags=excluded.tags,
            published_at=excluded.published_at,
            valid=excluded.valid,
            week_tag=excluded.week_tag,
            llm_confidence=excluded.llm_confidence,
            llm_reason=excluded.llm_reason,
            platform_type=excluded.platform_type,
            game_type=excluded.game_type
            """, (
                uid, wid, it.get("title",""), summary, it.get("text",""), it.get("news_word1"),
                url, it.get("source_id"), it.get("category"), it.get("region"),
                tags or "[]", _to_dtstr(it.get("date")), int(valid), week_tag,
                llm_conf,                    # <== 用序列化后的 llm_conf
                llm_reason,
                platform_type,
                game_type
            ))



def refresh_dws(lookback_days: int = 60):
    """近 N 天涉及到的周重新聚合（幂等刷新）。"""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT DISTINCT strftime('%Y-W%W', published_at) AS yw
            FROM dwd_news
            WHERE published_at >= datetime('now', ?)
        """, (f'-{int(lookback_days)} day',)).fetchall()
        weeks = [r[0] for r in rows] or []
        if weeks:
            conn.execute("DELETE FROM dws_weekly WHERE year_week IN (%s)" %
                         ",".join("?"*len(weeks)), weeks)
        conn.execute("""
            INSERT INTO dws_weekly(year_week, source_id, category, cnt)
            SELECT
              strftime('%Y-W%W', published_at) AS year_week,
              source_id,
              COALESCE(NULLIF(LOWER(category),''),'unknown') AS category,
              COUNT(*) AS cnt
            FROM dwd_news
            WHERE valid=1 AND published_at IS NOT NULL
            GROUP BY 1,2,3
        """)

def refresh_ads(lookback_days: int = 30, per_week_cap: int = 200):
    """简单的周级二次精排（可按需调整公式）。"""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT DISTINCT strftime('%Y-W%W', published_at) AS yw
            FROM dwd_news
            WHERE valid=1 AND published_at >= datetime('now', ?)
        """, (f'-{int(lookback_days)} day',)).fetchall()
        weeks = [r[0] for r in rows] or []
        if weeks:
            conn.execute("DELETE FROM ads_items WHERE year_week IN (%s)" %
                         ",".join("?"*len(weeks)), weeks)

        conn.execute(f"""
        INSERT INTO ads_items(year_week, uid, title, url, summary, category, source_id, published_at, score, rank)
        WITH base AS (
            SELECT
              uid, title, url, summary,
              COALESCE(NULLIF(LOWER(category),''),'unknown') AS category,
              source_id, published_at,
              1.0 / (1.0 + (julianday('now') - julianday(published_at)) / 3.0) AS recency,
              CASE LOWER(category)
                WHEN 'market' THEN 0.30
                WHEN 'product' THEN 0.20
                WHEN 'news'    THEN 0.15
                WHEN 'method'  THEN 0.10
                ELSE 0.00
              END AS cat_bonus,
              MIN(LENGTH(COALESCE(summary,'')), 400) / 400.0 AS brevity
            FROM dwd_news
            WHERE valid=1 AND published_at >= datetime('now', ?)
        ),
        scored AS (
            SELECT
              strftime('%Y-W%W', published_at) AS year_week,
              uid, title, url, summary, category, source_id, published_at,
              (0.6*recency + cat_bonus + 0.2*brevity) AS score
            FROM base
        ),
        ranked AS (
            SELECT
              year_week, uid, title, url, summary, category, source_id, published_at, score,
              ROW_NUMBER() OVER (PARTITION BY year_week ORDER BY score DESC) AS rnk
            FROM scored
        )
        SELECT year_week, uid, title, url, summary, category, source_id, published_at, score, rnk
        FROM ranked
        WHERE rnk <= ?
        """, (f'-{int(lookback_days)} day', int(per_week_cap)))
