
# -*- coding: utf-8 -*-
import os, re, feedparser
from datetime import datetime
from ..config_loader import (BASE_FEED, start, end, tzinfo, MIN_TEXT_LENGTH)
from ..config_loader import load_sources_cfg
from ..utils.http_utils import http_get, normalize_url, parse_date
from ..utils.text_utils import text_from_html, sanitize_for_llm, plain_text_len
from ..utils.simhash_utils import drop_near_duplicates_within_source, drop_near_duplicates_across_sources
from ..config_loader import SIMHASH_HAMMING_THRESHOLD, ENABLE_NEAR_DUP_DROP
from ..config_loader import ROOT
from ..config_loader import getenv
from ..config_loader import CONFIG_DIR
from ..config_loader import tzinfo
from ..utils.http_utils import md_date
from ..config_loader import now
from ..config_loader import ENABLE_LLM_IGNORE
from ..config_loader import ENABLE_AD_SCORE_FILTER, AD_SCORE_THRESHOLD
from ..pipeline.filter_rules import ad_score
from ..llm.analyze_article import analyze_article_llm, posterior_category_with_priors
from ..config_loader import load_sources_cfg, TIMEZONE
from ..utils.text_utils import hide_links
from ..config_loader import DOCS
from ..config_loader import TEMPLATES
from ..config_loader import USE_LLM
from ..llm.llm_client import get_llm_client_and_model
from ..config_loader import priors_for_source

from warehouse import ods_insert_raw, dwd_upsert

def load_ids():
    cfg = load_sources_cfg() or {}
    weights = cfg.get("weights", {}) if isinstance(cfg, dict) else {}
    if not weights:
        raise SystemExit("sources.yml 中没有找到 weights 或为空，请确认已生成正确的 sources.yml")
    ids_from_sources = list(weights.keys())
    print(f"[INFO] 从 sources.yml 读取到 {len(ids_from_sources)} 个源")
    return ids_from_sources

def fetch_items_from_feed(source_id):
    url = f"{BASE_FEED}/{source_id}.md"
    print(f"[DEBUG] 抓取: {url}")
    raw = http_get(url)
    if not raw:
        return []
    fp = feedparser.parse(raw)
    items = []
    for e in fp.entries:
        title = (e.get("title") or "").strip()
        link = ""
        if "link" in e and e["link"]:
            link = e["link"]
        elif e.get("links"):
            for l in e["links"]:
                if l.get("href"):
                    link = l["href"]; break

        dt = None
        for k in ("updated", "published", "created"):
            if e.get(k):
                dt = parse_date(e.get(k)); 
                if dt: break

        title_text = title
        summary_text = ""
        if e.get("summary"):
            summary_text = text_from_html(e["summary"])
        content_text = ""
        if e.get("content"):
            try:
                for c in e["content"]:
                    if c.get("value"):
                        content_text = text_from_html(c["value"])
                        content_text = re.sub(r'(https?://\S+|www\.\S+)', '', content_text)
                        content_text = sanitize_for_llm(content_text)
                        break
            except Exception:
                pass

        txt = f"title:{title_text}|summary:{summary_text}|content:{content_text}"
        if not (title_text or summary_text or content_text):
            txt = title

        ods_insert_raw({
            "source_id": source_id,
            "title": title,
            "link": link,
            "url_norm": normalize_url(link),
            "date": dt,
            "text": txt,
            "summary_raw": summary_text
        })

        items.append({
            "source_id": source_id,
            "title": title,
            "link": link,
            "date": dt,
            "text": txt,
            "summary_raw": summary_text
        })
    return items

def collect_items():
    ids = load_ids()
    all_items = []
    for sid in ids:
        all_items.extend(fetch_items_from_feed(sid))

    inwin = []
    for it in all_items:
        dt = it.get("date")
        if not dt:
            continue
        if start <= dt <= end:
            inwin.append(it)

    seen = set(); uniq = []
    for it in inwin:
        u = normalize_url(it.get("link"))
        it["url_norm"] = u
        if not u:
            continue
        if u in seen: 
            continue
        seen.add(u); uniq.append(it)

    filtered = []
    short_drops = 0
    for it in uniq:
        plen = plain_text_len(it.get("text",""))
        if plen < MIN_TEXT_LENGTH:
            print(f"[FILTER] 文本过短 → 丢弃：{it.get('title','')[:30]}... (len={plen}, min={MIN_TEXT_LENGTH})")
            short_drops += 1
            dwd_upsert(it, valid=2)
            continue
        filtered.append(it)

    print(f"[STATS] 文本过短丢弃 {short_drops} 篇（阈值={MIN_TEXT_LENGTH}），保留 {len(filtered)} 篇")
    return filtered
