
# -*- coding: utf-8 -*-
from datetime import datetime
from config_loader import now, start, end, TIMEZONE, SITE_TITLE
from pipeline.collector import collect_items
from pipeline.bucketizer import analyze_and_bucket
from pipeline.renderer import render_markdown, write_docs
from config_loader import load_sources_cfg
from warehouse import init_db, refresh_dws, refresh_ads

def main():
    init_db()
    print("[INFO] 开始抓取 md feed …")
    items = collect_items()
    print(f"[INFO] 抓取完成，候选 {len(items)} 条")

    print("[INFO] 读 sources.yml …")
    sources_cfg = load_sources_cfg()

    print("[INFO] 逐条分析（一次 LLM 完成分类+摘要） …")
    buckets = analyze_and_bucket(items, sources_cfg)

    year, week, _ = now.isocalendar()
    context = {
        "site_title": SITE_TITLE,
        "year": year,
        "week": week,
        "timezone": TIMEZONE,
        "window_start": start.strftime("%Y-%m-%d"),
        "window_end": end.strftime("%Y-%m-%d"),
        "generated_at": now.strftime("%Y-%m-%d %H:%M %Z"),

        "news_cn":       [{"line": it["summary"]} for it in buckets["news_cn"]],
        "news_overseas": [{"line": it["summary"]} for it in buckets["news_overseas"]],
        "market":  [{"line": it["summary_nodate"]} for it in buckets["market"]],
        "product_mobile": [
            {"line": it["summary_nodate"], "game_type": it.get("game_type", "").strip()}
            for it in buckets["product"] if it.get("platform_type") in (0, 1)
        ],
        "product_pc_console": [
            {"line": it["summary_nodate"], "game_type": it.get("game_type", "").strip()}
            for it in buckets["product"] if it.get("platform_type") in (2, 3)
        ],
        "method":        [{"line": it["summary_nodate"]} for it in buckets["method"]],
    }

    print("[INFO] 渲染 Markdown …")
    md = render_markdown(context)
    print("[INFO] 写入文件 …")
    write_docs(md, year, week)
    refresh_dws(lookback_days=60)
    refresh_ads(lookback_days=30, per_week_cap=2000)
    print(f"[SUCCESS] 生成完成：docs/{year}-W{week:02d}.md")

if __name__ == "__main__":
    main()
