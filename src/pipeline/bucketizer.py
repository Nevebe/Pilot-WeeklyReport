
# -*- coding: utf-8 -*-
import os
from datetime import datetime
from ..config_loader import tzinfo, ENABLE_LLM_IGNORE, ENABLE_AD_SCORE_FILTER, AD_SCORE_THRESHOLD
from ..config_loader import SIMHASH_HAMMING_THRESHOLD, ENABLE_NEAR_DUP_DROP
from ..utils.http_utils import md_date
from ..utils.simhash_utils import drop_near_duplicates_within_source, drop_near_duplicates_across_sources
from ..pipeline.filter_rules import ad_score
from ..llm.analyze_article import analyze_article_llm, posterior_category_with_priors
from ..config_loader import priors_for_source
from ..utils.text_utils import hide_links
from warehouse import dwd_upsert

def analyze_and_bucket(items, sources_cfg):
    buckets = {"news_cn": [], "news_overseas": [], "product": [], "method": [], "market": []}
    ad_drops = 0; ignore_drops = 0

    if not items:
        return buckets

    if ENABLE_NEAR_DUP_DROP:
        items = drop_near_duplicates_within_source(
            items,
            hamming_threshold=SIMHASH_HAMMING_THRESHOLD,
            keep_policy=os.environ.get("NEAR_DUP_KEEP_POLICY", "earliest").lower()
        )
    if os.environ.get("ENABLE_CROSS_SOURCE_DUP_DROP", "true").lower() in ("1","true","yes"):
        items = drop_near_duplicates_across_sources(
            items, sources_cfg,
            hamming_threshold=int(os.environ.get("CROSS_SIMHASH_HAMMING_THRESHOLD", str(SIMHASH_HAMMING_THRESHOLD))),
            keep_policy=os.environ.get("CROSS_KEEP_POLICY","prefer_weight_then_earliest").lower()
        )

    client, model = None, None
    from ..llm.llm_client import get_llm_client_and_model
    from ..config_loader import USE_LLM
    if USE_LLM:
        client, model = get_llm_client_and_model()

    for it in items:
        if ENABLE_AD_SCORE_FILTER:
            _score = ad_score(it.get("title",""), it.get("text",""))
            if _score >= AD_SCORE_THRESHOLD:
                print(f"[FILTER] 广告分数过高 → 丢弃：{it.get('title','')[:30]}... (score={_score}, threshold={AD_SCORE_THRESHOLD})")
                ad_drops += 1
                dwd_upsert(it, valid=3)
                continue

        sid = it.get("source_id") or ""
        res = analyze_article_llm(it, sources_cfg, client, model)

        if ENABLE_LLM_IGNORE and res.get("category") == "ignore":
            print(f"[FILTER] LLM 守门员判定为 ignore → 丢弃：{it.get('title','')[:30]}...")
            ignore_drops += 1
            dwd_upsert(it, valid=4)
            continue

        weight, expert = priors_for_source(sources_cfg, sid)
        final_cat, _ = posterior_category_with_priors(
            res["category"],
            (res.get("confidence") or {}).get("category", 0.6),
            expert,
            weight,
        )
        region = res.get("region","none")

        link = it.get("url_norm") or it.get("link") or ""
        d = md_date(it.get("date"))
        one = (res.get("summary") or "").strip()

        if final_cat == "news" and d:
            line = f"{d}，{one} {link}".strip()
        else:
            line = f"{one} {link}".strip()

        line = hide_links(line)

        it["summary"] = line
        it["summary_nodate"] = hide_links(f"{one} {link}".strip())
        final_cat = final_cat.lower()

        tags = set(res.get("tags") or [])
        if final_cat in ("product", "method") and ({"市场数据", "market"} & tags):
            final_cat = "market"

        it["final_cat"] = final_cat
        it["category"] = final_cat
        it["region"] = region
        it["tags"] = list(tags)
        it["llm_confidence"] = res.get("confidence")
        it["llm_reason"]     = res.get("reason")
        dt = it.get("date")
        it["week_tag"] = dt.strftime("%Y-W%W") if dt else None
        it["platform_type"] = res.get("platform_type", 0)
        if final_cat == "product" and it["platform_type"] == 1:
            it["game_type"] = res.get("game_type") or ""
        else:
            it["game_type"] = ""

        dwd_upsert(it, valid=1)

        if final_cat == "news":
            if region == "cn":
                buckets["news_cn"].append(it)
            else:
                buckets["news_overseas"].append(it)
        elif final_cat == "market":
            buckets["market"].append(it)
        elif final_cat == "product":
            buckets["product"].append(it)
        else:
            buckets["method"].append(it)

    for key in ("news_cn","news_overseas"):
        buckets[key].sort(key=lambda x: x.get("date") or datetime(1970,1,1, tzinfo=tzinfo), reverse=True)

    total_kept = sum(len(v) for v in buckets.values())
    print(f"[STATS] 过滤统计：广告分数丢弃 {ad_drops} 篇；LLM ignore 丢弃 {ignore_drops} 篇；最终入桶 {total_kept} 篇")
    print("[STATS] 各桶数量：",
      f"news_cn={len(buckets['news_cn'])},",
      f"news_overseas={len(buckets['news_overseas'])},",
      f"product={len(buckets['product'])},",
      f"method={len(buckets['method'])}")
    return buckets
