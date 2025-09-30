
# -*- coding: utf-8 -*-
import os, re, json
from ..config_loader import (USE_LLM, TEXT_MAXLEN, ENABLE_LLM_IGNORE, HARD_WEIGHT)
from ..config_loader import priors_for_source
from ..utils.cache_utils import cache_get, cache_set
from ..utils.text_utils import fallback_summarize
from .llm_client import get_llm_client_and_model

def _infer_platform_fallback(title, text):
    t = f"{title} {text}".lower()
    if any(k in t for k in ["ios","android","手游","mobile","taptap","app store","google play","测试服手游"]):
        return 1
    if any(k in t for k in ["steam","epic","pc 版","pc版","windows","macos","mac os","mac"]):
        return 2
    if any(k in t for k in ["switch","ns版","ns 版","ps5","ps4","playstation","xbox","主机版"]):
        return 3
    return 0

def analyze_article_llm(it, sources_cfg, client=None, model=None):
    title = it.get("title","")
    text  = it.get("text","")
    source_id = it.get("source_id") or it.get("source") or ""

    hit = cache_get(title, text, source_id)
    if hit is not None:
        return hit

    if not USE_LLM:
        t = (title + " " + text).lower()
        if any(k in t for k in ["政策","合规","规则","调整","发布","更新","报告","榜单","隐私","税","抽成","分成","dma","数据","趋势"]):
            cat = "news"
        elif any(k in t for k in ["玩法","版本","上线","新作","demo","评测","测评","分析","定位"]):
            cat = "product"
        else:
            cat = "method"
        if any(k in t for k in ["中国","国内","广州","上海","北京","字节","腾讯","米哈游","taptap"]):
            region = "cn"
        elif any(k in t for k in ["overseas","欧美","美国","欧洲","日本","韩国","全球","海外","google","apple","steam"]):
            region = "overseas"
        else:
            region = "none"
        result = {
            "category": cat,
            "region": region,
            "summary": fallback_summarize(text, 60, 90),
            "confidence": {"category":0.55, "region":0.5},
            "tags": []
        }
        cache_set(title, text, source_id, result)
        return result

    if client is None or model is None:
        client, model = get_llm_client_and_model()

    weight, expert = priors_for_source(sources_cfg, source_id)
    prior_note = ""
    if expert:
        prior_note = f"该来源更擅长方向：{','.join(expert)}。仅作为轻微先验，不要违背事实。"

    cls_line = "   - category: 'news'(要闻速览) | 'product'(产品分析) | 'market'(产品/市场数据) | 'method'(方法论学习) | 'ignore'(无关/招聘/广告/活动/声明)\n" if ENABLE_LLM_IGNORE else "   - category: 'news' | 'product' | 'market' | 'method'\n"

    prompt = (
        "请阅读以下文章，完成两个任务并只输出一个 JSON：\n"
        "1) 分类：\n"
        f"{cls_line}"
        "   - region: 'cn'(国内) | 'overseas'(海外) | 'none'(不适用/不确定)\n"
        "   - platform_type: 1=移动、2=PC、3=主机、0=未知\n"
        "2) 摘要：\n"
        "   - 输出一句中文行业资讯，≤200字；市场数据类需体现来源。\n"
        "3) 游戏类型（仅当 category='product' 且 platform_type=1 时输出）：\n"
        "   - game_type：如 SLG、卡牌 等；无法判断给空串。\n"
        "请严格输出 JSON。\n"
        f"标题：{title}\n"
        f"正文：{text[:TEXT_MAXLEN]}\n"
    )

    try:
        resp = client.chat.completions.create(
            model=model, temperature=0.2, max_tokens=300,
            messages=[
                {"role":"system","content":"你是严谨的行业研究助理，擅长结构化输出。"},
                {"role":"user","content":prompt},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        first, last = raw.find("{"), raw.rfind("}")
        if first >= 0 and last > first:
            raw = raw[first:last+1]
        data = json.loads(raw)
    except Exception as ex:
        print(f"[WARN] LLM analyze失败，兜底：{ex}")
        data = {
            "category":"method",
            "region":"none",
            "summary":fallback_summarize(text, 60, 90),
            "confidence":{"category":0.5,"region":0.5},
            "tags":[]
        }

    cat = (data.get("category") or "").lower()
    allowed = ("news","product","market","method","ignore") if ENABLE_LLM_IGNORE else ("news","product","market","method")
    if cat not in allowed:
        if "市场数据" in (data.get("tags") or []):
            cat = "market"
        else:
            cat = "method"
    reg = (data.get("region") or "").lower()
    if reg not in ("cn","overseas","none"):
        reg = "none"
    one = (data.get("summary") or "").strip()
    one = re.sub(r'^[“"\']+|[”"\']+$', "", one)

    p = data.get("platform_type", 0)
    try:
        platform_type = int(p)
        if platform_type not in (0,1,2,3):
             platform_type = 0
    except Exception:
        platform_type = 0

    if platform_type == 0:
        platform_type = _infer_platform_fallback(title, text)
    gt = (data.get("game_type") or "").strip() if (cat == "product" and platform_type == 1) else ""

    out = {
        "category": cat,
        "region": reg,
        "summary": one,
        "confidence": data.get("confidence") or {"category":0.6,"region":0.5},
        "tags": data.get("tags") or [],
        "reason": data.get("reason",""),
        "platform_type": platform_type,
        "game_type": gt,
    }
    cache_set(title, text, source_id, out)
    return out

def posterior_category_with_priors(llm_cat, confid, expert, weight=1.0):
    if expert and len(expert) == 1 and weight >= HARD_WEIGHT:
        e = expert[0]
        if "要闻" in e: return "news", {"news":1.0,"product":0.0,"method":0.0}
        if "产品" in e: return "product", {"news":0.0,"product":1.0,"method":0.0}
        if "方法论" in e or "方法" in e: return "method", {"news":0.0,"product":0.0,"method":1.0}

    base = {"news": 0.25, "product": 0.25, "market": 0.25, "method": 0.25}
    c = float(confid or 0.6)
    base[llm_cat] += 0.15 * max(0.0, min(c, 1.0))

    bias_unit = 0.12 * max(0.5, min(weight, 4.0))
    for e in (expert or []):
        if "要闻" in e: base["news"] += bias_unit
        elif "产品" in e: base["product"] += bias_unit
        elif "方法论" in e or "方法" in e or "运营/研发/投放方法论" in e: base["method"] += bias_unit
        elif "市场数据" in e: base["market"] += bias_unit

    s = sum(base.values()) or 1.0
    for k in base:
        base[k] /= s
    final_cat = max(base.items(), key=lambda x: x[1])[0]
    return final_cat, base
