
# -*- coding: utf-8 -*-
import re, hashlib
from datetime import datetime
from ..config_loader import tzinfo, priors_for_source
def norm_text_for_hash(title: str, text: str) -> str:
    s = f"{title or ''} {text or ''}"
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace("％", "%").replace("，", ",").replace("。", ".")
    s = re.sub(r"[\u3000\t\r\n]+", " ", s)
    s = re.sub(r"[~!@#$^&*()_+\-=\[\]{}|;:'\",.<>/?，。！、；：‘’“”…（）【】—-]", " ", s)
    return s

def simhash64(s: str) -> int:
    if not s: return 0
    tokens = []
    s = s.strip()
    for i in range(len(s) - 1):
        tokens.append(s[i:i+2])
    if not tokens:
        tokens = [s]
    from collections import Counter
    cnt = Counter(tokens)
    v = [0] * 64
    for tok, w in cnt.items():
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        for i in range(64):
            bit = (h >> i) & 1
            v[i] += w if bit else -w
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= (1 << i)
    return out

def hamming64(a: int, b: int) -> int:
    return (a ^ b).bit_count()

def drop_near_duplicates_within_source(items, hamming_threshold=4, keep_policy="earliest"):
    from collections import defaultdict
    enriched = []
    for it in items:
        sid = it.get("source_id") or ""
        dt  = it.get("date")
        txt = it.get("summary_raw", "") or ""
        nt  = norm_text_for_hash(it.get("title",""), txt)
        h   = simhash64(nt)
        enriched.append((sid, h, dt, len(txt), it))

    by_src = defaultdict(list)
    for tup in enriched:
        by_src[tup[0]].append(tup)

    kept_ids = set(); dropped = 0

    for sid, arr in by_src.items():
        if len(arr) <= 1:
            kept_ids.add(id(arr[0][-1]))
            continue
        if keep_policy == "latest":
            arr.sort(key=lambda x: (x[2] or datetime.min.replace(tzinfo=tzinfo)), reverse=True)
        elif keep_policy == "longest":
            arr.sort(key=lambda x: x[3], reverse=True)
        else:
            arr.sort(key=lambda x: (x[2] or datetime.max.replace(tzinfo=tzinfo)))
        used_hashes = []
        for sid_i, hi, dti, li, iti in arr:
            if any(hamming64(hi, h_used) <= hamming_threshold for h_used in used_hashes):
                dropped += 1
                continue
            kept_ids.add(id(iti))
            used_hashes.append(hi)
    if dropped:
        print(f"[FILTER] 同源近似去重 → 丢弃 {dropped} 篇 (策略={keep_policy})")
    return [it for it in items if id(it) in kept_ids]

def drop_near_duplicates_across_sources(items, sources_cfg,
                                        hamming_threshold=4,
                                        keep_policy="prefer_weight_then_earliest"):
    enriched = []
    for it in items:
        base_txt = it.get("summary_raw") or it.get("text") or ""
        nt = norm_text_for_hash(it.get("title",""), base_txt)
        h = simhash64(nt)
        dt = it.get("date")
        plen = len(base_txt)
        sid = it.get("source_id") or ""
        w, _expert = priors_for_source(sources_cfg, sid)
        enriched.append({"sid": sid, "h": h, "date": dt, "plen": plen, "weight": float(w or 1.0), "it": it})

    if keep_policy == "latest":
        enriched.sort(key=lambda x: (x["date"] or datetime.min.replace(tzinfo=tzinfo)), reverse=True)
    elif keep_policy == "longest":
        enriched.sort(key=lambda x: x["plen"], reverse=True)
    elif keep_policy == "earliest":
        enriched.sort(key=lambda x: (x["date"] or datetime.max.replace(tzinfo=tzinfo)))
    else:
        enriched.sort(key=lambda x: (-x["weight"], x["date"] or datetime.max.replace(tzinfo=tzinfo)))

    kept = []; used_hashes = []; dropped = 0
    for row in enriched:
        h = row["h"]
        if any(hamming64(h, uh) <= hamming_threshold for uh in used_hashes):
            dropped += 1
            continue
        kept.append(row["it"]); used_hashes.append(h)
    if dropped:
        print(f"[FILTER] 跨源近似去重 → 丢弃 {dropped} 篇 (策略={keep_policy})")
    return kept
