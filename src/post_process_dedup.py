
# -*- coding: utf-8 -*-
"""
post_process_dedup.py —— 周报 Markdown 二次清洗脚本（仅处理 MD，全部参数由 .env 配置）

功能：
- 从 Markdown 周报读取条目（形如 "- 09月18日，……。[原文](url)" 的列表行）。
- 通过 Shingle+Jaccard 与 SimHash 召回“疑似重复”，可选用 LLM 做二次语义判定。
- 导出去重后的 Markdown 与一份 CSV 审计（列出被合并项及合并理由）。

使用：
1) 在 .env 中配置（示例见下）。
2) 直接运行：  python post_process_dedup.py

.env 示例：
----------------
# 输入与输出
MD_PATH=/mnt/data/2025-W39.md
OUT_DIR=/mnt/data

# 去重策略（可选值 earliest|latest|longest）
KEEP_POLICY=earliest

# 近似召回阈值
JACCARD_TH=0.62
SIMHASH_HAM_TH=8

# LLM 开关与模型
LLM=false
MODEL=deepseek-chat
OPENAI_BASE_URL=https://api.deepseek.com
DEEPSEEK_API_KEY=你的API_KEY  # 或 OPENAI_API_KEY
----------------

说明：
- 所有参数通过 .env 读取，未配置时使用脚本内的默认值。
- 若 LLM=true，需在 .env 中提供 MODEL 与 API Key（DEEPSEEK_API_KEY 或 OPENAI_API_KEY）。
"""

import os, re, csv, json, hashlib
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Tuple, Dict
from urllib.parse import urlparse

from dotenv import load_dotenv
try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # 启用 LLM 时才需要

# ============== 文本与相似度工具 ==============
PUNCS = r"\s~!@#$%^&*()_+\-=\[\]{}|;:'\",.<>/?，。！、；：‘’“”…（）【】—-"

def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\（.*?\）|\(.*?\)", " ", s)  # 去括号尾巴
    s = s.lower()
    s = re.sub(fr"[{PUNCS}]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def shingles(s: str, k: int = 8) -> set:
    if not s: return set()
    s = s.replace(" ", "")
    if len(s) <= k: return {s}
    return {s[i:i+k] for i in range(len(s)-k+1)}

def jaccard(a: set, b: set) -> float:
    if not a or not b: return 0.0
    inter = len(a & b)
    if inter == 0: return 0.0
    return inter / float(len(a | b))

def simhash64(s: str) -> int:
    if not s: return 0
    toks = [s[i:i+2] for i in range(max(len(s)-1,1))]
    from collections import Counter
    cnt = Counter(toks)
    v = [0]*64
    for tok, w in cnt.items():
        import hashlib as _hashlib
        h = int(_hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        for i in range(64):
            v[i] += w if (h >> i) & 1 else -w
    out = 0
    for i in range(64):
        if v[i] > 0: out |= (1 << i)
    return out

def hamdist64(a: int, b: int) -> int:
    return (a ^ b).bit_count()

# ============== 数据结构 ==============
@dataclass
class Item:
    id: str
    title: str
    text: str
    url: str
    date: str
    source_id: str
    raw: str
    norm: str = ""
    sh: int = 0
    shing: set = None

    def prepare(self):
        self.norm = normalize_text(f"{self.title} {self.text}")
        self.sh = simhash64(self.norm)
        self.shing = shingles(self.norm, 8)

# ============== 读取 Markdown ==============
def parse_md_items(md_path: str) -> List[Item]:
    items = []
    pat = re.compile(r"^-+\s*(.*?)(?:\[原文\]\((https?://[^\s)]+)\))?\s*$")
    sid = Path(md_path).stem
    with open(md_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("- "):
                continue
            m = pat.match(line)
            if not m:
                body = line[2:].strip()
                url = ""
            else:
                body, url = m.group(1).strip(), (m.group(2) or "").strip()
            parts = re.split(r"[，,：:——-]\s*", body, maxsplit=1)
            title = parts[0].strip()
            text = parts[1].strip() if len(parts) > 1 else ""
            dm = re.search(r"(\d{1,2})月(\d{1,2})日", body)
            date = dm.group(0) if dm else ""
            it = Item(
                id=hashlib.sha1((url or body).encode("utf-8")).hexdigest()[:12],
                title=title, text=text, url=url, date=date, source_id=sid, raw=line
            )
            it.prepare()
            items.append(it)
    return items

# ============== 候选召回 ==============
def candidate_pairs(items: List[Item], jaccard_th: float, ham_th: int) -> List[Tuple[int,int]]:
    pairs = []
    buckets: Dict[str, List[int]] = defaultdict(list)
    for i, it in enumerate(items):
        host = urlparse(it.url).netloc if it.url else ""
        buckets[host].append(i)

    def add_pairs(indices):
        m = len(indices)
        for a in range(m):
            ia = indices[a]; A = items[ia]
            for b in range(a+1, m):
                ib = indices[b]; B = items[ib]
                if abs(len(A.norm) - len(B.norm)) > 200:
                    continue
                j = jaccard(A.shing, B.shing)
                if j < jaccard_th:
                    continue
                h = hamdist64(A.sh, B.sh)
                if h > ham_th:
                    continue
                pairs.append((ia, ib))

    # 同域优先；样本不大时再加一轮全量兜底
    for host, idxs in buckets.items():
        if len(idxs) > 1:
            add_pairs(idxs)
    if len(items) <= 400:
        add_pairs(list(range(len(items))))
    return pairs

# ============== LLM 语义确认（可选） ==============
PROMPT_TMPL = """你是行业资讯去重助手。判断两条中文资讯是否“表达的是同一实质事件/事实”，
不是看文字是否完全相同，而是看语义是否等价（同一主体+同一事件+数据/结论近似）。
输出 JSON：{"duplicate": true/false, "reason": "简要说明"}。

A: {a}
B: {b}
"""

def llm_confirm(client, model: str, a: Item, b: Item):
    text_a = (a.title + "。" + a.text)[:500]
    text_b = (b.title + "。" + b.text)[:500]
    prompt = PROMPT_TMPL.format(a=text_a, b=text_b)
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.0,
            max_tokens=120,
            messages=[
                {"role":"system", "content": "你是严谨的行业资讯去重助手，只输出指定 JSON。"},
                {"role":"user", "content": prompt}
            ]
        )
        msg = (resp.choices[0].message.content or "").strip()
        m1, m2 = msg.find("{"), msg.rfind("}")
        if m1 >= 0 and m2 > m1:
            msg = msg[m1:m2+1]
        data = json.loads(msg)
        return bool(data.get("duplicate")), str(data.get("reason",""))
    except Exception as e:
        return False, f"LLM 调用失败：{e}"

# ============== 主流程（全部由 .env 控制） ==============
def main():
    load_dotenv()

    # 读取环境变量（默认值在右侧）
    md_path   = os.getenv("MD_PATH", "").strip()
    out_dir   = os.getenv("OUT_DIR", "").strip()
    keep_pol  = os.getenv("KEEP_POLICY", "earliest").strip().lower()
    j_th      = float(os.getenv("JACCARD_TH", "0.62"))
    ham_th    = int(os.getenv("SIMHASH_HAM_TH", "8"))
    use_llm   = os.getenv("LLM", "false").strip().lower() == "true"
    model     = os.getenv("MODEL", "").strip()
    max_pairs = int(os.getenv("MAX_PAIRS", "200"))

    if not md_path:
        raise SystemExit("请在 .env 中设置 MD_PATH（要处理的 Markdown 文件路径）。")
    if not Path(md_path).exists():
        raise SystemExit(f"未找到 MD 文件：{md_path}")

    items = parse_md_items(md_path)
    if not items:
        raise SystemExit("未读取到任何条目。")

    pairs = candidate_pairs(items, j_th, ham_th)

    client = None
    if use_llm:
        if OpenAI is None:
            raise SystemExit("需要 openai 包。请先安装：pip install openai")
        api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        if not api_key:
            raise SystemExit("LLM=true 但未提供 API Key（DEEPSEEK_API_KEY 或 OPENAI_API_KEY）。")
        if not model:
            model = "deepseek-chat"
        client = OpenAI(api_key=api_key, base_url=base_url)

    # 确认重复边
    dup_edges = []  # (i,j, reason)
    confirmed = 0
    for idx, (ia, ib) in enumerate(pairs):
        A, B = items[ia], items[ib]
        # 强规则：同域+高相似
        if urlparse(A.url).netloc == urlparse(B.url).netloc and jaccard(A.shing, B.shing) >= 0.8:
            dup_edges.append((ia, ib, "同域高相似"))
            continue
        if use_llm and idx < max_pairs:
            is_dup, reason = llm_confirm(client, model, A, B)
            if is_dup:
                dup_edges.append((ia, ib, reason or "LLM 判定重复"))
                confirmed += 1

    # 并查集合并为簇
    parent = list(range(len(items)))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a,b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[rb] = ra

    for ia, ib, _ in dup_edges:
        union(ia, ib)

    clusters: Dict[int, List[int]] = defaultdict(list)
    for i in range(len(items)):
        clusters[find(i)].append(i)

    # 选择保留项
    keep = set()
    dropped_rows = []
    for rid, idxs in clusters.items():
        if len(idxs) == 1:
            keep.add(idxs[0]); continue
        grp = [items[i] for i in idxs]
        if keep_pol == "latest":
            grp.sort(key=lambda x: x.date or "", reverse=True)
        elif keep_pol == "longest":
            grp.sort(key=lambda x: len(x.norm), reverse=True)
        else:
            grp.sort(key=lambda x: x.date or "")
        keep_idx = items.index(grp[0])
        keep.add(keep_idx)
        for i in idxs:
            if i == keep_idx:
                continue
            reason = ""
            for ia, ib, r in dup_edges:
                if (ia == keep_idx and ib == i) or (ib == keep_idx and ia == i) or (ia == i and ib == keep_idx):
                    reason = r; break
            dropped_rows.append({
                "kept_id": items[keep_idx].id,
                "kept_title": items[keep_idx].title,
                "dropped_id": items[i].id,
                "dropped_title": items[i].title,
                "reason": reason or "同簇合并"
            })

    # 输出
    out_dir_path = Path(out_dir) if out_dir else Path(md_path).parent
    out_dir_path.mkdir(parents=True, exist_ok=True)

    stem = Path(md_path).stem
    md_out = out_dir_path / f"{stem}.dedup.md"
    csv_out = out_dir_path / f"{stem}.dedup.audit.csv"

    with open(md_out, "w", encoding="utf-8") as fw:
        for i, it in enumerate(items):
            if i in keep:
                fw.write(it.raw + "\n")

    with open(csv_out, "w", encoding="utf-8", newline="") as fw:
        writer = csv.DictWriter(fw, fieldnames=["kept_id","kept_title","dropped_id","dropped_title","reason"])
        writer.writeheader()
        writer.writerows(dropped_rows)

    print(f"[OK] 保留 {len(keep)} / {len(items)} 条。")
    print(f"[OUT] Markdown: {md_out}")
    print(f"[OUT] 审计表:   {csv_out}")
    if use_llm:
        print(f"[LLM] 候选对 {len(pairs)}，确认 {confirmed} 对。")

if __name__ == "__main__":
    main()
