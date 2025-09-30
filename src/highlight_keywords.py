# -*- coding: utf-8 -*-
"""
highlight_keywords.py —— 周报 Markdown 关键词加粗（后处理）
用法示例：
  python highlight_keywords.py docs/2025-W38.md --in-place
  python highlight_keywords.py docs/2025-W38.md --out docs/2025-W38.hl.md --topk 4

设计约束：
- 仅处理 Markdown 列表项（以 "- " 或 "* " 开头的行），避免破坏标题/代码块；
- 不修改链接与代码片段；不在 [原文](url) 内部加粗；
- 关键词必须为“原文已出现”的子串，减少幻觉。
"""

import os, re, sys, json, hashlib, pathlib, argparse
from typing import List, Tuple

# ==== 读取 .env，与主脚本一致 ====
from dotenv import load_dotenv
ROOT = pathlib.Path(__file__).resolve().parent
# 假设本文件放在项目根或 scripts/ 下，向上寻找 .env
for p in [ROOT, ROOT.parent, ROOT.parent.parent]:
    envp = p / ".env"
    if envp.exists():
        load_dotenv(envp)
        break

# ==== OpenAI/DeepSeek 统一客户端（与 generate_report.py 保持一致）====
from openai import OpenAI

def getenv(name, default=None):
    v = os.environ.get(name)
    return v if (v is not None and v != "") else default

def get_llm_client_and_model():
    provider = getenv("LLM_PROVIDER", "openai").lower()
    if provider == "deepseek":
        api_key  = getenv("DEEPSEEK_API_KEY") or getenv("OPENAI_API_KEY")
        base_url = getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        model    = getenv("MODEL", "deepseek-chat")
    else:
        api_key  = getenv("OPENAI_API_KEY")
        base_url = getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        model    = getenv("MODEL", "gpt-4o-mini")
    if not api_key:
        raise SystemExit("未设置 API Key（DEEPSEEK_API_KEY 或 OPENAI_API_KEY）")
    client = OpenAI(api_key=api_key, base_url=base_url)
    return client, model

# ==== 小工具 ====
CODE_FENCE_RE = re.compile(r"^```")
LIST_BULLET_RE = re.compile(r"^\s*[-*]\s+")
ORIGINAL_LINK_RE = re.compile(r"\[原文\]\([^)]+\)\s*$")
MD_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
BOLD_RE = re.compile(r"\*\*[^*]+\*\*")

def strip_original_link(line: str) -> Tuple[str, str]:
    """从行尾剥离 [原文](url)，返回 (正文, 尾链 或 '')"""
    m = ORIGINAL_LINK_RE.search(line)
    if not m:
        return line, ""
    head = line[:m.start()].rstrip()
    tail = line[m.start():].rstrip()
    return head, tail

def split_protected_spans(text: str):
    """
    将文本划分为“安全可替换段”和“受保护段”（链接/粗体/行内代码）。
    返回列表[(segment, is_protected_bool)]
    """
    spans = []
    i = 0
    # 统一把三类受保护段抽出来：链接、粗体、行内代码
    pattern = re.compile(r"(`[^`]+`|\[([^\]]+)\]\([^)]+\)|\*\*[^*]+\*\*)")
    for m in pattern.finditer(text):
        if m.start() > i:
            spans.append((text[i:m.start()], False))
        spans.append((m.group(0), True))
        i = m.end()
    if i < len(text):
        spans.append((text[i:], False))
    return spans

def safe_bold(text: str, phrases: List[str]) -> str:
    """仅在非受保护段中对短语加粗；优先长词，避免重复加粗。"""
    phrases = sorted(set([p.strip() for p in phrases if p and p.strip()]), key=len, reverse=True)
    if not phrases:
        return text
    spans = split_protected_spans(text)
    out = []
    for seg, prot in spans:
        if prot:
            out.append(seg)
        else:
            s = seg
            for ph in phrases:
                # 跳过已经加粗的
                if f"**{ph}**" in s:
                    continue
                # 用边界温和替换，尽量避免破词；中文可直接替换
                s = s.replace(ph, f"**{ph}**")
            out.append(s)
    return "".join(out)

# ==== 简单缓存（避免重复请求） ====
CACHE_DIR = pathlib.Path(getenv("HL_CACHE_DIR", ".cache/hl"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def cache_get(line: str):
    fn = CACHE_DIR / (_hash(line) + ".json")
    if fn.exists():
        try:
            return json.loads(fn.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def cache_set(line: str, data):
    fn = CACHE_DIR / (_hash(line) + ".json")
    try:
        fn.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

# ==== 调 LLM 取“文中已出现”的关键词 ====
PROMPT_TMPL = (
    "你是一名信息编辑，目标读者是手游公司员工。请从下面这条“行业资讯”中，挑选不超过 {topk} 个“最关键信息点”的中文短语，用于在周报里加粗。  \n"
    "必须严格满足：①短语为原文已出现的**连续子串**；②长度 2–10 字；③输出 **JSON 数组**（仅此，勿含多余文本）。\n\n"
    "【选择优先级（从高到低，按出现顺序取，去重）】\n"
    "1) 公司/机构/团队全称与核心人物（如：腾讯、米哈游、Savvy Games Group、贺甲、某局/法院等执法机构）。\n"
    "2) 产品/项目/游戏名（尤其《》内全称；若含系列/版本如“六周年发布会”，可连同关键修饰词一起取，≤10字）。\n"
    "3) 关键行为或事件结论（如：立项/上线/首测/发布会/登顶/封禁/起诉/胜诉/融资/并购/处罚/降息 等动宾短语）。\n"
    "4) 关键指标与排名（金额/百分比/名次/MAU/DAU/流水/ARR 等，保留单位与符号，如“71.5亿美元”“增长251%”“Top10”）。\n"
    "5) 与手游业务强相关的高频行业词（如：SLG、GVG、买量、版号、真金游戏、小游戏、出海、AIGC、AI智能体 等）。\n"
    "6) 时间节点仅作补位（如“09月18日”），当以上要点不足以达到 {topk} 时再选。\n\n"
    "【负例/排除】\n"
    "- 不要链接、@、表情、引号外解释；不要超过10字的长短语；不要虚词与空泛词（如“重要合作”“显著提升”）；不要重复近义词或同一信息的不同写法。\n"
    "- 若游戏名位于《》内，优先整段《……》；若包含数字，尽量连同单位与符号一起取（如“3亿美元”“Top1”“-25bp”）。\n\n"
    "请仅输出 JSON 数组，例如：[\"短语1\",\"短语2\"]。\n\n"
    "资讯：{sent}\n"
)

def extract_phrases_via_llm(client, model, sent: str, topk=3) -> List[str]:
    cached = cache_get(sent)
    if cached:
        return cached
    msg = PROMPT_TMPL.format(topk=topk, sent=sent)
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.2,
            max_tokens=120,
            messages=[
                {"role":"system","content":"你是严谨的中文编辑助手，所有短语必须为输入文本的原文子串。"},
                {"role":"user","content":msg},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        # 仅保留第一个 JSON 段
        first, last = raw.find("["), raw.rfind("]")
        if first >= 0 and last > first:
            raw = raw[first:last+1]
        arr = json.loads(raw)
        arr = [str(x).strip() for x in arr if isinstance(x, (str,))]
    except Exception:
        arr = []
    cache_set(sent, arr)
    return arr

# ==== 主处理 ====
def process_markdown(md: str, client, model, topk=3) -> str:
    lines = md.splitlines()
    out_lines = []
    in_codeblock = False

    for line in lines:
        # 代码块开关
        if CODE_FENCE_RE.match(line.strip()):
            in_codeblock = not in_codeblock
            out_lines.append(line)
            continue

        if in_codeblock or not LIST_BULLET_RE.match(line):
            out_lines.append(line)
            continue

        # 剥离行尾 [原文](url)
        body, tail = strip_original_link(line)

        # 仅对 bullet 的正文部分做 LLM 关键词提取
        # 去掉日期引导（如“09月12日，”），保证挑选更聚焦
        body4llm = re.sub(r"^\s*[-*]\s+\d{2}月\d{2}日，?", "", body)

        phrases = extract_phrases_via_llm(client, model, body4llm, topk=topk)
        new_body = safe_bold(body, phrases)

        new_line = (new_body + (" " + tail if tail else "")).rstrip()
        out_lines.append(new_line)

    return "\n".join(out_lines) + ("\n" if md.endswith("\n") else "")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("md_path", help="输入周报 Markdown 文件路径（如 docs/2025-W38.md）")
    ap.add_argument("--topk", type=int, default=3, help="每条最多加粗的短语数")
    ap.add_argument("--in-place", action="store_true", help="原地覆写")
    ap.add_argument("--out", default="", help="输出到指定文件（不与 --in-place 同用）")
    ap.add_argument("--dry-run", action="store_true", help="仅打印到控制台，不写回文件")
    args = ap.parse_args()

    client, model = get_llm_client_and_model()

    p = pathlib.Path(args.md_path)
    if not p.exists():
        sys.exit(f"文件不存在：{p}")

    md = p.read_text(encoding="utf-8")
    new_md = process_markdown(md, client, model, topk=args.topk)

    if args.dry_run:
        print(new_md)
        return

    if args.in_place:
        p.write_text(new_md, encoding="utf-8")
        print(f"[OK] 已原地加粗：{p}")
    else:
        outp = pathlib.Path(args.out) if args.out else p.with_suffix(".hl.md")
        outp.write_text(new_md, encoding="utf-8")
        print(f"[OK] 已输出到：{outp}")

if __name__ == "__main__":
    main()
