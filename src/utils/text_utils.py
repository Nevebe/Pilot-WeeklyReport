
# -*- coding: utf-8 -*-
import re

def text_from_html(s):
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def fallback_summarize(text: str, minlen=60, maxlen=90):
    t = (text or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t)
    if len(t) <= maxlen:
        return t
    end = 0
    for m in re.finditer(r"[。！!？?；;]", t[:maxlen+20]):
        end = m.end()
    if end >= minlen:
        return t[:end]
    return t[:maxlen]

def plain_text_len(s: str) -> int:
    if not s:
        return 0
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return len(s.strip())

def sanitize_for_llm(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r'!\[[^\]]*\]\([^)]+\)', '', s)
    s = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', r'\1', s)
    s = re.sub(r'(https?://\S+|www\.\S+)', '', s)
    s = re.sub(r'^\s*(图片|image|gif)?\s*$', '', s, flags=re.I | re.M)
    s = re.sub(r'^\s*!\[[^\]]*\]\([^)]+\)\s*$', '', s, flags=re.M)
    s = re.sub(r'^\s*\[\s*[^\]]*\s*\]\(\s*\)\s*$', '', s, flags=re.M)
    s = re.sub(r'(?:^|\n)\s*(参考|References)\s*[\s\S]*$', '', s, flags=re.I)
    s = re.sub(r'[\(（]\s*(?:https?://\S+|www\.\S+)\s*[\)）]', '', s)
    s = re.sub(r'[ \t]+', ' ', s)
    s = re.sub(r'\n{3,}', '\n\n', s)
    return s.strip()

def hide_links(text: str) -> str:
    if not text:
        return text
    t = text.rstrip()
    t = re.sub(r"[（(]\s*\[原文\]\((https?://[^\s)）]+)\)\s*[)）]$", r"[原文](\1)", t)
    t = re.sub(r"\[原文\]\(\s*\[原文\]\((https?://[^\s)]+)\)\s*\)$", r"[原文](\1)", t)
    t = re.sub(r"[（(]\s*(https?://[^\s)）]+)\s*[)）]$", r"[原文](\1)", t)
    t = re.sub(r"(https?://[^\s)）]+)$", r"[原文](\1)", t)
    return t
