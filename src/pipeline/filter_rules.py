
# -*- coding: utf-8 -*-
import re

AD_WORDS = [
    "报名","报名通道","扫码","二维码","添加微信","加微信","VX","VX：","V：","咨询",
    "优惠","折扣","团购","到店","限时","仅需","私信","合作","转发抽奖","抽奖",
    "直播预告","公开课","沙龙","峰会","购票","订阅","投放","招商","招募","征稿"
]

def ad_score(title: str, text: str) -> int:
    t = f"{title or ''} {text or ''}"
    score = 0
    if len(text) < 120: score += 1
    if re.search(r"https?://", t): score += 1
    if re.search(r"\b1[3-9]\d{9}\b", t): score += 2
    if re.search(r"(?:vx|v信|wx|微信|加微|VX[:：])", t, re.I): score += 2
    if re.search(r"[!！]{2,}", t): score += 1
    score += sum(1 for w in AD_WORDS if w in t)
    return score
