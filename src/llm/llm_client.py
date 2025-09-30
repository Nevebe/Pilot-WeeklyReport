
# -*- coding: utf-8 -*-
from openai import OpenAI
from ..config_loader import getenv, MODEL

def get_llm_client_and_model():
    provider = getenv("LLM_PROVIDER", "openai").lower()
    if provider == "deepseek":
        api_key  = getenv("DEEPSEEK_API_KEY") or getenv("OPENAI_API_KEY")
        base_url = getenv("OPENAI_BASE_URL", "https://api.deepseek.com")
        model    = getenv("MODEL", MODEL or "deepseek-chat")
    else:
        api_key  = getenv("OPENAI_API_KEY")
        base_url = getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        model    = getenv("MODEL", MODEL or "gpt-4o-mini")
    if not api_key:
        raise SystemExit("未设置 API Key（DEEPSEEK_API_KEY 或 OPENAI_API_KEY）")
    client = OpenAI(api_key=api_key, base_url=base_url)
    return client, model
