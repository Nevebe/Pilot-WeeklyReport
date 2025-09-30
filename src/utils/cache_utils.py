
# -*- coding: utf-8 -*-
import json, hashlib, time
from ..config_loader import CACHE_ENABLED, CACHE_DIR, CACHE_TTL_HOURS

def _hash_for_cache(title, text, source_id):
    h = hashlib.sha256()
    h.update((title or "").encode("utf-8"))
    h.update(b"\n--\n")
    h.update((text or "").encode("utf-8"))
    h.update(b"\n--\n")
    h.update((source_id or "").encode("utf-8"))
    return h.hexdigest()

def cache_get(title, text, source_id):
    if not CACHE_ENABLED:
        return None
    CACHE_DIR.mkdir(exist_ok=True, parents=True)
    fn = CACHE_DIR / (_hash_for_cache(title, text, source_id) + ".json")
    if not fn.exists():
        return None
    if CACHE_TTL_HOURS > 0:
        age_hours = (time.time() - fn.stat().st_mtime) / 3600.0
        if age_hours > CACHE_TTL_HOURS:
            try: fn.unlink(missing_ok=True)
            except Exception: pass
            return None
    try:
        return json.loads(fn.read_text(encoding="utf-8"))
    except Exception:
        return None

def cache_set(title, text, source_id, data):
    if not CACHE_ENABLED:
        return
    CACHE_DIR.mkdir(exist_ok=True, parents=True)
    fn = CACHE_DIR / (_hash_for_cache(title, text, source_id) + ".json")
    try:
        fn.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
