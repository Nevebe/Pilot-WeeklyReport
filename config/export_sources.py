# -*- coding: utf-8 -*-
from pathlib import Path
import re, math
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[0]
CONFIG_DIR = ROOT
EXCEL_PATH = CONFIG_DIR / "sources.xlsx"
OUTPUT_YML = CONFIG_DIR / "sources.yml"

def norm_col(s: str) -> str:
    if s is None: return ""
    s = str(s).replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    s = s.replace("_", "").lower()
    return s

def find_col(df, aliases):
    norm_map = {norm_col(c): c for c in df.columns}
    for a in aliases:
        a_norm = norm_col(a)
        if a_norm in norm_map:
            return norm_map[a_norm]
    return None

def to_bool_relaxed(v, default=True):
    """更宽松的启用判断：0/false/no/否/停用/禁用/关闭 => False，其余 => True"""
    if v is None: return default
    s = str(v).strip().lower()
    if s in ("", "nan"): return default
    negatives = {"0","false","f","no","n","否","不","停用","禁用","关闭","關閉","停用中","disabled","inactive"}
    return s not in negatives

def to_bool(v, default=False):
    if v is None: return default
    s = str(v).strip().lower()
    return s in ("1","true","t","y","yes","是","對","对")

def to_int(v, default=None):
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default

def to_float(v, default=None):
    try:
        return float(str(v).strip())
    except Exception:
        return default

SEP = re.compile(r"[,\|，、；;]+")
def split_expertise(v):
    if v is None: return []
    parts = [x.strip() for x in SEP.split(str(v)) if x and str(x).strip()]
    return [p for p in parts if p]

def clean_id(v: str) -> str:
    if v is None: return ""
    s = str(v)
    s = s.replace("\u200b","").replace("\ufeff","").replace("\u3000"," ")
    s = s.strip()
    return s

def read_all_sheets(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    dfs = []
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh, dtype=str, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        df["__sheet__"] = sh
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def main():
    if not EXCEL_PATH.exists():
        raise SystemExit(f"未找到 Excel：{EXCEL_PATH}")

    df = read_all_sheets(EXCEL_PATH)

    col_id         = find_col(df, ["id","源id","账号id","sourceid","账号","帐号","account","accountid"])
    col_rank       = find_col(df, ["rank","排序","级别","权重级别"])
    col_isofficial = find_col(df, ["isofficial","official","是否官方","官方"])
    col_expertise  = find_col(df, ["擅长","领域","标签","expertise","goodat","标签/擅长"])
    col_weight     = find_col(df, ["weight","权重","权重值"])
    col_des        = find_col(df, ["des","desc","描述","备注","说明"])
    col_isindie    = find_col(df, ["isindie","indie","是否独立","独立"])
    col_enabled    = find_col(df, ["enabled","enable","启用","是否启用","启用状态","active","状态"])

    if not col_id:
        raise SystemExit("Excel 缺少必填列：id（或别名）")

    total_rows = len(df)
    empty_id = 0
    disabled = 0
    dup_overwrite = 0

    weights, meta = {}, {}
    seen = set()

    print("\n—— 解析明细（自检）——")
    for _, row in df.iterrows():
        sid = clean_id(row.get(col_id) if col_id else "")
        if not sid:
            empty_id += 1
            continue

        # 启用判断
        enabled = True
        if col_enabled:
            # 若存在启用列，用宽松判断：负面词 => False，其余 => True
            enabled = to_bool_relaxed(row.get(col_enabled), True)
        if not enabled:
            disabled += 1
            print(f"- 跳过 {sid}（未启用）")
            continue

        rank       = to_int(row.get(col_rank), None)               if col_rank       else None
        isofficial = to_bool(row.get(col_isofficial), False)       if col_isofficial else False
        exp_raw    = row.get(col_expertise)                        if col_expertise  else None
        expertise  = split_expertise(exp_raw)                      if exp_raw        else []
        des = str(row.get(col_des) or "").strip() if col_des else ""
        indie      = to_bool(row.get(col_isindie), None)           if col_isindie    else None
        w_raw      = row.get(col_weight)                           if col_weight     else None
        w_val      = to_float(w_raw, None)

        if w_val is not None:
            base_w = max(0.5, min(w_val, 5.0))
        else:
            if rank is not None:
                base_w = 1.4 - 0.1 * (rank - 1)
                base_w = max(0.95, round(base_w, 2))
            else:
                base_w = 1.0

        if sid in weights:
            dup_overwrite += 1  # 记录被覆盖次数

        entry = {"weight": float(base_w), "official": bool(isofficial)}
        if expertise:
            entry["expertise"] = expertise
        weights[sid] = entry

        meta[sid] = {
            "rank": rank,
            "isofficial": bool(isofficial),
            "weight_raw": None if (w_raw in (None,"")) else str(w_raw),
            "des": des or None,
            "isIndie": indie,
            "__sheet__": row.get("__sheet__"),
        }

        print(f"- {sid} | weight={base_w}（raw={w_raw}） | rank={rank} | official={isofficial} | expertise={expertise} | sheet={row.get('__sheet__')}")

    data = {
        "weights": weights,
        "defaults": {"weight": 1.0, "official": False},
        "meta": meta
    }

    OUTPUT_YML.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_YML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

    print("\n—— 汇总 ——")
    print(f"总行数: {total_rows}")
    print(f"空 id 行: {empty_id}")
    print(f"未启用行: {disabled}")
    print(f"重复 id 覆盖次数: {dup_overwrite}")
    print(f"最终导出: {len(weights)}")

if __name__ == "__main__":
    main()
