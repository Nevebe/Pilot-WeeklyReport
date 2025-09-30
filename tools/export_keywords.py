# tools/export_keywords.py
import pathlib
import re
import pandas as pd
import yaml

# --------- 路径 ---------
ROOT = pathlib.Path(__file__).resolve().parents[1]
XLSX = ROOT / "data" / "keywords.xlsx"     # 输入：Excel
YML  = ROOT / "config" / "keywords.yml"    # 输出：YAML

# --------- 规范化与解析工具 ---------
def norm_bool(v, default=False):
    if pd.isna(v):
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "y", "yes", "是", "對", "对")

def norm_float(v, default=None):
    if pd.isna(v) or str(v).strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def norm_int(v, default=None):
    if pd.isna(v) or str(v).strip() == "":
        return default
    try:
        return int(float(v))
    except Exception:
        return default

_SEP_PATTERN = re.compile(r"[,\|，；;、]+")

def split_any(v):
    if pd.isna(v) or str(v).strip() == "":
        return []
    return [x.strip() for x in _SEP_PATTERN.split(str(v)) if x.strip()]

def map_scope(v):
    """Excel 里常写 all -> YAML 用 both；默认 both"""
    if pd.isna(v) or str(v).strip() == "":
        return "both"
    s = str(v).strip().lower()
    if s in ("all", "both", "全部", "所有"):
        return "both"
    if s in ("title", "标题"):
        return "title"
    if s in ("desc", "summary", "描述", "正文"):
        return "desc"
    return "both"

def _norm_header(h: str) -> str:
    """列名规范化：小写 + 去空格/下划线/破折号"""
    return re.sub(r"[\s_\-]+", "", h.strip().lower())

# 允许的列名映射（标准键 -> 备选写法）
COL_ALIASES = {
    "keyword":   ["keyword", "关键词", "关键字"],
    "weight":    ["weight", "权重"],
    "category":  ["category", "类别", "分类"],
    "regex":     ["regex", "是否正则", "isregex"],
    "synonyms":  ["synonyms", "同义词", "同義詞"],
    "negatives": ["negatives", "负向词", "排除词", "负面词"],
    "sources":   ["sources", "来源限定", "源限定"],
    "scope":     ["scope", "范围"],
    "decay_day": ["decay day", "decay_day", "decay_days", "衰减天数", "衰減天數"],
    "note":      ["note", "备注", "備註"],
    "enabled":   ["enabled", "启用", "enable", "是否启用"],
    "desc":      ["desc", "说明", "描述"],
}

def resolve_cols(df: pd.DataFrame):
    """把 DataFrame 的真实列名映射到标准键"""
    # 规范化后的列名 -> 原始列名
    lut = {_norm_header(c): c for c in df.columns}
    out = {}
    for std, alts in COL_ALIASES.items():
        for a in alts:
            key = _norm_header(a)
            if key in lut:
                out[std] = lut[key]
                break
    return out

def pick_sheet(file_path: pathlib.Path) -> pd.DataFrame:
    """自动选择一个包含关键列(至少 keyword & category)的工作表"""
    xl = pd.ExcelFile(file_path)
    candidate = None

    # 优先选择叫 Sheet1 的
    prefer = [s for s in xl.sheet_names if _norm_header(s) in ("sheet1", "keywords")]
    names = prefer + [s for s in xl.sheet_names if s not in prefer]

    for name in names:
        df = xl.parse(name)
        cols = resolve_cols(df)
        if "keyword" in cols and "category" in cols:
            return df
        # 记录一个次优候选
        if candidate is None and "keyword" in cols:
            candidate = df

    if candidate is not None:
        return candidate

    raise SystemExit("在 Excel 中找不到同时包含 'keyword' 和 'category' 的工作表，请检查。")

# --------- 主流程 ---------
def main():
    if not XLSX.exists():
        raise SystemExit(f"未找到 Excel：{XLSX}")

    df = pick_sheet(XLSX)
    cols = resolve_cols(df)

    # 校验必填列
    for required in ("keyword", "category"):
        if required not in cols:
            raise SystemExit(f"Excel 缺少必填列：{required}")

    # 取列并清洗
    df = df.dropna(subset=[cols["keyword"], cols["category"]]).copy()
    df["keyword"]  = df[cols["keyword"]].astype(str).str.strip()
    df["category"] = df[cols["category"]].astype(str).str.strip()
    df = df[(df["keyword"] != "") & (df["category"] != "")]

    data = {
        "defaults": {
            "weight": 1.0,
            "scope": "both",
            "decay_days": 14,
        },
        "categories": {},        # {category: [entries...]}
        "global_negatives": [],  # 全局负词
    }

    for _, row in df.iterrows():
        category = str(row[cols["category"]]).strip()
        keyword  = str(row[cols["keyword"]]).strip()

        # enabled（可选）: 不填默认启用
        enabled = True
        if "enabled" in cols:
            enabled = norm_bool(row.get(cols["enabled"]), True)
        if not enabled:
            continue

        # 若是 global_negatives 类，作为全局负词
        if _norm_header(category) == "globalnegatives":
            data["global_negatives"].append(keyword)
            continue

        weight = norm_float(row.get(cols["weight"])) if "weight" in cols else None
        regex  = norm_bool(row.get(cols["regex"]), False) if "regex" in cols else False
        scope  = map_scope(row.get(cols["scope"])) if "scope" in cols else "both"
        decay  = norm_int(row.get(cols["decay_day"]), 14) if "decay_day" in cols else 14

        synonyms  = split_any(row.get(cols["synonyms"])) if "synonyms" in cols else []
        negatives = split_any(row.get(cols["negatives"])) if "negatives" in cols else []
        sources   = split_any(row.get(cols["sources"])) if "sources" in cols else []
        note      = str(row.get(cols["note"])).strip() if "note" in cols and not pd.isna(row.get(cols["note"])) else ""
        _desc     = str(row.get(cols["desc"])).strip() if "desc" in cols and not pd.isna(row.get(cols["desc"])) else ""

        entry = {
            "keyword": keyword,
            "weight": weight if weight is not None else 1.0,
            "regex": regex,
            "synonyms": synonyms,     # 仅记录；同义词会展开为独立条目
            "negatives": negatives,
            "sources": sources,       # [] 表示全局
            "scope": scope,           # both/title/desc
            "decay_days": decay,
        }
        if note:
            entry["note"] = note
        if _desc:
            entry["desc"] = _desc

        data["categories"].setdefault(category, []).append(entry)

        # 把同义词展开为独立规则（与主词同配置）
        for syn in synonyms:
            if syn.strip():
                data["categories"][category].append({
                    **{k: v for k, v in entry.items() if k not in ("keyword", "synonyms")},
                    "keyword": syn.strip(),
                })

    # 输出 YAML
    YML.parent.mkdir(parents=True, exist_ok=True)
    with YML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

    cats = len(data["categories"])
    total = sum(len(v) for v in data["categories"].values())
    print(f"[OK] 导出完成：{YML}")
    print(f"     类别数={cats}，词条数（含同义词展开）={total}，全局负词={len(data['global_negatives'])}")

if __name__ == "__main__":
    main()
