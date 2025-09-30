import math
import pathlib
import pandas as pd
import yaml

# ------------- 路径 -------------
ROOT = pathlib.Path(__file__).resolve().parents[1]
XLSX = ROOT / "data" / "sources.xlsx"        # Excel 输入
IDS_TXT = ROOT / "data" / "ids.txt"          # 输出：ids 列表（供抓取脚本读取）
SRC_YML = ROOT / "config" / "sources.yml"    # 输出：源权重/官方配置 + 元数据

# ------------- 工具 -------------
def to_bool(v) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "y", "yes", "t", "是", "真", "官方")

def tier_weight(rank):
    """
    根据 rank 推权重（rank 越小越重要，可按需改）：
      1:1.35, 2:1.30, 3:1.25, 4:1.20, 5:1.15, 6+:1.12→0.95 逐步下降
    支持 rank 缺失或非法时返回 1.0
    """
    if pd.isna(rank):
        return 1.0
    try:
        r = int(rank)
    except Exception:
        return 1.0
    if r <= 1: return 1.35
    if r == 2: return 1.30
    if r == 3: return 1.25
    if r == 4: return 1.20
    if r == 5: return 1.15
    # 6 开始缓降，不低于 0.95
    return max(0.95, 1.15 - 0.03 * (r - 5))

# 可能的列名映射（不区分大小写）
COL_ALIASES = {
    "id": ["id", "账号id", "public_id", "mp_id"],
    "rank": ["rank", "层级", "权重序", "排序"],
    "weight": ["weight", "权重"],
    "isofficial": ["isofficial", "official", "is_official", "官方"],
    "category": ["分类", "category", "cate", "类别"],
    "desc": ["des", "desc", "备注", "描述"],
    "isindie": ["isindie", "indie", "is_indie", "独立"],
}

def resolve_cols(df: pd.DataFrame):
    """返回实际列名映射：{'逻辑名': '真实列名'(或 None)}"""
    low = {c.strip().lower(): c for c in df.columns}
    resolved = {}
    for key, names in COL_ALIASES.items():
        real = None
        for n in names:
            if n.lower() in low:
                real = low[n.lower()]
                break
        resolved[key] = real
    return resolved

# ------------- 主流程 -------------
def main():
    if not XLSX.exists():
        raise SystemExit(f"未找到 Excel：{XLSX}")

    # 读第一张表；dtype=str 尽量保留原值
    df = pd.read_excel(XLSX, dtype=str)
    cols = resolve_cols(df)

    if not cols["id"]:
        raise SystemExit("Excel 里必须有 id 列！(支持别名: id / 账号id / public_id / mp_id)")

    # 清洗 ID
    df["__id__"] = df[cols["id"]].astype(str).str.strip()
    df = df[df["__id__"] != ""].copy()
    df = df.drop_duplicates(subset=["__id__"], keep="first")

    # 尝试拉 rank/weight
    if cols["rank"]:
        def _to_num(v):
            try:
                return float(v)
            except Exception:
                return math.inf
        df["__rank__"] = df[cols["rank"]].apply(_to_num)
    else:
        df["__rank__"] = math.inf

    # 从表里读取 weight（如提供则覆盖策略权重）
    if cols["weight"]:
        def _to_w(v):
            try:
                x = float(v)
                # 合理性保护
                return max(0.5, min(2.0, x))
            except Exception:
                return None
        df["__weight_override__"] = df[cols["weight"]].apply(_to_w)
    else:
        df["__weight_override__"] = None

    # 排序：rank->id
    df = df.sort_values(by=["__rank__", "__id__"], ascending=[True, True]).reset_index(drop=True)

    # 生成 ids.txt
    IDS_TXT.parent.mkdir(parents=True, exist_ok=True)
    with IDS_TXT.open("w", encoding="utf-8") as f:
        for _id in df["__id__"].tolist():
            f.write(_id + "\n")
    print(f"[OK] 写出 {IDS_TXT}（{len(df)} 行）")

    # 生成 sources.yml
    weights = {}
    metas = {}
    has_official = bool(cols["isofficial"])

    for _, row in df.iterrows():
        pid = row["__id__"]
        # 计算权重
        if row["__weight_override__"] is not None:
            w = float(row["__weight_override__"])
        else:
            # 无覆盖则由 rank 推导
            rank_val = row[cols["rank"]] if cols["rank"] else None
            w = tier_weight(rank_val)

        # 官方标记
        official = to_bool(row[cols["isofficial"]]) if cols["isofficial"] else False

        weights[pid] = {"weight": float(f"{w:.2f}"), "official": bool(official)}

        # meta（可选，便于审阅/将来扩展）
        meta = {}
        if cols["category"] and pd.notna(row[cols["category"]]):
            meta["category"] = str(row[cols["category"]]).strip()
        if cols["desc"] and pd.notna(row[cols["desc"]]):
            meta["desc"] = str(row[cols["desc"]]).strip()
        if cols["isindie"]:
            meta["isIndie"] = bool(to_bool(row[cols["isindie"]]))
        if meta:
            metas[pid] = meta

    data = {
        "weights": weights,
        "defaults": {"weight": 1.0, "official": False},
    }
    if metas:
        data["meta"] = metas

    SRC_YML.parent.mkdir(parents=True, exist_ok=True)
    with SRC_YML.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

    print(f"[OK] 写出 {SRC_YML}（含权重与官方标记{'，含 meta' if metas else ''}）")
    print(f"[SUM] 条目：{len(df)}，官方源：{sum(1 for v in weights.values() if v['official'])}，"
          f"权重均值：{sum(v['weight'] for v in weights.values())/len(weights):.2f}")

if __name__ == "__main__":
    main()
