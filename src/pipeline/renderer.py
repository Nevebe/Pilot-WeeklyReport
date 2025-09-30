
# -*- coding: utf-8 -*-
from jinja2 import Environment, FileSystemLoader, select_autoescape
from ..config_loader import TEMPLATES, DOCS

def render_markdown(ctx):
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)),
                      autoescape=select_autoescape())
    tpl = env.get_template("weekly_report.md.j2")
    return tpl.render(**ctx)

def write_docs(md_text, year, week):
    DOCS.mkdir(parents=True, exist_ok=True)
    fname = f"{year}-W{week:02d}.md"
    (DOCS / fname).write_text(md_text, encoding="utf-8")

    index_path = DOCS / "index.md"
    if index_path.exists():
        old = index_path.read_text(encoding="utf-8")
    else:
        old = "# 周报索引\n\n"
    line = f"- [{year}年第 {week:02d} 周]({fname})\n"
    if line not in old:
        index_path.write_text(old + line, encoding="utf-8")
