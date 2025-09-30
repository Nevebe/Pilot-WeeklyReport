
"""
Send nicely formatted weekly Markdown as HTML email (with inline CSS) to a list of recipients.
- Converts Markdown -> HTML
- Wraps it in a clean email template
- Inlines CSS for better Outlook/Gmail rendering
- Supports preview-only or actually sending via SMTP

Requirements:
    pip install markdown premailer python-dotenv

.env (put at repo root or alongside this script):
    SMTP_HOST=smtp.yourhost.com
    SMTP_PORT=587
    SMTP_USER=your_user
    SMTP_PASS=your_password
    FROM_NAME=Weekly Bot
    FROM_EMAIL=weeklybot@yourcompany.com

Example:
    python send_weekly.py \
        --md ./docs/2025-W38-bold.md \
        --subject "行业周报 · 2025 第38周" \
        --recipients ./recipients.csv \
        --css ./email_style.css \
        --preview ./preview.html \
        --send
"""
import argparse
import csv
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, make_msgid
from pathlib import Path

from dotenv import load_dotenv
from markdown import markdown
from premailer import transform as inline_css


TEMPLATE = """\
<!doctype html>
<html lang="zh-CN">
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{subject}</title>
  <style>{css}</style>
</head>
<body>
  <div class="wrapper">
    <div class="header">
      <h1>{title}</h1>
      <p>{subtitle}</p>
      <p class="window">{window}</p>
    </div>
    <p style="color: #6b7280; font-size: 12px; padding:12px 24px; background:#ffffff; border-left:2px solid #6b7280; margin:0;">
        ℹ️ 本报告基于自动化信息采集、聚合与自然语言处理技术完成初步编纂，
        旨在快速传递本周关键行业动态。由于算法仍在调优阶段，少数数据或分类可能存在偏差，
        建议结合原始来源进行交叉验证。
    </p>
    <div class="container">
      {content}
      <hr/>
      <p>💡 如果排版在某些客户端显示不理想，请使用“在浏览器中查看”。</p>
    </div>
    <div class="footer">
      本邮件由自动化脚本发送 · 请勿直接回复。如需反馈，请联系作者。
    </div>
  </div>
</body>
</html>
"""



def load_css(css_path: Path) -> str:
    if css_path and css_path.exists():
        return css_path.read_text(encoding="utf-8")
    # Fallback minimal CSS
    return "body{font-family:Arial,Helvetica,sans-serif;line-height:1.6;color:#333}"


def render_html(md_text: str, css_text: str, subject: str) -> str:
    # 拿到前三个“非空行”作为：标题 / 生成时间 / 数据时间窗口
    lines = [l.rstrip("\n") for l in md_text.splitlines()]
    non_empty_idx = [i for i, l in enumerate(lines) if l.strip()][:3]

    title = lines[non_empty_idx[0]].lstrip("# ").strip() if non_empty_idx else subject
    subtitle = lines[non_empty_idx[1]].strip() if len(non_empty_idx) > 1 else ""
    window = lines[non_empty_idx[2]].strip() if len(non_empty_idx) > 2 else ""

    # 从正文中移除这三行（以及它们附近紧跟的一条水平线 '---' 和空行）
    skip = set(non_empty_idx)
    body_lines = []
    i = 0
    while i < len(lines):
        if i in skip:
            i += 1
            # 连带吞掉紧随其后的空行/分隔线
            while i < len(lines) and (not lines[i].strip() or lines[i].strip() in ("---", "***", "___")):
                i += 1
            continue
        body_lines.append(lines[i])
        i += 1
    body_md = "\n".join(body_lines)

    # 渲染正文
    content_html = markdown(body_md, extensions=["extra", "tables", "sane_lists", "toc"])

    html = TEMPLATE.format(
        subject=subject,
        title=title or subject,
        subtitle=subtitle,
        window=window,
        content=content_html,
        css=css_text,
    )
    return inline_css(html)




def build_message(subject: str, html_body: str, plain_fallback: str, sender_name: str, sender_email: str, to_email: str) -> MIMEMultipart:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = formataddr((sender_name, sender_email))
    msg["To"] = to_email
    msg["Message-ID"] = make_msgid()
    # Plain part (fallback) uses raw markdown to keep it readable
    msg.attach(MIMEText(plain_fallback, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))
    return msg


def send_smtp(message, host, port, user, password):
    import smtplib, ssl
    port = int(port)
    timeout = 20  # 防止一直挂起

    if port == 465:  # 直连 SSL
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=timeout) as server:
            server.set_debuglevel(1)  # 输出调试信息，帮助排查问题
            server.login(user, password)
            server.send_message(message)
    else:  # 587 STARTTLS
        with smtplib.SMTP(host, port, timeout=timeout) as server:
            server.set_debuglevel(1)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(user, password)
            server.send_message(message)



def main():
    parser = argparse.ArgumentParser(description="Send Markdown weekly report as styled HTML email")
    parser.add_argument("--md", required=True, help="Path to the markdown file to send")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--recipients", required=True, help="CSV with 'email' and optional 'name' columns")
    parser.add_argument("--css", default="email_style.css", help="CSS to style the email (will be inlined)")
    parser.add_argument("--preview", default="", help="Write a preview HTML to this path (no sending)")
    parser.add_argument("--send", action="store_true", help="Actually send the email")
    args = parser.parse_args()

    load_dotenv()  # loads .env if present

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    from_name = os.getenv("FROM_NAME", "Weekly Bot")
    from_email = os.getenv("FROM_EMAIL", smtp_user or "no-reply@example.com")

    md_path = Path(args.md)
    css_path = Path(args.css)
    rec_path = Path(args.recipients)

    md_text = md_path.read_text(encoding="utf-8")
    css_text = load_css(css_path)

    html = render_html(md_text, css_text, args.subject)

    if args.preview:
        Path(args.preview).write_text(html, encoding="utf-8")
        print(f"Preview written to: {args.preview}")

    # Read recipients
    recipients = []
    with rec_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip()
            if not email:
                continue
            name = (row.get("name") or "").strip()
            recipients.append({"email": email, "name": name})

    if not args.send:
        print(f"[DRY RUN] Would send to {len(recipients)} recipients. Use --send to actually send.")
        return

    # Safety checks
    for key, label in [(smtp_host, "SMTP_HOST"), (smtp_user, "SMTP_USER"), (smtp_pass, "SMTP_PASS")]:
        if not key:
            raise RuntimeError(f"Missing SMTP config: {label}")

    # Send one by one (or could use BCC if preferred)
    sent = 0
    for r in recipients:
        to_addr = r["email"]
        msg = build_message(args.subject, html, md_text, from_name, from_email, to_addr)
        send_smtp(msg, smtp_host, smtp_port, smtp_user, smtp_pass)
        sent += 1
        print(f"Sent to {to_addr}")
    print(f"Done. Sent {sent} messages.")

if __name__ == "__main__":
    main()
