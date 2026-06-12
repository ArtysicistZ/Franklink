"""
Compose the product-walkthrough cards used in the README.

Each card bakes a phone screenshot on one side and a heading + bullet points on
the other into a single PNG, so the README embeds one clean centered image per
step (no markdown tables or floats). Re-run to regenerate:

    "D:/Anaconda3/envs/ctestenv/python.exe" docs/cards/generate_cards.py
"""

from __future__ import annotations

import os

from PIL import Image, ImageDraw, ImageFilter, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
IMG = os.path.join(HERE, "..", "images")
OUT = HERE

# Brand palette (matches the website + charts).
INK = (15, 23, 42)
MUTED = (71, 85, 105)
ACCENT = (37, 99, 235)
CARD = (255, 255, 255)
BORDER = (226, 232, 240)
SHADOW = (15, 23, 42)

W, H = 1560, 820
PAD = 30
PHONE_H = 700
PHONE_RATIO = 1290 / 2796
PHONE_W = round(PHONE_H * PHONE_RATIO)
RADIUS = 36


def _font(names, size):
    for n in names:
        for path in (f"C:/Windows/Fonts/{n}", n):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


F_NUM = _font(["seguisb.ttf", "segoeuib.ttf", "arialbd.ttf"], 30)
F_TITLE = _font(["segoeuib.ttf", "arialbd.ttf"], 52)
F_BULLET = _font(["segoeui.ttf", "arial.ttf"], 33)
F_NOTE = _font(["seguisbi.ttf", "segoeuii.ttf", "ariali.ttf"], 30)


def _rounded(im: Image.Image, radius: int) -> Image.Image:
    mask = Image.new("L", im.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, im.size[0], im.size[1]], radius, fill=255)
    out = im.convert("RGBA")
    out.putalpha(mask)
    return out


def _wrap(draw, text, font, max_w):
    words, lines, cur = text.split(), [], ""
    for w in words:
        trial = f"{cur} {w}".strip()
        if draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def make_card(screenshot, number, title, bullets, phone_left, note, out_name):
    base = Image.new("RGBA", (W, H), (0, 0, 0, 0))

    # Card surface with a soft shadow + 1px border.
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle([PAD - 4, PAD + 6, W - PAD + 4, H - PAD + 18],
                                             40, fill=SHADOW + (55,))
    base.alpha_composite(shadow.filter(ImageFilter.GaussianBlur(18)))
    card = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(card).rounded_rectangle([PAD, PAD, W - PAD, H - PAD], 40,
                                           fill=CARD + (255,), outline=BORDER + (255,), width=2)
    base.alpha_composite(card)
    draw = ImageDraw.Draw(base)

    # Phone screenshot with rounded corners + its own shadow.
    phone = _rounded(Image.open(screenshot).convert("RGB").resize((PHONE_W, PHONE_H)), RADIUS)
    py = (H - PHONE_H) // 2
    px = (PAD + 48) if phone_left else (W - PAD - 48 - PHONE_W)
    psh = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(psh).rounded_rectangle([px + 6, py + 12, px + PHONE_W + 6, py + PHONE_H + 12],
                                          RADIUS, fill=SHADOW + (75,))
    base.alpha_composite(psh.filter(ImageFilter.GaussianBlur(18)))
    base.alpha_composite(phone, (px, py))

    # Text column on the opposite side.
    tx = (px + PHONE_W + 80) if phone_left else (PAD + 70)
    tx_max = (W - PAD - 70) if phone_left else (px - 80)
    text_w = tx_max - tx

    # Pre-compute wrapped lines so the whole block can be vertically centered.
    title_lines = _wrap(draw, title, F_TITLE, text_w)
    bullet_lines = [_wrap(draw, b, F_BULLET, text_w - 46) for b in bullets]

    block_h = 52 + len(title_lines) * 64 + 22
    block_h += sum(len(bl) * 44 + 16 for bl in bullet_lines)
    if note:
        block_h += 10 + 40
    y = max(PAD + 40, (H - block_h) // 2)

    # Accent step number.
    draw.text((tx, y), f"STEP {number}", font=F_NUM, fill=ACCENT)
    y += 52
    for line in title_lines:
        draw.text((tx, y), line, font=F_TITLE, fill=INK)
        y += 64
    y += 22

    # Bullets with an accent dot.
    for lines in bullet_lines:
        draw.ellipse([tx + 4, y + 15, tx + 18, y + 29], fill=ACCENT)
        for line in lines:
            draw.text((tx + 46, y), line, font=F_BULLET, fill=MUTED)
            y += 44
        y += 16

    if note:
        y += 10
        draw.text((tx, y), note, font=F_NOTE, fill=ACCENT)

    base.convert("RGB").save(os.path.join(OUT, out_name))


CARDS = [
    ("onboarding-1.png", "01", "Meet your AI concierge",
     ["Onboards through natural conversation, no forms or profile builder",
      "Learns your background, interests, and career goals as you chat",
      "Feels like texting a friend, not filling out a survey"],
     True, "", "card-01-onboarding.png"),
    ("onboarding-2.png", "02", "Privacy-first email connect",
     ["Read-only Gmail context through Composio OAuth",
      "Reads professional signals without storing sensitive data",
      "You stay in control of exactly what Frank can see"],
     False, "", "card-02-email.png"),
    ("supporting-1.png", "03", "Define your needs",
     ["Say what you want: internship advice, a co-founder, or a mentor",
      "Captures demand signals for semantic matching",
      "No keywords or filters to configure"],
     True, "", "card-03-needs.png"),
    ("supporting-2.png", "04", "Showcase your value",
     ["Share your projects and experience in plain text",
      "Frank extracts value signals and stores them as embeddings",
      "Others find you by what you actually bring to the table"],
     False, "", "card-04-value.png"),
    ("networking-1.png", "05", "Connection requests",
     ["Personalized introductions delivered to your iMessage",
      "AI-curated messages, no app required",
      "Accept or pass with a single reply"],
     True, "", "card-05-requests.png"),
    ("networking-2.png", "06", "Grow your network",
     ["New members trigger relevant introductions automatically",
      "Contacts are saved as your network expands",
      "Real-time community announcements keep the network alive"],
     False, "", "card-06-grow.png"),
    ("group-matching.png", "07", "Smart group matching",
     ["Ask Frank to find more people with shared interests",
      "Forms multi-person group chats with AI-generated icebreakers",
      "Powered by the Zep knowledge graph"],
     True, "The first chat that becomes your startup.", "card-07-group.png"),
]


if __name__ == "__main__":
    for shot, num, title, bullets, left, note, out in CARDS:
        make_card(os.path.join(IMG, shot), num, title, bullets, left, note, out)
    print(f"{len(CARDS)} cards written to {OUT}")
