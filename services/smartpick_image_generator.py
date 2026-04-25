"""
SmartPick image generator — PIL-based PNG renderer for Telegram sendPhoto.

Output: 1200x628 PNG (Telegram link-preview optimal ratio).
Pure, no network, no DB. Raises on invalid payload; caller decides fallback.
"""
from __future__ import annotations

import io
import logging
from math import pi, cos, sin
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger("sesomnod.smartpick_image")

# Canvas
W, H = 1200, 628

# Palette
BG_TOP = (10, 10, 10)          # #0A0A0A obsidian
BG_BOTTOM = (15, 30, 46)       # #0F1E2E dark navy
INK = (255, 244, 209)          # cream
INK_DIM = (167, 157, 140)      # muted cream
INK_FAINT = (90, 88, 80)

TIER_COLORS = {
    "ATOMIC": (0, 229, 255),   # cyan
    "EDGE": (212, 175, 55),    # gold
    "MONITORED": (120, 120, 120),
}

FONT_PATHS = [
    # Linux / Railway
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans.ttf",
    # macOS (local dev)
    "/System/Library/Fonts/Helvetica.ttc",
    "/Library/Fonts/Arial Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    """Try known paths; fall back to PIL default."""
    for path in FONT_PATHS:
        try:
            return ImageFont.truetype(path, size=size)
        except (OSError, IOError):
            continue
    logger.warning("[SmartPickImage] No TrueType font found — falling back to PIL default")
    return ImageFont.load_default()


def _text_size(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
) -> tuple[int, int]:
    """Return (width, height) for a text string under given font."""
    try:
        l, t, r, b = draw.textbbox((0, 0), text, font=font)
        return r - l, b - t
    except AttributeError:
        # very old PIL
        return draw.textsize(text, font=font)  # type: ignore[attr-defined]


def _vertical_gradient(img: Image.Image, top: tuple, bottom: tuple) -> None:
    """Paint a vertical gradient from top to bottom color onto img (in-place)."""
    width, height = img.size
    base = Image.new("RGB", (1, height), color=top)
    px = base.load()
    for y in range(height):
        t = y / max(1, height - 1)
        r = int(top[0] + (bottom[0] - top[0]) * t)
        g = int(top[1] + (bottom[1] - top[1]) * t)
        b = int(top[2] + (bottom[2] - top[2]) * t)
        px[0, y] = (r, g, b)
    img.paste(base.resize((width, height)), (0, 0))


def _truncate_to_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> str:
    """Ellipsize a string so it fits max_width when rendered with font."""
    w, _ = _text_size(draw, text, font)
    if w <= max_width:
        return text
    ellipsis = "…"
    while text and _text_size(draw, text + ellipsis, font)[0] > max_width:
        text = text[:-1]
    return (text + ellipsis) if text else ""


def _draw_tier_badge(
    img: Image.Image,
    draw: ImageDraw.ImageDraw,
    tier: str,
    right_edge: int,
    top: int,
) -> None:
    color = TIER_COLORS.get(tier, TIER_COLORS["MONITORED"])
    label = tier.upper()
    font = _load_font(22, bold=True)
    w, h = _text_size(draw, label, font)
    pad_x, pad_y = 16, 8
    box_w = w + pad_x * 2
    box_h = h + pad_y * 2
    x1 = right_edge - box_w
    y1 = top
    x2 = right_edge
    y2 = top + box_h

    # Glow: draw a faint halo layer on a larger transparent image, then paste
    glow = Image.new("RGBA", (box_w + 40, box_h + 40), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    for i, alpha in enumerate((40, 80, 140)):
        glow_draw.rounded_rectangle(
            (20 - i * 4, 20 - i * 4, 20 + box_w + i * 4, 20 + box_h + i * 4),
            radius=6 + i,
            outline=(*color, alpha),
            width=1,
        )
    img.paste(glow, (x1 - 20, y1 - 20), glow)

    # Solid pill
    draw.rounded_rectangle((x1, y1, x2, y2), radius=6, outline=color, width=2)
    draw.text(
        (x1 + pad_x, y1 + pad_y - 2),
        label,
        font=font,
        fill=color,
    )


def _draw_atomic_ring(
    draw: ImageDraw.ImageDraw,
    cx: int,
    cy: int,
    radius: int,
    score: int,
    max_score: int,
    color: tuple,
) -> None:
    """Circular 'X / 9' ring — progress arc + centered numeric."""
    # Background ring
    draw.ellipse(
        (cx - radius, cy - radius, cx + radius, cy + radius),
        outline=INK_FAINT,
        width=4,
    )
    # Progress arc (start at top, 12 o'clock)
    fraction = max(0.0, min(1.0, score / max(1, max_score)))
    if fraction > 0:
        end_angle = -90 + fraction * 360
        draw.arc(
            (cx - radius, cy - radius, cx + radius, cy + radius),
            start=-90,
            end=end_angle,
            fill=color,
            width=8,
        )
    # Score text inside
    score_font = _load_font(56, bold=True)
    label_font = _load_font(18, bold=False)
    score_text = f"{score}"
    sw, sh = _text_size(draw, score_text, score_font)
    draw.text(
        (cx - sw // 2, cy - sh // 2 - 10),
        score_text,
        font=score_font,
        fill=INK,
    )
    denom = f"/ {max_score}"
    dw, _ = _text_size(draw, denom, label_font)
    draw.text(
        (cx - dw // 2, cy + sh // 2 - 4),
        denom,
        font=label_font,
        fill=INK_DIM,
    )


def _safe_get(d: Optional[dict], *keys, default=""):
    """Walk nested dict keys, return default on any miss."""
    cur = d or {}
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def generate_smartpick_image(payload: dict) -> bytes:
    """
    Render a 1200x628 PNG from a SmartPick payload.
    Safe on missing fields — uses sensible defaults.
    Returns PNG bytes.
    """
    if not isinstance(payload, dict):
        raise TypeError(f"payload must be dict, got {type(payload).__name__}")

    home = str(_safe_get(payload, "match", "home_team", default="—"))
    away = str(_safe_get(payload, "match", "away_team", default="—"))
    league = str(_safe_get(payload, "match", "league", default=""))
    kickoff = str(_safe_get(payload, "match", "kickoff_oslo", default=""))
    market = str(_safe_get(payload, "selection", "market", default="—"))
    odds_raw = _safe_get(payload, "selection", "odds", default=0.0)
    try:
        odds = float(odds_raw) if odds_raw is not None else 0.0
    except (TypeError, ValueError):
        odds = 0.0
    tier = str(_safe_get(payload, "math", "tier", default="MONITORED") or "MONITORED").upper()
    try:
        edge_pct = float(_safe_get(payload, "math", "edge_pct", default=0.0))
    except (TypeError, ValueError):
        edge_pct = 0.0
    try:
        ev_pct = float(_safe_get(payload, "math", "ev_pct", default=0.0))
    except (TypeError, ValueError):
        ev_pct = 0.0
    try:
        atomic_score = int(_safe_get(payload, "math", "atomic_score", default=0) or 0)
    except (TypeError, ValueError):
        atomic_score = 0

    tier_color = TIER_COLORS.get(tier, TIER_COLORS["MONITORED"])

    # Canvas
    img = Image.new("RGB", (W, H), color=BG_TOP)
    _vertical_gradient(img, BG_TOP, BG_BOTTOM)
    draw = ImageDraw.Draw(img, "RGBA")

    # Subtle accent line across top (tier-colored)
    draw.rectangle((0, 0, W, 3), fill=(*tier_color, 255))

    # Margins
    M = 48

    # Wordmark top-left
    wm_font = _load_font(28, bold=True)
    draw.text((M, M - 4), "SESOMNOD", font=wm_font, fill=INK, stroke_width=0)
    # Tracker caption under wordmark
    caption_font = _load_font(14, bold=False)
    draw.text(
        (M, M + 28),
        "Quantitative football intelligence",
        font=caption_font,
        fill=INK_DIM,
    )

    # Tier badge top-right
    _draw_tier_badge(img, draw, tier, right_edge=W - M, top=M - 6)

    # Left column: match meta + teams
    col_left_x = M
    meta_y = 150
    meta_font = _load_font(16, bold=False)
    meta_line = " · ".join(x for x in (league, kickoff) if x)
    if meta_line:
        draw.text(
            (col_left_x, meta_y),
            _truncate_to_width(draw, meta_line.upper(), meta_font, 680),
            font=meta_font,
            fill=INK_DIM,
        )

    team_font = _load_font(54, bold=True)
    vs_font = _load_font(22, bold=False)

    home_trunc = _truncate_to_width(draw, home, team_font, 700)
    away_trunc = _truncate_to_width(draw, away, team_font, 700)

    home_y = 190
    vs_y = home_y + 68
    away_y = vs_y + 32

    draw.text((col_left_x, home_y), home_trunc, font=team_font, fill=INK)
    draw.text((col_left_x, vs_y), "vs", font=vs_font, fill=INK_FAINT)
    draw.text((col_left_x, away_y), away_trunc, font=team_font, fill=INK)

    # Right column: atomic score ring
    ring_cx = W - 180
    ring_cy = 260
    ring_radius = 94
    _draw_atomic_ring(
        draw,
        cx=ring_cx,
        cy=ring_cy,
        radius=ring_radius,
        score=atomic_score,
        max_score=9,
        color=tier_color,
    )
    ring_label_font = _load_font(14, bold=False)
    ring_label = "ATOMIC SCORE"
    lw, _ = _text_size(draw, ring_label, ring_label_font)
    draw.text(
        (ring_cx - lw // 2, ring_cy + ring_radius + 14),
        ring_label,
        font=ring_label_font,
        fill=INK_DIM,
    )

    # Divider line above footer
    divider_y = H - 152
    draw.line((M, divider_y, W - M, divider_y), fill=(60, 60, 60, 255), width=1)

    # Footer: PICK + metrics
    pick_label_font = _load_font(14, bold=False)
    draw.text(
        (M, divider_y + 18),
        "PICK",
        font=pick_label_font,
        fill=INK_DIM,
    )
    pick_font = _load_font(32, bold=True)
    odds_str = f"@ {odds:.2f}" if odds > 0 else ""
    pick_text = f"{market} {odds_str}".strip()
    draw.text(
        (M, divider_y + 36),
        _truncate_to_width(draw, pick_text, pick_font, W - M * 2),
        font=pick_font,
        fill=INK,
    )

    # Metrics row
    metric_font = _load_font(18, bold=False)
    metric_value_font = _load_font(22, bold=True)
    edge_label = "EDGE"
    ev_label = "EV"
    edge_val = f"+{edge_pct:.1f}%" if edge_pct >= 0 else f"{edge_pct:.1f}%"
    ev_val = f"+{ev_pct:.1f}%" if ev_pct >= 0 else f"{ev_pct:.1f}%"

    metric_y = H - 60
    draw.text((M, metric_y), edge_label, font=metric_font, fill=INK_DIM)
    el_w, _ = _text_size(draw, edge_label, metric_font)
    draw.text((M + el_w + 8, metric_y - 3), edge_val, font=metric_value_font, fill=tier_color)

    gap_x = M + el_w + 8
    ev_w, _ = _text_size(draw, edge_val, metric_value_font)
    ev_x = gap_x + ev_w + 48
    draw.text((ev_x, metric_y), ev_label, font=metric_font, fill=INK_DIM)
    evl_w, _ = _text_size(draw, ev_label, metric_font)
    draw.text((ev_x + evl_w + 8, metric_y - 3), ev_val, font=metric_value_font, fill=INK)

    # Bottom-right: URL
    url_font = _load_font(14, bold=False)
    url_text = "sesomnod.com"
    uw, uh = _text_size(draw, url_text, url_font)
    draw.text(
        (W - M - uw, H - M - uh + 4),
        url_text,
        font=url_font,
        fill=INK_FAINT,
    )

    # Encode PNG
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# Convenience: caption-length guard (Telegram sendPhoto caption limit = 1024)
TELEGRAM_PHOTO_CAPTION_MAX = 1024
