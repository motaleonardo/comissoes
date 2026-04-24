"""Simple PDF export helpers for report sections."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Iterable

import pandas as pd
from PIL import Image, ImageDraw, ImageFont


PAGE_WIDTH = 3508
PAGE_HEIGHT = 2480
MARGIN_X = 40
MARGIN_Y = 40
TITLE_FONT_SIZE = 34
SECTION_FONT_SIZE = 22
HEADER_FONT_SIZE = 14
BODY_FONT_SIZE = 12
ROW_HEIGHT = 32
HEADER_HEIGHT = 42
SECTION_GAP = 20
CELL_PADDING_X = 8
MIN_COL_WIDTH = 90
GRID_COLOR = "#BFC5D2"
HEADER_FILL = "#E9EDF5"
TEXT_COLOR = "#111827"


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = []
    if bold:
        candidates.extend(
            [
                Path(r"C:\Windows\Fonts\arialbd.ttf"),
                Path(r"C:\Windows\Fonts\consolab.ttf"),
            ]
        )
    candidates.extend(
        [
            Path(r"C:\Windows\Fonts\arial.ttf"),
            Path(r"C:\Windows\Fonts\consola.ttf"),
            Path(r"C:\Windows\Fonts\cour.ttf"),
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def _new_page(document_title: str) -> tuple[Image.Image, ImageDraw.ImageDraw, int]:
    image = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
    draw = ImageDraw.Draw(image)
    title_font = _load_font(TITLE_FONT_SIZE, bold=True)
    draw.text((MARGIN_X, MARGIN_Y), document_title, font=title_font, fill=TEXT_COLOR)
    y_pos = MARGIN_Y + TITLE_FONT_SIZE + 26
    return image, draw, y_pos


def _normalize_dataframe(content: pd.DataFrame) -> pd.DataFrame:
    if content.empty:
        return pd.DataFrame({"Sem dados": ["Sem dados."]})
    return content.fillna("").astype(str)


def _measure_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> float:
    return draw.textlength(str(text), font=font)


def _truncate_text(draw: ImageDraw.ImageDraw, text: str, max_width: int, font: ImageFont.ImageFont) -> str:
    text = str(text)
    if _measure_text(draw, text, font) <= max_width:
        return text
    ellipsis = "..."
    low, high = 0, len(text)
    best = ellipsis
    while low <= high:
        mid = (low + high) // 2
        candidate = text[:mid].rstrip() + ellipsis
        if _measure_text(draw, candidate, font) <= max_width:
            best = candidate
            low = mid + 1
        else:
            high = mid - 1
    return best


def _compute_column_widths(
    draw: ImageDraw.ImageDraw,
    df: pd.DataFrame,
    table_width: int,
    header_font: ImageFont.ImageFont,
    body_font: ImageFont.ImageFont,
) -> list[int]:
    widths: list[int] = []
    for column in df.columns:
        header_width = _measure_text(draw, column, header_font)
        sample_width = header_width
        for value in df[column].tolist():
            sample_width = max(sample_width, _measure_text(draw, value, body_font))
        widths.append(int(sample_width + (CELL_PADDING_X * 2)))

    widths = [max(width, MIN_COL_WIDTH) for width in widths]
    total_width = sum(widths)
    if total_width <= table_width:
        return widths

    scale = table_width / total_width
    scaled = [max(MIN_COL_WIDTH, int(width * scale)) for width in widths]
    total_scaled = sum(scaled)

    if total_scaled <= table_width:
        remainder = table_width - total_scaled
        idx = 0
        while remainder > 0 and scaled:
            scaled[idx % len(scaled)] += 1
            remainder -= 1
            idx += 1
        return scaled

    # When min widths still overflow, distribute the available width as evenly as possible.
    base = max(int(table_width / max(len(widths), 1)), 1)
    even = [base for _ in widths]
    remainder = table_width - sum(even)
    idx = 0
    while remainder > 0 and even:
        even[idx % len(even)] += 1
        remainder -= 1
        idx += 1
    return even


def _render_dataframe_pages(
    document_title: str,
    section_title: str,
    df: pd.DataFrame,
) -> list[Image.Image]:
    df = _normalize_dataframe(df)
    header_font = _load_font(HEADER_FONT_SIZE, bold=True)
    body_font = _load_font(BODY_FONT_SIZE)
    section_font = _load_font(SECTION_FONT_SIZE, bold=True)

    pages: list[Image.Image] = []
    image, draw, y_pos = _new_page(document_title)
    available_width = PAGE_WIDTH - (MARGIN_X * 2)
    col_widths = _compute_column_widths(draw, df, available_width, header_font, body_font)

    def draw_section_header(draw_obj: ImageDraw.ImageDraw, y_value: int, title: str) -> int:
        draw_obj.text((MARGIN_X, y_value), title, font=section_font, fill=TEXT_COLOR)
        return y_value + SECTION_FONT_SIZE + SECTION_GAP

    def draw_table_header(draw_obj: ImageDraw.ImageDraw, y_value: int) -> int:
        x_value = MARGIN_X
        for width, column in zip(col_widths, df.columns):
            draw_obj.rectangle(
                [x_value, y_value, x_value + width, y_value + HEADER_HEIGHT],
                outline=GRID_COLOR,
                fill=HEADER_FILL,
                width=1,
            )
            draw_obj.text(
                (x_value + CELL_PADDING_X, y_value + 10),
                _truncate_text(draw_obj, column, width - (CELL_PADDING_X * 2), header_font),
                font=header_font,
                fill=TEXT_COLOR,
            )
            x_value += width
        return y_value + HEADER_HEIGHT

    y_pos = draw_section_header(draw, y_pos, section_title)
    y_pos = draw_table_header(draw, y_pos)

    for row_index in range(len(df)):
        if y_pos + ROW_HEIGHT > PAGE_HEIGHT - MARGIN_Y:
            pages.append(image)
            image, draw, y_pos = _new_page(document_title)
            continued_title = f"{section_title} (continuação)"
            y_pos = draw_section_header(draw, y_pos, continued_title)
            y_pos = draw_table_header(draw, y_pos)

        x_value = MARGIN_X
        for width, column in zip(col_widths, df.columns):
            draw.rectangle(
                [x_value, y_pos, x_value + width, y_pos + ROW_HEIGHT],
                outline=GRID_COLOR,
                fill="white",
                width=1,
            )
            value = df.iloc[row_index][column]
            draw.text(
                (x_value + CELL_PADDING_X, y_pos + 8),
                _truncate_text(draw, value, width - (CELL_PADDING_X * 2), body_font),
                font=body_font,
                fill=TEXT_COLOR,
            )
            x_value += width
        y_pos += ROW_HEIGHT

    pages.append(image)
    return pages


def _render_text_pages(document_title: str, section_title: str, lines: Iterable[str]) -> list[Image.Image]:
    body_font = _load_font(BODY_FONT_SIZE)
    section_font = _load_font(SECTION_FONT_SIZE, bold=True)
    line_height = BODY_FONT_SIZE + 8

    pages: list[Image.Image] = []
    image, draw, y_pos = _new_page(document_title)
    draw.text((MARGIN_X, y_pos), section_title, font=section_font, fill=TEXT_COLOR)
    y_pos += SECTION_FONT_SIZE + SECTION_GAP

    for line in lines:
        if y_pos + line_height > PAGE_HEIGHT - MARGIN_Y:
            pages.append(image)
            image, draw, y_pos = _new_page(document_title)
            draw.text((MARGIN_X, y_pos), f"{section_title} (continuação)", font=section_font, fill=TEXT_COLOR)
            y_pos += SECTION_FONT_SIZE + SECTION_GAP
        draw.text((MARGIN_X, y_pos), str(line), font=body_font, fill=TEXT_COLOR)
        y_pos += line_height

    pages.append(image)
    return pages


def build_reports_pdf(document_title: str, sections: list[tuple[str, object]]) -> bytes:
    pages: list[Image.Image] = []

    for section_title, content in sections:
        if isinstance(content, pd.DataFrame):
            pages.extend(_render_dataframe_pages(document_title, section_title, content))
        else:
            if content is None:
                lines = ["Sem dados."]
            else:
                lines = str(content).splitlines() or ["Sem dados."]
            pages.extend(_render_text_pages(document_title, section_title, lines))

    if not pages:
        image, _, _ = _new_page(document_title)
        pages = [image]

    buffer = BytesIO()
    first_page, *other_pages = pages
    first_page.save(buffer, format="PDF", resolution=150.0, save_all=True, append_images=other_pages)
    buffer.seek(0)
    return buffer.getvalue()
