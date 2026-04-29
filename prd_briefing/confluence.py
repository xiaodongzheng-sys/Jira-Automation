from __future__ import annotations

import base64
import re
from dataclasses import dataclass, field
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from .storage import BriefingStore


@dataclass
class ParsedSection:
    title: str
    section_path: str
    content: str
    html_content: str = ""
    image_refs: list[str] = field(default_factory=list)


@dataclass
class IngestedConfluencePage:
    page_id: str
    title: str
    source_url: str
    updated_at: str
    language: str
    sections: list[ParsedSection]


@dataclass
class ResolvedPageRef:
    base_url: str
    source_url: str
    page_id: str | None = None
    space_key: str | None = None
    title_hint: str | None = None


class ConfluenceConnector:
    def __init__(
        self,
        *,
        base_url: str | None,
        email: str | None,
        api_token: str | None,
        bearer_token: str | None,
        store: BriefingStore,
    ) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self.email = (email or "").strip()
        self.api_token = (api_token or "").strip()
        self.bearer_token = (bearer_token or "").strip()
        self.store = store

    def ingest_page(self, page_ref: str, session_id: str) -> IngestedConfluencePage:
        resolved = self._resolve_page(page_ref)
        payload = self._fetch_page_payload(resolved)
        html = payload.get("body", {}).get("export_view", {}).get("value", "")
        page_id = str(payload.get("id") or resolved.page_id or "")
        title = payload.get("title") or f"Confluence Page {page_id or 'unknown'}"
        updated_at = payload.get("version", {}).get("when") or ""

        sections = self._parse_sections(
            html=html,
            base_url=resolved.base_url,
            source_url=resolved.source_url,
            session_id=session_id,
        )
        return IngestedConfluencePage(
            page_id=page_id,
            title=str(title),
            source_url=resolved.source_url,
            updated_at=str(updated_at),
            language="en",
            sections=sections,
        )

    def _resolve_page(self, page_ref: str) -> ResolvedPageRef:
        value = page_ref.strip()
        if value.isdigit():
            if not self.base_url:
                raise ValueError("A raw Confluence page ID requires CONFLUENCE_BASE_URL to be configured.")
            return ResolvedPageRef(
                base_url=self.base_url,
                page_id=value,
                source_url=f"{self.base_url}/pages/viewpage.action?pageId={value}",
            )
        parsed = urlparse(value)
        if re.match(r"^/x/[^/]+/?$", parsed.path):
            return self._resolve_short_link(value)
        return self._resolve_parsed_page(value, parsed)

    def _resolve_short_link(self, page_ref: str) -> ResolvedPageRef:
        response = self._request(page_ref, accept="text/html", allow_redirects=True)
        resolved_url = str(getattr(response, "url", "") or "").strip()
        if not resolved_url or resolved_url == page_ref:
            raise ValueError("Could not resolve Confluence short link to a page URL.")
        return self._resolve_parsed_page(resolved_url, urlparse(resolved_url))

    def _resolve_parsed_page(self, value: str, parsed: Any) -> ResolvedPageRef:
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        query = parse_qs(parsed.query)
        query_page_id = query.get("pageId", [None])[0]
        page_id = query_page_id
        if not page_id:
            match = re.search(r"/pages/(\d+)", parsed.path)
            if match:
                page_id = match.group(1)
        if page_id:
            return ResolvedPageRef(base_url=base_url, page_id=page_id, source_url=value)
        query_space_key = query.get("spaceKey", [None])[0]
        query_title = query.get("title", [None])[0]
        if query_space_key and query_title:
            return ResolvedPageRef(
                base_url=base_url,
                source_url=value,
                space_key=query_space_key,
                title_hint=self._normalize_display_title(query_title),
            )
        display_match = re.search(r"^/display/([^/]+)/(.+)$", parsed.path)
        if display_match:
            return ResolvedPageRef(
                base_url=base_url,
                source_url=value,
                space_key=display_match.group(1),
                title_hint=self._normalize_display_title(display_match.group(2)),
            )
        raise ValueError("Could not find a supported Confluence page reference in the provided URL.")

    def _fetch_page_payload(self, resolved: ResolvedPageRef) -> dict:
        if resolved.page_id:
            last_error: Exception | None = None
            for rest_base in self._rest_api_candidates(resolved.base_url):
                try:
                    response = self._request(
                        f"{rest_base}/content/{resolved.page_id}",
                        params={"expand": "body.export_view,version"},
                    )
                    return response.json()
                except Exception as error:  # noqa: BLE001
                    last_error = error
            raise RuntimeError(f"Could not fetch Confluence page by ID. Last error: {last_error}")

        if resolved.space_key and resolved.title_hint:
            last_error = None
            for rest_base in self._rest_api_candidates(resolved.base_url):
                try:
                    response = self._request(
                        f"{rest_base}/content",
                        params={
                            "spaceKey": resolved.space_key,
                            "title": resolved.title_hint,
                            "expand": "body.export_view,version",
                        },
                    )
                    payload = response.json()
                    results = payload.get("results") or []
                    if results:
                        return results[0]
                except Exception as error:  # noqa: BLE001
                    last_error = error
            renamed = self._resolve_renamed_display_page(resolved)
            if renamed:
                resolved.source_url = renamed.source_url
                return self._fetch_page_payload(renamed)
            similar = self._search_similar_display_page(resolved)
            if similar:
                resolved.source_url = similar.source_url
                return self._fetch_page_payload(similar)
            raise RuntimeError(f"Could not resolve Confluence display URL to a page. Last error: {last_error}")

        raise ValueError("Confluence page reference was missing both page ID and display title.")

    def _resolve_renamed_display_page(self, resolved: ResolvedPageRef) -> ResolvedPageRef | None:
        try:
            response = self._request(resolved.source_url, accept="text/html")
        except Exception:  # noqa: BLE001 - old display URLs may still be inaccessible through API auth.
            return None
        soup = BeautifulSoup(response.text or "", "html.parser")
        for anchor in soup.find_all("a", href=True):
            text = self._clean_text(anchor.get_text(" ", strip=True))
            href = str(anchor.get("href") or "").strip()
            if not text or not href:
                continue
            if resolved.title_hint and text.casefold() == resolved.title_hint.casefold():
                continue
            candidate_url = urljoin(resolved.base_url, href)
            try:
                candidate = self._resolve_parsed_page(candidate_url, urlparse(candidate_url))
            except ValueError:
                continue
            if candidate.page_id or candidate.space_key:
                return candidate
        return None

    def _search_similar_display_page(self, resolved: ResolvedPageRef) -> ResolvedPageRef | None:
        if not resolved.space_key or not resolved.title_hint:
            return None
        for phrase in self._title_search_phrases(resolved.title_hint):
            cql = f'space = "{self._escape_cql_value(resolved.space_key)}" and type = page and title ~ "{self._escape_cql_value(phrase)}"'
            for rest_base in self._rest_api_candidates(resolved.base_url):
                try:
                    response = self._request(
                        f"{rest_base}/search",
                        params={"cql": cql, "limit": 5, "expand": "content.version"},
                    )
                    payload = response.json()
                except Exception:  # noqa: BLE001 - try the next phrase/candidate.
                    continue
                for item in payload.get("results") or []:
                    content = item.get("content") if isinstance(item, dict) else None
                    if not isinstance(content, dict):
                        continue
                    page_id = str(content.get("id") or "").strip()
                    title = str(content.get("title") or "").strip()
                    if not page_id and not title:
                        continue
                    return ResolvedPageRef(
                        base_url=resolved.base_url,
                        source_url=urljoin(resolved.base_url, str(item.get("url") or item.get("link") or resolved.source_url)),
                        page_id=page_id or None,
                        space_key=resolved.space_key if not page_id else None,
                        title_hint=title if not page_id else None,
                    )
        return None

    @staticmethod
    def _title_search_phrases(title: str) -> list[str]:
        cleaned = re.sub(r"\[[^\]]+\]", " ", title)
        words = re.findall(r"[A-Za-z0-9]+", cleaned)
        meaningful = [
            word
            for word in words
            if len(word) > 1 and word.casefold() not in {"prd", "table", "page", "doc", "document", "requirement"}
        ]
        phrases = []
        if cleaned.strip():
            phrases.append(cleaned.strip())
        if len(meaningful) >= 3:
            phrases.append(" ".join(meaningful[:4]))
        if len(meaningful) >= 4:
            phrases.append(" ".join(meaningful[1:5]))
        phrases.append(title)
        return list(dict.fromkeys(phrase for phrase in phrases if phrase))

    @staticmethod
    def _escape_cql_value(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _rest_api_candidates(self, base_url: str) -> list[str]:
        candidates = []
        if "/wiki" in base_url:
            candidates.append(f"{base_url.rstrip('/')}/rest/api")
        candidates.append(f"{base_url.rstrip('/')}/rest/api")
        candidates.append(f"{base_url.rstrip('/')}/wiki/rest/api")
        return list(dict.fromkeys(candidates))

    def _parse_sections(self, *, html: str, base_url: str, source_url: str, session_id: str) -> list[ParsedSection]:
        soup = BeautifulSoup(html, "html.parser")
        wrapper = soup.body or soup
        self._drop_struck_content(wrapper)
        self._drop_marker_only_blocks(wrapper)
        sections: list[ParsedSection] = []
        current_title = "Overview"
        current_lines: list[str] = []
        current_blocks: list[str] = []
        current_images: list[str] = []

        def flush_section() -> None:
            body = "\n".join(self._dedupe_lines(current_lines)).strip()
            html_body = "\n".join(block for block in current_blocks if block).strip()
            if not body and not current_images and not html_body:
                return
            sections.append(
                ParsedSection(
                    title=current_title,
                    section_path=current_title,
                    content=body,
                    html_content=html_body,
                    image_refs=list(dict.fromkeys(current_images)),
                )
            )
            current_lines.clear()
            current_blocks.clear()
            current_images.clear()

        for node in wrapper.children:
            if isinstance(node, NavigableString):
                continue
            if not isinstance(node, Tag):
                continue
            if self._is_toc_block(node):
                continue
            if node.name in {"h1", "h2", "h3", "h4"}:
                heading = self._clean_text(node.get_text(" ", strip=True))
                if not heading:
                    continue
                flush_section()
                current_title = heading
                continue
            lines, blocks, images = self._extract_block_content(node, base_url=base_url)
            current_lines.extend(lines)
            current_blocks.extend(blocks)
            current_images.extend(images)

        flush_section()
        filtered = [section for section in sections if section.content.strip() or section.image_refs]
        return filtered or [
            ParsedSection(
                title="Overview",
                section_path="Overview",
                content=self._clean_text(wrapper.get_text(" ", strip=True)),
                html_content="".join(str(child) for child in wrapper.contents).strip(),
                image_refs=[],
            )
        ]

    def _resolve_image_ref(self, src: str, *, base_url: str) -> str:
        return urljoin(base_url, src)

    def _extract_block_content(self, node: Tag, *, base_url: str) -> tuple[list[str], list[str], list[str]]:
        if self._is_struck_node(node) or self._is_toc_block(node) or node.name in {"style", "script"}:
            return [], [], []

        if node.name == "img":
            src = (node.get("src") or "").strip()
            image_ref = self._resolve_image_ref(src, base_url=base_url) if src else None
            block = self._render_html_fragment(node, base_url=base_url)
            return [], ([block] if block else []), ([image_ref] if image_ref else [])

        if node.name in {"ul", "ol"}:
            lines = []
            images = []
            for li in node.find_all("li", recursive=False):
                text = self._clean_text(li.get_text(" ", strip=True))
                if text:
                    lines.append(text)
                for image in li.find_all("img"):
                    src = (image.get("src") or "").strip()
                    if src:
                        images.append(self._resolve_image_ref(src, base_url=base_url))
            block = self._render_html_fragment(node, base_url=base_url)
            return lines, ([block] if block else []), images

        if node.name == "table" or "table-wrap" in (node.get("class") or []):
            table = node if node.name == "table" else node.find("table")
            if table and not self._table_has_displayable_content(table):
                return [], [], []
            block = self._render_html_fragment(node, base_url=base_url)
            return (self._extract_table_lines(table) if table else []), ([block] if block else []), []

        if node.name in {"p", "blockquote", "pre"}:
            text = self._clean_text(node.get_text(" ", strip=True))
            images = []
            for image in node.find_all("img"):
                src = (image.get("src") or "").strip()
                if src:
                    images.append(self._resolve_image_ref(src, base_url=base_url))
            block = self._render_html_fragment(node, base_url=base_url)
            return ([text] if text else []), ([block] if block else []), images

        lines: list[str] = []
        blocks: list[str] = []
        images: list[str] = []
        for child in node.children:
            if isinstance(child, NavigableString):
                text = self._clean_text(str(child))
                if text:
                    lines.append(text)
                continue
            if not isinstance(child, Tag):
                continue
            child_lines, child_blocks, child_images = self._extract_block_content(child, base_url=base_url)
            lines.extend(child_lines)
            blocks.extend(child_blocks)
            images.extend(child_images)
        return lines, blocks, images

    def _extract_table_lines(self, table: Tag) -> list[str]:
        if not self._table_has_displayable_content(table):
            return []
        rows: list[list[str]] = []
        for tr in table.find_all("tr"):
            cells_for_display = tr.find_all(["th", "td"], recursive=False)
            if cells_for_display and all(not self._cell_has_displayable_content(cell) for cell in cells_for_display):
                continue
            cells = [
                self._clean_text(cell.get_text(" ", strip=True))
                for cell in cells_for_display
            ]
            cleaned = [cell for cell in cells if cell]
            if cleaned:
                rows.append(cleaned)
        if not rows:
            return []
        if len(rows) == 1:
            return [" | ".join(rows[0])]
        header = rows[0]
        rendered = []
        for row in rows[1:]:
            if len(row) == len(header) and len(header) > 1:
                rendered.append("; ".join(f"{header[index]}: {value}" for index, value in enumerate(row) if value))
            else:
                rendered.append(" | ".join(row))
        return rendered

    def _dedupe_lines(self, lines: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for raw_line in lines:
            line = self._clean_text(raw_line)
            if not line:
                continue
            key = line.casefold()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(line)
        return deduped

    def _render_html_fragment(self, node: Tag, *, base_url: str) -> str:
        fragment = BeautifulSoup(str(node), "html.parser")
        for unwanted in fragment.find_all(["script", "style"]):
            unwanted.decompose()
        for toc in fragment.find_all(class_="toc-macro"):
            toc.decompose()
        self._drop_struck_content(fragment)
        self._drop_marker_only_blocks(fragment)
        self._drop_empty_tables(fragment)
        for image in fragment.find_all("img"):
            src = (image.get("src") or "").strip()
            if src:
                resolved_src = self._resolve_image_ref(src, base_url=base_url)
                image["src"] = f"/prd-briefing/image-proxy?src={quote(resolved_src, safe='')}"
            image["loading"] = "lazy"
            image["decoding"] = "async"
        for anchor in fragment.find_all("a"):
            href = (anchor.get("href") or "").strip()
            if href:
                anchor["href"] = urljoin(base_url, href)
            anchor["target"] = "_blank"
            anchor["rel"] = "noreferrer"
        body = fragment.body or fragment
        html = "".join(str(child) for child in body.contents).strip()
        return html

    def _is_toc_block(self, node: Tag) -> bool:
        classes = node.get("class") or []
        if "toc-macro" in classes or any(str(item).startswith("rbtoc") for item in classes):
            return True
        if node.find(class_="toc-macro") is not None:
            return True
        text = self._clean_text(node.get_text(" ", strip=True))
        return bool(node.name == "h1" and "1. Project Management" in text and "1.1 Version Control" in text and "2. Introduction" in text)

    def _drop_struck_content(self, node: Tag | BeautifulSoup) -> None:
        for struck in list(node.find_all(self._is_struck_node)):
            struck.decompose()

    def _drop_empty_tables(self, node: Tag | BeautifulSoup) -> None:
        for table in list(node.find_all("table")):
            if not self._table_has_displayable_content(table):
                wrapper = table.find_parent(class_="table-wrap")
                if wrapper is not None:
                    wrapper.decompose()
                else:
                    table.decompose()

    def _drop_marker_only_blocks(self, node: Tag | BeautifulSoup) -> None:
        for row in list(node.find_all("tr")):
            cells = row.find_all(["td", "th"], recursive=False)
            if cells and all(not self._cell_has_displayable_content(cell) for cell in cells):
                row.decompose()
        for item in list(node.find_all(["p", "li"])):
            if item.find(["img", "table"]) is not None:
                continue
            if not self._is_meaningful_cell_text(item.get_text(" ", strip=True)):
                item.decompose()

    def _table_has_displayable_content(self, table: Tag) -> bool:
        data_cells = table.find_all("td")
        cells = data_cells or table.find_all(["td", "th"])
        return any(self._cell_has_displayable_content(cell) for cell in cells)

    def _cell_has_displayable_content(self, cell: Tag) -> bool:
        if cell.find("img") is not None:
            return True
        return self._is_meaningful_cell_text(cell.get_text(" ", strip=True))

    def _is_meaningful_cell_text(self, value: str) -> bool:
        text = self._clean_text(value)
        if not text:
            return False
        text = re.sub(r"[\s\u00a0]+", "", text)
        if not text:
            return False
        if re.fullmatch(r"(?:[0-9]+[.)、]?)+", text):
            return False
        if re.fullmatch(r"[a-zA-Z][.)、]?", text):
            return False
        if re.fullmatch(r"[ivxlcdmIVXLCDM]+[.)、]", text):
            return False
        if re.fullmatch(r"[•·▪▫◦○oO]+", text):
            return False
        return bool(re.search(r"[\w\u4e00-\u9fff]", text))

    @staticmethod
    def _is_struck_node(node: Tag) -> bool:
        if not isinstance(node, Tag):
            return False
        if node.name in {"s", "strike", "del"}:
            return True
        style = re.sub(r"\s+", "", str(node.get("style") or "").casefold())
        return "text-decoration:line-through" in style or "text-decoration-line:line-through" in style

    def _request(self, url: str, *, accept: str = "application/json", **kwargs: Any) -> requests.Response:
        last_response: requests.Response | None = None
        last_error: Exception | None = None
        for headers in self._headers_candidates(accept=accept):
            try:
                response = requests.get(url, headers=headers, timeout=60, **kwargs)
                if response.status_code == 401:
                    last_response = response
                    continue
                response.raise_for_status()
                return response
            except Exception as error:  # noqa: BLE001
                last_error = error
        if last_response is not None:
            last_response.raise_for_status()
        if last_error is not None:
            raise last_error
        raise RuntimeError("Confluence request did not return a response.")

    def _headers_candidates(self, *, accept: str = "application/json") -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        base_headers = {"Accept": accept}
        if self.bearer_token:
            candidates.append({**base_headers, "Authorization": f"Bearer {self.bearer_token}"})
        if self.api_token and self.email:
            # Self-hosted Confluence often uses PATs over Bearer even when users describe them as API tokens.
            candidates.append({**base_headers, "Authorization": f"Bearer {self.api_token}"})
            encoded = base64.b64encode(f"{self.email}:{self.api_token}".encode("utf-8")).decode("utf-8")
            candidates.append({**base_headers, "Authorization": f"Basic {encoded}"})
        if not candidates:
            candidates.append(base_headers)
        return candidates

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", unescape(value)).strip()

    @staticmethod
    def _normalize_display_title(value: str) -> str:
        return unquote_plus(value).strip()
