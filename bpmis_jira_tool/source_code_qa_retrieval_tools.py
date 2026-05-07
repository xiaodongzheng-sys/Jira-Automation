"""Index search, targeted retrieval, tool-loop, and trace helpers for Source Code QA."""
from __future__ import annotations

import functools


def _bind_source_code_qa_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _search_repo_index(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    tokens: list[str],
    *,
    question: str,
    focus_terms: list[str] | None = None,
    trace_stage: str = "direct",
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    index_path = self._index_path(repo_path)
    matches: list[dict[str, Any]] = []
    repo_score = self._repo_match_score(entry, tokens)
    trace_stage_bonus = 90 if trace_stage == "two_hop" or trace_stage == "query_decomposition" or trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX) or trace_stage.startswith("agent_trace") or trace_stage.startswith("agent_plan") or trace_stage == QUALITY_GATE_TRACE_STAGE else 0
    normalized_focus_terms = [term.lower() for term in (focus_terms or []) if term]
    query_terms = list(dict.fromkeys([*tokens, *normalized_focus_terms]))
    large_index = self._is_large_index_file(index_path)
    structure_term_limit = 8 if trace_stage == "direct" or not large_index else 2
    structure_query_terms = self._structure_lookup_query_terms(
        tokens,
        normalized_focus_terms,
        large_index=large_index,
        limit=structure_term_limit,
    )
    intent = self._question_retrieval_features(question, request_cache=request_cache).get("intent") or {}
    simple_intent = (
        any((intent or {}).get(key) for key in ("rule_logic", "api", "config"))
        and not any(
            (intent or {}).get(key)
            for key in (
                "data_source",
                "module_dependency",
                "static_qa",
                "impact_analysis",
                "test_coverage",
                "operational_boundary",
            )
        )
    )
    file_hits: dict[str, dict[str, Any]] = {}
    with sqlite3.connect(index_path) as connection:
        connection.row_factory = sqlite3.Row
        index_rows = self._targeted_index_rows(
            connection,
            index_path,
            tokens=tokens,
            focus_terms=normalized_focus_terms,
            intent=intent,
            request_cache=request_cache,
            structure_term_limit=structure_term_limit,
        )
        file_rows = index_rows["files"]
        file_rows_by_path = index_rows["files_by_path"]
        line_rows = index_rows["lines"]
        file_symbols_by_path = index_rows.get("file_symbols_by_path") or {}
        line_symbols_by_key = index_rows.get("line_symbols_by_key") or {}
        for file_row in file_rows:
            path_text = str(file_row["lower_path"] or "")
            file_symbols = file_symbols_by_path.get(str(file_row["path"]))
            if file_symbols is None:
                file_symbols = set(json.loads(file_row["symbols"] or "[]"))
            path_score = sum(10 for token in tokens if token in path_text)
            if intent.get("config") and path_text.endswith((".properties", ".yaml", ".yml", ".conf", ".toml")):
                path_score += 70
            if intent.get("module_dependency") and (
                path_text.endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json"))
                or ".gradle" in path_text
            ):
                path_score += 76
            if normalized_focus_terms and any(hint in path_text for hint in DEPENDENCY_PATH_HINTS):
                path_score += 18
            symbol_score = sum(16 for token in tokens if token in file_symbols)
            symbol_score += sum(20 for term in normalized_focus_terms if term in file_symbols)
            if path_score or symbol_score or repo_score:
                file_hits[file_row["path"]] = {
                    "path_text": path_text,
                    "file_symbols": file_symbols,
                    "path_score": path_score,
                    "symbol_score": symbol_score,
                    "best_line": 1,
                    "best_score": path_score + symbol_score + repo_score + trace_stage_bonus,
                    "structure_hits": [],
                }
        for term in structure_query_terms:
            if len(term) < 3:
                continue
            for row in self._cached_structure_like_rows(
                connection,
                index_path,
                table="definitions",
                lower_column="lower_name",
                term=term,
                limit=40,
                request_cache=request_cache,
            ):
                file_path = str(row["file_path"])
                hit = file_hits.setdefault(
                    file_path,
                    {
                        "path_text": file_path.lower(),
                        "file_symbols": set(),
                        "path_score": 0,
                        "symbol_score": 0,
                        "best_line": int(row["line_no"]),
                        "best_score": 0,
                        "structure_hits": [],
                    },
                )
                boost = 72 if str(row["lower_name"]) == term else 42
                score = boost + repo_score + trace_stage_bonus
                if score > hit.get("best_score", 0):
                    hit["best_line"] = int(row["line_no"])
                    hit["best_score"] = score
                hit["structure_hits"].append(f"{row['kind']} definition {row['name']}")
            for row in self._cached_structure_like_rows(
                connection,
                index_path,
                table="references_index",
                lower_column="lower_target",
                term=term,
                limit=60,
                request_cache=request_cache,
            ):
                file_path = str(row["file_path"])
                hit = file_hits.setdefault(
                    file_path,
                    {
                        "path_text": file_path.lower(),
                        "file_symbols": set(),
                        "path_score": 0,
                        "symbol_score": 0,
                        "best_line": int(row["line_no"]),
                        "best_score": 0,
                        "structure_hits": [],
                    },
                )
                boost = 58 if str(row["lower_target"]) == term else 32
                if str(row["kind"]) in {"sql_table", "route"}:
                    boost += 18
                if intent.get("config") and str(row["kind"]) in {"config_value", "http_endpoint", "downstream_api"}:
                    boost += 34
                if intent.get("module_dependency") and str(row["kind"]) == "module_dependency":
                    boost += 36
                score = boost + repo_score + trace_stage_bonus
                if score > hit.get("best_score", 0):
                    hit["best_line"] = int(row["line_no"])
                    hit["best_score"] = score
                hit["structure_hits"].append(f"{row['kind']} reference {row['target']}")
        for row in self._fts_search_rows(
            connection,
            tokens,
            normalized_focus_terms,
            index_path=index_path,
            request_cache=request_cache,
        ):
            file_path = str(row["file_path"])
            hit = file_hits.setdefault(
                file_path,
                {
                    "path_text": file_path.lower(),
                    "file_symbols": set(),
                    "path_score": 0,
                    "symbol_score": 0,
                    "best_line": int(row["line_no"]),
                    "best_score": 0,
                    "structure_hits": [],
                },
            )
            score = max(12, int(80 - min(float(row["rank"]), 60))) + repo_score + trace_stage_bonus
            if score > hit.get("best_score", 0):
                hit["best_line"] = int(row["line_no"])
                hit["best_score"] = score
            hit["structure_hits"].append("bm25 content match")
        for row in line_rows:
            lower_text = str(row["lower_text"] or "")
            file_path = str(row["file_path"])
            file_hit = file_hits.get(file_path)
            if file_hit is None:
                file_row = file_rows_by_path.get(file_path)
                if file_row is None:
                    continue
                file_symbols = file_symbols_by_path.get(file_path)
                if file_symbols is None:
                    file_symbols = set(json.loads(file_row["symbols"] or "[]"))
                file_hit = {
                    "path_text": str(file_row["lower_path"] or ""),
                    "file_symbols": file_symbols,
                    "path_score": 0,
                    "symbol_score": 0,
                    "best_line": 1,
                    "best_score": 0,
                    "structure_hits": [],
                }
            line_no = int(row["line_no"])
            line_symbols = line_symbols_by_key.get((file_path, line_no))
            if line_symbols is None:
                line_symbols = set(json.loads(row["symbols"] or "[]"))
            score = sum(3 + min(lower_text.count(token), 3) for token in tokens if token in lower_text)
            score += sum(12 for token in tokens if token in line_symbols)
            score += sum(16 for term in normalized_focus_terms if term in line_symbols)
            score += sum(45 for term in normalized_focus_terms if term in lower_text)
            if int(row["is_declaration"] or 0):
                score += sum(8 for token in tokens if token in lower_text or token in line_symbols)
                score += sum(10 for term in normalized_focus_terms if term in lower_text or term in line_symbols)
            if int(row["has_pathish"] or 0):
                score += sum(6 for token in tokens if token in lower_text)
            if intent.get("config") and file_path.lower().endswith((".properties", ".yaml", ".yml", ".conf", ".toml")):
                score += 45
            if intent.get("module_dependency") and file_path.lower().endswith(("pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "package.json")):
                score += 48
            if score:
                score += (
                    file_hit["path_score"]
                    + file_hit["symbol_score"]
                    + repo_score
                    + self._keyword_proximity_bonus(lower_text, tokens)
                    + self._hybrid_query_bonus(question, lower_text, line_symbols, intent=intent)
                    + trace_stage_bonus
                )
                if score > file_hit.get("best_score", 0):
                    file_hit.update(
                        {
                            "best_line": line_no,
                            "best_score": score,
                        }
                    )
                file_hits[file_path] = file_hit
        matches.extend(
            self._semantic_chunk_matches(
                connection,
                entry=entry,
                tokens=tokens,
                question=question,
                focus_terms=normalized_focus_terms,
                trace_stage=trace_stage,
                repo_score=repo_score,
                trace_stage_bonus=trace_stage_bonus,
                rows=index_rows.get("semantic_chunks"),
                intent=intent,
            )
        )
        matches.extend(
            self._persistent_index_matches_from_hits(
                connection,
                index_path,
                entry=entry,
                tokens=tokens,
                question=question,
                focus_terms=normalized_focus_terms,
                trace_stage=trace_stage,
                simple_intent=simple_intent,
                file_hits=file_hits,
                request_cache=request_cache,
            )
        )
    return matches


def _persistent_index_matches_from_hits(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    *,
    entry: RepositoryEntry,
    tokens: list[str],
    question: str,
    focus_terms: list[str],
    trace_stage: str,
    simple_intent: bool,
    file_hits: dict[str, dict[str, Any]],
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if simple_intent and trace_stage == "direct" and len(file_hits) > 60:
        file_hits = dict(
            sorted(
                file_hits.items(),
                key=lambda item: int(item[1].get("best_score") or 0),
                reverse=True,
            )[:60]
        )
        self._increment_retrieval_stat(request_cache, "simple_file_hit_prunes")
    matches: list[dict[str, Any]] = []
    for relative_path, hit in file_hits.items():
        if not hit.get("best_score"):
            continue
        lines = self._cached_file_lines(
            connection,
            index_path,
            str(relative_path),
            request_cache=request_cache,
        )
        if not lines:
            continue
        start, end = self._best_snippet_window(lines, int(hit["best_line"]))
        snippet = "\n".join(lines[start - 1 : end]).strip()
        reason = self._match_reason(
            tokens,
            hit["path_text"],
            snippet,
            file_symbols=hit["file_symbols"],
            question=question,
            focus_terms=focus_terms,
            trace_stage=trace_stage,
        )
        structure_hits = list(dict.fromkeys(hit.get("structure_hits") or []))
        if structure_hits:
            reason = f"{reason}; structure matched: {', '.join(structure_hits[:4])}"
        matches.append(
            {
                "repo": entry.display_name,
                "path": relative_path,
                "line_start": start,
                "line_end": end,
                "score": hit["best_score"],
                "snippet": snippet[:2400],
                "reason": reason,
                "trace_stage": trace_stage,
                "retrieval": "persistent_index",
            }
        )
    return matches


def _cached_index_rows(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    *,
    request_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cache_key = self._index_fingerprint(index_path)
    if request_cache is not None:
        rows_cache = request_cache.setdefault("index_rows", {})
        cached = rows_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "index_rows_hits")
            return cached
    self._increment_retrieval_stat(request_cache, "index_rows_misses")
    file_rows = connection.execute("select * from files").fetchall()
    line_rows = connection.execute(
        "select * from lines order by file_path, line_no limit ?",
        (MAX_CACHED_INDEX_LINES,),
    ).fetchall()
    files_by_path = {str(row["path"]): row for row in file_rows}
    file_symbols_by_path = {
        str(row["path"]): set(json.loads(row["symbols"] or "[]"))
        for row in file_rows
    }
    lines_by_path: dict[str, list[tuple[int, str]]] = {}
    line_symbols_by_key: dict[tuple[str, int], set[str]] = {}
    for row in line_rows:
        file_path = str(row["file_path"])
        line_no = int(row["line_no"])
        lines_by_path.setdefault(file_path, []).append((line_no, str(row["line_text"])))
        line_symbols_by_key[(file_path, line_no)] = set(json.loads(row["symbols"] or "[]"))
    normalized_lines_by_path = {
        file_path: [line_text for _, line_text in sorted(rows, key=lambda item: item[0])]
        for file_path, rows in lines_by_path.items()
    }
    semantic_rows: list[sqlite3.Row] | None = None
    semantic_chunks: list[dict[str, Any]] | None = None
    if self.semantic_index_enabled:
        try:
            semantic_rows = connection.execute(
                "select * from semantic_chunks order by file_path, start_line limit ?",
                (MAX_CACHED_SEMANTIC_CHUNKS,),
            ).fetchall()
            semantic_chunks = [
                {
                    "chunk_id": str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}"),
                    "file_path": str(row["file_path"] or ""),
                    "start_line": int(row["start_line"] or 1),
                    "end_line": int(row["end_line"] or row["start_line"] or 1),
                    "chunk_text": str(row["chunk_text"] or ""),
                    "lower_text": str(row["lower_text"] or ""),
                    "tokens_set": set(json.loads(row["tokens"] or "[]")),
                    "symbols_set": set(json.loads(row["symbols"] or "[]")),
                }
                for row in semantic_rows
            ]
        except sqlite3.Error:
            semantic_rows = []
            semantic_chunks = []
    payload = {
        "files": file_rows,
        "files_by_path": files_by_path,
        "file_symbols_by_path": file_symbols_by_path,
        "lines": line_rows,
        "line_symbols_by_key": line_symbols_by_key,
        "lines_by_path": normalized_lines_by_path,
        "semantic_chunks": semantic_chunks if semantic_chunks is not None else semantic_rows,
        "bounded": {
            "max_lines": MAX_CACHED_INDEX_LINES,
            "max_semantic_chunks": MAX_CACHED_SEMANTIC_CHUNKS,
        },
    }
    if request_cache is not None:
        rows_cache[cache_key] = payload
    return payload


def _is_large_index_file(index_path: Path) -> bool:
    try:
        return index_path.stat().st_size >= 100_000_000
    except OSError:
        return False


def _structure_lookup_query_terms(
    tokens: list[str],
    focus_terms: list[str] | None,
    *,
    large_index: bool,
    limit: int,
) -> list[str]:
    normalized_focus_terms = [str(term or "").strip().lower() for term in (focus_terms or []) if str(term or "").strip()]
    focus_term_set = set(normalized_focus_terms)
    ordered_terms = list(dict.fromkeys([*normalized_focus_terms, *(str(token or "").strip().lower() for token in tokens)]))
    terms: list[str] = []
    plain_terms_kept = 0
    for term in ordered_terms:
        normalized = str(term or "").strip().lower()
        if not normalized or normalized in terms:
            continue
        if len(normalized) < 3 or normalized in STOPWORDS or normalized in LOW_VALUE_CALL_SYMBOLS:
            continue
        if not large_index:
            terms.append(normalized)
        else:
            has_separator = any(separator in normalized for separator in ("_", ".", "/", "-"))
            is_focus_term = normalized in focus_term_set
            meaningful_plain_term = 4 <= len(normalized) <= 18 and normalized not in LOW_VALUE_FOCUS_TERMS
            if has_separator or is_focus_term:
                terms.append(normalized)
            elif meaningful_plain_term and plain_terms_kept < 4:
                terms.append(normalized)
                plain_terms_kept += 1
        if len(terms) >= max(1, int(limit or 16)):
            break
    return terms


def _targeted_index_rows(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    *,
    tokens: list[str],
    focus_terms: list[str],
    intent: dict[str, Any],
    request_cache: dict[str, Any] | None = None,
    structure_term_limit: int = 16,
) -> dict[str, Any]:
    try:
        indexed_file_count = int(connection.execute("select count(*) from files").fetchone()[0] or 0)
    except sqlite3.Error:
        indexed_file_count = 0
    large_index = indexed_file_count >= 2000 or self._is_large_index_file(index_path)
    focus_term_set = {str(term).lower() for term in focus_terms}
    query_terms = []
    for term in list(dict.fromkeys([*(str(token).lower() for token in tokens), *focus_term_set])):
        if len(term) < 3 or term in STOPWORDS:
            continue
        if large_index and term in LOW_VALUE_FOCUS_TERMS:
            continue
        if large_index and "_" not in term and "/" not in term and "." not in term:
            if len(term) > 28:
                continue
            if len(term) > 14 and term not in focus_term_set:
                continue
        query_terms.append(term)
        if len(query_terms) >= 24:
            break
    structure_query_terms = self._structure_lookup_query_terms(
        query_terms,
        focus_terms,
        large_index=large_index,
        limit=structure_term_limit,
    )
    simple_intent = (
        any((intent or {}).get(key) for key in ("rule_logic", "api", "config"))
        and not any(
            (intent or {}).get(key)
            for key in (
                "data_source",
                "module_dependency",
                "message_flow",
                "static_qa",
                "impact_analysis",
                "test_coverage",
                "operational_boundary",
            )
        )
    )
    max_target_files = 48 if simple_intent else MAX_TARGETED_INDEX_FILES
    max_target_lines = 160 if simple_intent else MAX_TARGETED_INDEX_LINES
    max_target_semantic_chunks = 64 if simple_intent else MAX_TARGETED_SEMANTIC_CHUNKS
    intent_key = ",".join(sorted(key for key, enabled in (intent or {}).items() if enabled))
    cache_key = f"{self._index_fingerprint(index_path)}:targeted:{'|'.join(query_terms)}:{intent_key}"
    if request_cache is not None:
        rows_cache = request_cache.setdefault("targeted_index_rows", {})
        cached = rows_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "targeted_index_rows_hits")
            return cached
        self._increment_retrieval_stat(request_cache, "targeted_index_rows_misses")
    file_rows_by_path: dict[str, sqlite3.Row] = {}
    line_rows_by_key: dict[tuple[str, int], sqlite3.Row] = {}

    def add_file_rows(rows: list[sqlite3.Row]) -> None:
        for row in rows:
            if len(file_rows_by_path) >= max_target_files:
                break
            file_rows_by_path.setdefault(str(row["path"]), row)

    def add_line_rows(rows: list[sqlite3.Row]) -> None:
        for row in rows:
            if len(line_rows_by_key) >= max_target_lines:
                break
            line_rows_by_key.setdefault((str(row["file_path"]), int(row["line_no"])), row)

    def placeholders(values: list[str]) -> str:
        return ",".join("?" for _ in values)

    file_fts_rows = self._file_fts_search_rows(
        connection,
        tokens,
        focus_terms,
        index_path=index_path,
        request_cache=request_cache,
    )
    for row in file_fts_rows:
        file_path = str(row.get("path") if isinstance(row, dict) else row["path"])
        try:
            file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
        except sqlite3.Error:
            file_row = None
        if file_row is not None:
            add_file_rows([file_row])
    if not file_rows_by_path and query_terms:
        token_lookup_supported = True
        try:
            add_file_rows(
                connection.execute(
                    f"""
                    select files.*
                    from file_tokens
                    join files on files.path = file_tokens.file_path
                    where file_tokens.token in ({placeholders(query_terms)})
                    group by files.path
                    order by count(*) desc, files.path
                    limit ?
                    """,
                    (*query_terms, max_target_files),
                ).fetchall()
            )
        except sqlite3.Error:
            token_lookup_supported = False
        if not file_rows_by_path and not token_lookup_supported:
            for term in query_terms[:12]:
                try:
                    add_file_rows(
                        connection.execute(
                            "select * from files where lower_path like ? order by path limit ?",
                            (f"%{term}%", max_target_files),
                        ).fetchall()
                    )
                except sqlite3.Error:
                    continue
    for row in self._fts_search_rows(
        connection,
        tokens,
        focus_terms,
        index_path=index_path,
        request_cache=request_cache,
    ):
        file_path = str(row.get("file_path") if isinstance(row, dict) else row["file_path"])
        line_no = int(row.get("line_no") if isinstance(row, dict) else row["line_no"])
        try:
            file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
            if file_row is not None:
                add_file_rows([file_row])
            line_row = connection.execute("select * from lines where file_path = ? and line_no = ?", (file_path, line_no)).fetchone()
            if line_row is not None:
                add_line_rows([line_row])
        except sqlite3.Error:
            continue
    if not line_rows_by_key and query_terms:
        token_lookup_supported = True
        try:
            add_line_rows(
                connection.execute(
                    f"""
                    select lines.*
                    from line_tokens
                    join lines on lines.file_path = line_tokens.file_path and lines.line_no = line_tokens.line_no
                    where line_tokens.token in ({placeholders(query_terms[:12])})
                    group by lines.file_path, lines.line_no
                    order by count(*) desc, lines.file_path, lines.line_no
                    limit ?
                    """,
                    (*query_terms[:12], max_target_lines),
                ).fetchall()
            )
        except sqlite3.Error:
            token_lookup_supported = False
        if not line_rows_by_key and not token_lookup_supported:
            for term in query_terms[:12]:
                try:
                    add_line_rows(
                        connection.execute(
                            "select * from lines where lower_text like ? order by file_path, line_no limit ?",
                            (f"%{term}%", max_target_lines),
                        ).fetchall()
                    )
                except sqlite3.Error:
                    continue
    line_file_paths = list(dict.fromkeys(file_path for file_path, _line_no in line_rows_by_key))
    for file_path in line_file_paths:
        if file_path in file_rows_by_path:
            continue
        try:
            file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
        except sqlite3.Error:
            file_row = None
        if file_row is not None:
            add_file_rows([file_row])
    intent_file_patterns: list[str] = []
    if intent.get("config"):
        intent_file_patterns.extend(["%.properties", "%.yaml", "%.yml", "%.conf", "%.toml"])
    if intent.get("module_dependency"):
        intent_file_patterns.extend(["%pom.xml", "%build.gradle", "%build.gradle.kts", "%settings.gradle", "%settings.gradle.kts", "%package.json"])
    for pattern in intent_file_patterns:
        try:
            add_file_rows(connection.execute("select * from files where lower_path like ? order by path limit 40", (pattern,)).fetchall())
        except sqlite3.Error:
            continue
    for term in structure_query_terms:
        for table, lower_column in (("definitions", "lower_name"), ("references_index", "lower_target")):
            for row in self._cached_structure_like_rows(
                connection,
                index_path,
                table=table,
                lower_column=lower_column,
                term=term,
                limit=40,
                request_cache=request_cache,
            ):
                file_path = str(row.get("file_path") or "")
                line_no = int(row.get("line_no") or 1)
                try:
                    file_row = connection.execute("select * from files where path = ?", (file_path,)).fetchone()
                    if file_row is not None:
                        add_file_rows([file_row])
                    line_row = connection.execute("select * from lines where file_path = ? and line_no = ?", (file_path, line_no)).fetchone()
                    if line_row is not None:
                        add_line_rows([line_row])
                except sqlite3.Error:
                    continue
    if file_rows_by_path and len(line_rows_by_key) < max_target_lines:
        paths = list(file_rows_by_path)[: (10 if simple_intent else 80)]
        line_fill_limit = min(max_target_lines - len(line_rows_by_key), 80 if simple_intent else max_target_lines)
        try:
            add_line_rows(
                connection.execute(
                    f"select * from lines where file_path in ({placeholders(paths)}) order by file_path, line_no limit ?",
                    (*paths, line_fill_limit),
                ).fetchall()
            )
        except sqlite3.Error:
            pass
    if not file_rows_by_path:
        try:
            add_file_rows(connection.execute("select * from files order by path limit ?", (min(max_target_files, 80),)).fetchall())
        except sqlite3.Error:
            pass
    semantic_rows_by_id = self._targeted_semantic_rows_by_id(
        connection,
        index_path,
        tokens=tokens,
        focus_terms=focus_terms,
        query_terms=query_terms,
        file_paths=list(file_rows_by_path),
        simple_intent=simple_intent,
        max_target_semantic_chunks=max_target_semantic_chunks,
        request_cache=request_cache,
    )
    file_rows = list(file_rows_by_path.values())
    line_rows = sorted(line_rows_by_key.values(), key=lambda row: (str(row["file_path"]), int(row["line_no"])))
    files_by_path = {str(row["path"]): row for row in file_rows}
    file_symbols_by_path = {str(row["path"]): set(json.loads(row["symbols"] or "[]")) for row in file_rows}
    line_symbols_by_key: dict[tuple[str, int], set[str]] = {}
    lines_by_path: dict[str, list[tuple[int, str]]] = {}
    for row in line_rows:
        file_path = str(row["file_path"])
        line_no = int(row["line_no"])
        line_symbols_by_key[(file_path, line_no)] = set(json.loads(row["symbols"] or "[]"))
        lines_by_path.setdefault(file_path, []).append((line_no, str(row["line_text"])))
    semantic_chunks = [
        {
            "chunk_id": str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}"),
            "file_path": str(row["file_path"] or ""),
            "start_line": int(row["start_line"] or 1),
            "end_line": int(row["end_line"] or row["start_line"] or 1),
            "chunk_text": str(row["chunk_text"] or ""),
            "lower_text": str(row["lower_text"] or ""),
            "tokens_set": set(json.loads(row["tokens"] or "[]")),
            "symbols_set": set(json.loads(row["symbols"] or "[]")),
        }
        for row in semantic_rows_by_id.values()
    ] if self.semantic_index_enabled else []
    payload = {
        "files": file_rows,
        "files_by_path": files_by_path,
        "file_symbols_by_path": file_symbols_by_path,
        "lines": line_rows,
        "line_symbols_by_key": line_symbols_by_key,
        "lines_by_path": {
            file_path: [line_text for _, line_text in sorted(rows, key=lambda item: item[0])]
            for file_path, rows in lines_by_path.items()
        },
        "semantic_chunks": semantic_chunks,
        "bounded": {
            "max_files": max_target_files,
            "max_lines": max_target_lines,
            "max_semantic_chunks": max_target_semantic_chunks,
            "targeted": True,
        },
    }
    if request_cache is not None:
        rows_cache[cache_key] = payload
    return payload


def _targeted_semantic_rows_by_id(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    *,
    tokens: list[str],
    focus_terms: list[str],
    query_terms: list[str],
    file_paths: list[str],
    simple_intent: bool,
    max_target_semantic_chunks: int,
    request_cache: dict[str, Any] | None = None,
) -> dict[str, sqlite3.Row]:
    semantic_rows_by_id: dict[str, sqlite3.Row] = {}
    if not self.semantic_index_enabled:
        return semantic_rows_by_id

    def placeholders(values: list[str]) -> str:
        return ",".join("?" for _ in values)

    for row in self._semantic_fts_search_rows(
        connection,
        tokens,
        focus_terms,
        index_path=index_path,
        request_cache=request_cache,
    ):
        chunk_id = str(row.get("chunk_id") if isinstance(row, dict) else row["chunk_id"])
        try:
            chunk_row = connection.execute("select * from semantic_chunks where chunk_id = ?", (chunk_id,)).fetchone()
        except sqlite3.Error:
            chunk_row = None
        if chunk_row is not None:
            semantic_rows_by_id.setdefault(chunk_id, chunk_row)
        if len(semantic_rows_by_id) >= max_target_semantic_chunks:
            break
    if not semantic_rows_by_id and query_terms:
        token_lookup_supported = True
        try:
            rows = connection.execute(
                f"""
                select semantic_chunks.*
                from semantic_chunk_tokens
                join semantic_chunks on semantic_chunks.chunk_id = semantic_chunk_tokens.chunk_id
                where semantic_chunk_tokens.token in ({placeholders(query_terms[:12])})
                group by semantic_chunks.chunk_id
                order by count(*) desc, semantic_chunks.file_path, semantic_chunks.start_line
                limit ?
                """,
                (*query_terms[:12], max_target_semantic_chunks),
            ).fetchall()
        except sqlite3.Error:
            token_lookup_supported = False
            rows = []
        if not rows and not token_lookup_supported:
            for term in query_terms[:12]:
                try:
                    rows.extend(
                        connection.execute(
                            "select * from semantic_chunks where lower_text like ? order by file_path, start_line limit ?",
                            (f"%{term}%", max_target_semantic_chunks),
                        ).fetchall()
                    )
                except sqlite3.Error:
                    continue
        for row in rows:
            if len(semantic_rows_by_id) >= max_target_semantic_chunks:
                break
            chunk_id = str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}")
            semantic_rows_by_id.setdefault(chunk_id, row)
    if file_paths and len(semantic_rows_by_id) < max_target_semantic_chunks:
        paths = file_paths[: (10 if simple_intent else 80)]
        semantic_fill_limit = min(
            max_target_semantic_chunks - len(semantic_rows_by_id),
            32 if simple_intent else max_target_semantic_chunks,
        )
        try:
            rows = connection.execute(
                f"select * from semantic_chunks where file_path in ({placeholders(paths)}) order by file_path, start_line limit ?",
                (*paths, semantic_fill_limit),
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            chunk_id = str(row["chunk_id"] if "chunk_id" in row.keys() else f"{row['file_path']}:{row['start_line']}")
            semantic_rows_by_id.setdefault(chunk_id, row)
    return semantic_rows_by_id


def _cached_file_lines(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    file_path: str,
    *,
    request_cache: dict[str, Any] | None = None,
) -> list[str]:
    normalized_path = str(file_path or "")
    if not normalized_path:
        return []
    if request_cache is not None:
        cache_key = f"{self._index_fingerprint(index_path)}:{normalized_path}"
        file_cache = request_cache.setdefault("file_lines", {})
        cached = file_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "file_lines_hits")
            return list(cached)
        self._increment_retrieval_stat(request_cache, "file_lines_misses")
    rows = connection.execute(
        "select line_text from lines where file_path = ? order by line_no",
        (normalized_path,),
    ).fetchall()
    lines = [str(row["line_text"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows]
    if request_cache is not None:
        request_cache.setdefault("file_lines", {})[cache_key] = list(lines)
    return lines


def _file_fts_search_rows(
    self,
    connection: sqlite3.Connection,
    tokens: list[str],
    focus_terms: list[str],
    *,
    index_path: Path | None = None,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    terms = []
    for term in [*tokens, *focus_terms]:
        normalized = str(term or "").strip().lower()
        if len(normalized) < 3 or normalized in STOPWORDS:
            continue
        if FTS_TOKEN_PATTERN.fullmatch(normalized):
            terms.append(normalized.replace('"', ""))
    terms = list(dict.fromkeys(terms))[:12]
    if not terms:
        return []
    query = " OR ".join(f'"{term}"' for term in terms)
    cache_key = ""
    if request_cache is not None and index_path is not None:
        cache_key = f"{self._index_fingerprint(index_path)}:{query}"
        file_fts_cache = request_cache.setdefault("file_fts", {})
        cached = file_fts_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "file_fts_hits")
            return cached
        self._increment_retrieval_stat(request_cache, "file_fts_misses")
    try:
        rows = list(
            connection.execute(
                """
                select path, bm25(files_fts) as rank
                from files_fts
                where files_fts match ?
                order by rank
                limit 80
                """,
                (query,),
            )
        )
    except sqlite3.Error:
        return []
    payload = [dict(row) for row in rows]
    if request_cache is not None and cache_key:
        request_cache.setdefault("file_fts", {})[cache_key] = payload
    return payload


def _fts_search_rows(
    self,
    connection: sqlite3.Connection,
    tokens: list[str],
    focus_terms: list[str],
    *,
    index_path: Path | None = None,
    request_cache: dict[str, Any] | None = None,
) -> list[sqlite3.Row | dict[str, Any]]:
    terms = []
    for term in [*tokens, *focus_terms]:
        normalized = str(term or "").strip().lower()
        if len(normalized) < 3 or normalized in STOPWORDS:
            continue
        if FTS_TOKEN_PATTERN.fullmatch(normalized):
            terms.append(normalized.replace('"', ""))
    terms = list(dict.fromkeys(terms))[:12]
    if not terms:
        return []
    query = " OR ".join(f'"{term}"' for term in terms)
    cache_key = ""
    if request_cache is not None and index_path is not None:
        cache_key = f"{self._index_fingerprint(index_path)}:{query}"
        fts_cache = request_cache.setdefault("fts", {})
        cached = fts_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "fts_hits")
            return cached
        self._increment_retrieval_stat(request_cache, "fts_misses")
    try:
        rows = list(
            connection.execute(
                """
                select file_path, line_no, bm25(lines_fts) as rank
                from lines_fts
                where lines_fts match ?
                order by rank
                limit 80
                """,
                (query,),
            )
        )
    except sqlite3.Error:
        return []
    payload = [dict(row) for row in rows]
    if request_cache is not None and cache_key:
        request_cache.setdefault("fts", {})[cache_key] = payload
    return payload


def _semantic_fts_search_rows(
    self,
    connection: sqlite3.Connection,
    tokens: list[str],
    focus_terms: list[str],
    *,
    index_path: Path | None = None,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    terms = []
    for term in [*tokens, *focus_terms]:
        normalized = str(term or "").strip().lower()
        if len(normalized) < 3 or normalized in STOPWORDS:
            continue
        if FTS_TOKEN_PATTERN.fullmatch(normalized):
            terms.append(normalized.replace('"', ""))
    terms = list(dict.fromkeys(terms))[:12]
    if not terms:
        return []
    query = " OR ".join(f'"{term}"' for term in terms)
    cache_key = ""
    if request_cache is not None and index_path is not None:
        cache_key = f"{self._index_fingerprint(index_path)}:{query}"
        semantic_fts_cache = request_cache.setdefault("semantic_fts", {})
        cached = semantic_fts_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "semantic_fts_hits")
            return cached
        self._increment_retrieval_stat(request_cache, "semantic_fts_misses")
    try:
        rows = list(
            connection.execute(
                """
                select chunk_id, file_path, bm25(semantic_chunks_fts) as rank
                from semantic_chunks_fts
                where semantic_chunks_fts match ?
                order by rank
                limit 120
                """,
                (query,),
            )
        )
    except sqlite3.Error:
        return []
    payload = [dict(row) for row in rows]
    if request_cache is not None and cache_key:
        request_cache.setdefault("semantic_fts", {})[cache_key] = payload
    return payload


def _cached_structure_like_rows(
    self,
    connection: sqlite3.Connection,
    index_path: Path,
    *,
    table: str,
    lower_column: str,
    term: str,
    limit: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    safe_table_columns = {
        "definitions": {"lower_name"},
        "references_index": {"lower_target"},
    }
    if table not in safe_table_columns or lower_column not in safe_table_columns[table]:
        return []
    normalized = str(term or "").strip().lower()
    if len(normalized) < 3:
        return []
    cache_key = f"{self._index_fingerprint(index_path)}:{table}:{lower_column}:{normalized}:{int(limit or 0)}"
    if request_cache is not None:
        structure_cache = request_cache.setdefault("structure_like", {})
        cached = structure_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "structure_like_hits")
            return cached
        self._increment_retrieval_stat(request_cache, "structure_like_misses")
    try:
        rows = connection.execute(
            f"select * from {table} where {lower_column} = ? limit ?",
            (normalized, int(limit or 40)),
        ).fetchall()
        if len(rows) < int(limit or 40):
            seen = {(str(row["file_path"]), int(row["line_no"]), str(row[lower_column])) for row in rows}
            prefix_rows = connection.execute(
                f"select * from {table} where {lower_column} like ? limit ?",
                (f"{normalized}%", max(0, int(limit or 40) - len(rows))),
            ).fetchall()
            for row in prefix_rows:
                key = (str(row["file_path"]), int(row["line_no"]), str(row[lower_column]))
                if key not in seen:
                    rows.append(row)
                    seen.add(key)
        large_index = self._is_large_index_file(index_path)
        allow_contains_lookup = (
            not large_index
            or (
                len(normalized) >= 12
                and any(separator in normalized for separator in ("_", ".", "/", "-"))
            )
        )
        if allow_contains_lookup and len(rows) < max(8, int(limit or 40) // 3):
            seen = {(str(row["file_path"]), int(row["line_no"]), str(row[lower_column])) for row in rows}
            contains_rows = connection.execute(
                f"select * from {table} where {lower_column} like ? limit ?",
                (f"%{normalized}%", max(0, int(limit or 40) - len(rows))),
            ).fetchall()
            for row in contains_rows:
                key = (str(row["file_path"]), int(row["line_no"]), str(row[lower_column]))
                if key not in seen:
                    rows.append(row)
                    seen.add(key)
    except sqlite3.Error:
        rows = []
    payload = [dict(row) for row in rows]
    if request_cache is not None:
        request_cache.setdefault("structure_like", {})[cache_key] = payload
    return payload


def _semantic_chunk_matches(
    self,
    connection: sqlite3.Connection,
    *,
    entry: RepositoryEntry,
    tokens: list[str],
    question: str,
    focus_terms: list[str],
    trace_stage: str,
    repo_score: int,
    trace_stage_bonus: int,
    rows: list[sqlite3.Row] | None = None,
    intent: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not self.semantic_index_enabled:
        return []
    intent = intent or self._question_intent(question)
    query_terms = self._semantic_query_terms(question, tokens, focus_terms, intent=intent)
    if not query_terms:
        return []
    matches: list[dict[str, Any]] = []
    query_set = set(query_terms)
    if rows is None:
        try:
            rows = connection.execute("select * from semantic_chunks").fetchall()
        except sqlite3.Error:
            return []
    for row in rows:
        if isinstance(row, dict):
            chunk_tokens = set(row.get("tokens_set") or set())
            chunk_symbols = set(row.get("symbols_set") or set())
            lower_text = str(row.get("lower_text") or "")
            file_path = str(row.get("file_path") or "")
            start_line = int(row.get("start_line") or 1)
            end_line = int(row.get("end_line") or start_line)
            chunk_text = str(row.get("chunk_text") or "")
        else:
            chunk_tokens = set(json.loads(row["tokens"] or "[]"))
            chunk_symbols = set(json.loads(row["symbols"] or "[]"))
            lower_text = str(row["lower_text"] or "")
            file_path = str(row["file_path"] or "")
            start_line = int(row["start_line"])
            end_line = int(row["end_line"])
            chunk_text = str(row["chunk_text"] or "")
        overlap = query_set & (chunk_tokens | chunk_symbols)
        phrase_hits = [term for term in query_terms if len(term) >= 5 and term in lower_text]
        if not overlap and not phrase_hits:
            continue
        score = 35 + repo_score + trace_stage_bonus
        score += min(len(overlap), 8) * 9
        score += min(len(phrase_hits), 6) * 12
        if intent.get("data_source") and any(term in lower_text for term in CONCRETE_SOURCE_HINTS):
            score += 26
        if intent.get("api") and any(term in lower_text for term in API_HINTS):
            score += 20
        if intent.get("config") and any(term in lower_text for term in CONFIG_HINTS):
            score += 20
        if intent.get("module_dependency") and any(term in lower_text for term in MODULE_DEPENDENCY_HINTS):
            score += 22
        if (intent.get("error") or intent.get("rule_logic")) and any(term in lower_text for term in ERROR_HINTS + RULE_HINTS):
            score += 18
        matched_terms = list(dict.fromkeys([*sorted(overlap), *phrase_hits]))[:6]
        reason = f"semantic chunk matched: {', '.join(matched_terms)}" if matched_terms else "semantic chunk matched local tokens"
        if trace_stage == "dependency":
            reason = f"dependency trace; {reason}"
        elif trace_stage == "two_hop":
            reason = f"two-hop trace; {reason}"
        elif trace_stage == "query_decomposition":
            reason = f"query decomposition; {reason}"
        matches.append(
            {
                "repo": entry.display_name,
                "path": file_path,
                "line_start": start_line,
                "line_end": end_line,
                "score": score,
                "snippet": chunk_text[:2400],
                "reason": reason,
                "trace_stage": trace_stage,
                "retrieval": "semantic_chunk",
            }
        )
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[:24]


def _semantic_query_terms(
    self,
    question: str,
    tokens: list[str],
    focus_terms: list[str],
    *,
    intent: dict[str, Any] | None = None,
) -> list[str]:
    terms = [*tokens, *focus_terms, *self._semantic_tokens(question)]
    intent = intent or self._question_intent(question)
    if intent.get("data_source"):
        terms.extend(["repository", "mapper", "dao", "jdbc", "jdbctemplate", "select", "from", "client", "integration", "provider", "source"])
    if intent.get("api"):
        terms.extend(["controller", "requestmapping", "postmapping", "getmapping", "endpoint", "api", "client"])
    if intent.get("config"):
        terms.extend(["config", "configuration", "properties", "yaml", "yml", "value"])
    if intent.get("module_dependency"):
        terms.extend(["dependency", "dependencies", "maven", "gradle", "pom", "artifactid", "groupid", "implementation", "package.json", "npm"])
    if intent.get("error") or intent.get("rule_logic"):
        terms.extend(["validate", "validation", "condition", "exception", "rule", "approval", "permission"])
    deduped: list[str] = []
    for term in terms:
        lowered = str(term or "").strip().lower()
        if len(lowered) < 3 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
            continue
        if lowered not in deduped:
            deduped.append(lowered)
    return deduped[:64]


def _extract_exact_lookup_terms(question: str) -> list[str]:
    terms: list[str] = []
    text = str(question or "")
    for match in EXACT_LOOKUP_TERM_PATTERN.finditer(text):
        term = match.group(0).strip("`'\".,;()[]{}<>").lower()
        if not term or term.startswith(("http://", "https://")):
            continue
        if len(term) < 8:
            continue
        if not any(separator in term for separator in (".", "/", ":")):
            continue
        if term not in terms:
            terms.append(term)
    for match in IDENTIFIER_PATTERN.finditer(text):
        term = match.group(0).strip("`'\".,;()[]{}<>").lower()
        if len(term) >= 8 and "_" in term and any(marker in term for marker in ("table", "dwd", "dim", "tmp", "ads", "ods", "snapshot")):
            if any(existing.endswith(f".{term}") for existing in terms):
                continue
            if term not in terms:
                terms.append(term)
            continue
        if len(term) < 24 or term.count("_") < 3:
            continue
        if any(existing.endswith(f".{term}") for existing in terms):
            continue
        if term not in terms:
            terms.append(term)
    return terms[:8]


def _exact_lookup_is_sufficient(terms: list[str], matches: list[dict[str, Any]]) -> bool:
    if not terms or not matches:
        return False
    covered_terms = {
        str((match.get("exact_lookup") or {}).get("term") or "").lower()
        for match in matches
        if (match.get("exact_lookup") or {}).get("term")
    }
    required_terms = {str(term).lower() for term in terms if term}
    return bool(required_terms) and required_terms.issubset(covered_terms)


def _exact_lookup_miss_should_stop(cls, terms: list[str]) -> bool:
    strong_terms = [term for term in terms if cls._is_strict_exact_lookup_term(term)]
    return bool(strong_terms) and len(strong_terms) == len([term for term in terms if str(term or "").strip()])


def _is_strict_exact_lookup_term(term: str) -> bool:
    value = str(term or "").strip().lower()
    if not value:
        return False
    path_extensions = (".java", ".kt", ".py", ".xml", ".sql", ".yaml", ".yml", ".properties", ".ts", ".tsx", ".js", ".jsx", ".md")
    if value.endswith(path_extensions):
        return True
    if "/" in value:
        parts = [part for part in value.split("/") if part]
        looks_like_repo_path = (
            value.startswith(("./", "../", "src/", "spec/", "mapper/", "config/", "resources/"))
            or any(part.endswith(path_extensions) for part in parts)
        )
        if looks_like_repo_path:
            return True
        return False
    if "_" not in value:
        return False
    table_markers = ("_tab", "_table", "dwd", "dim", "tmp", "ads", "ods", "snapshot", "process_info", "response_body")
    return any(marker in value for marker in table_markers)


def _exact_table_path_lookup_repo(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    *,
    question: str,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str, str]] = set()
    index_path = self._index_path(repo_path)
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        if request_cache is not None:
            self._increment_retrieval_stat(request_cache, "exact_lookup_repos")
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for term in terms[:8]:
                normalized = str(term or "").strip().lower()
                if not normalized:
                    continue
                term_matches = 0
                lookup_values = [normalized]
                if "." in normalized:
                    suffix = normalized.rsplit(".", 1)[-1].strip()
                    if len(suffix) >= 8 and suffix.count("_") >= 2 and suffix not in lookup_values:
                        lookup_values.append(suffix)

                def add_match(file_path: str, line_no: int, score: int, reason: str, source: str, lookup_value: str) -> None:
                    nonlocal term_matches
                    key = (str(file_path), int(line_no or 1), normalized, source)
                    if key in seen:
                        return
                    seen.add(key)
                    match = self._match_from_index_location(
                        entry,
                        connection,
                        str(file_path),
                        int(line_no or 1),
                        score=score,
                        reason=reason,
                        question=question,
                        trace_stage="exact_lookup",
                        retrieval="exact_table_path_lookup",
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                    if not match:
                        return
                    match["exact_lookup"] = {"term": normalized, "lookup_value": lookup_value, "source": source}
                    matches.append(match)
                    term_matches += 1

                for lookup_value in lookup_values:
                    for row in connection.execute(
                        """
                        select * from references_index
                        where lower_target = ?
                        order by
                            case kind
                                when 'sql_table' then 0
                                when 'db_read' then 1
                                when 'db_write' then 2
                                when 'runtime_sql' then 3
                                else 4
                            end,
                            file_path,
                            line_no
                        limit 80
                        """,
                        (lookup_value,),
                    ).fetchall():
                        add_match(
                            str(row["file_path"]),
                            int(row["line_no"] or 1),
                            282 if str(row["kind"]) == "sql_table" else 268,
                            f"exact table/path lookup: {row['kind']} {row['target']}",
                            "references_index",
                            lookup_value,
                        )
                    if "/" in lookup_value or lookup_value.endswith((".java", ".py", ".xml", ".sql", ".yaml", ".yml", ".properties", ".ts", ".tsx", ".js", ".jsx")):
                        for row in connection.execute(
                            """
                            select path from files
                            where lower_path = ? or lower_path like ?
                            order by case when lower_path = ? then 0 else 1 end, path
                            limit 40
                            """,
                            (lookup_value, f"%{lookup_value}%", lookup_value),
                        ).fetchall():
                            add_match(
                                str(row["path"]),
                                1,
                                260,
                                f"exact path lookup: {lookup_value}",
                                "files",
                                lookup_value,
                            )
                    try:
                        line_rows = connection.execute(
                            """
                            select lines.file_path, lines.line_no
                            from line_tokens
                            join lines on lines.file_path = line_tokens.file_path and lines.line_no = line_tokens.line_no
                            where line_tokens.token = ?
                            order by lines.file_path, lines.line_no
                            limit 100
                            """,
                            (lookup_value,),
                        ).fetchall()
                    except sqlite3.Error:
                        line_rows = []
                    for row in line_rows:
                        add_match(
                            str(row["file_path"]),
                            int(row["line_no"] or 1),
                            248,
                            f"exact line lookup: {lookup_value}",
                            "line_tokens",
                            lookup_value,
                        )
                if term_matches == 0:
                    for lookup_value in lookup_values:
                        fts_query = f'"{lookup_value.replace(chr(34), chr(34) + chr(34))}"'
                        try:
                            fallback_rows = connection.execute(
                                """
                                select file_path, line_no from lines_fts
                                where lines_fts match ?
                                order by file_path, line_no
                                limit 80
                                """,
                                (fts_query,),
                            ).fetchall()
                        except sqlite3.Error:
                            fallback_rows = []
                        for row in fallback_rows:
                            add_match(
                                str(row["file_path"]),
                                int(row["line_no"] or 1),
                                236,
                                f"exact indexed text lookup: {lookup_value}",
                                "lines_fts",
                                lookup_value,
                            )
                if request_cache is not None:
                    stat_key = "exact_lookup_hits" if term_matches else "exact_lookup_misses"
                    self._increment_retrieval_stat(request_cache, stat_key)
    except (OSError, sqlite3.Error):
        return []
    matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    return matches[:160]


def _index_fingerprint(self, index_path: Path) -> str:
    try:
        stat = index_path.stat()
    except OSError:
        return f"{index_path}:missing:{CODE_INDEX_VERSION}"
    return f"{index_path}:{stat.st_mtime_ns}:{stat.st_size}:{CODE_INDEX_VERSION}"


def _search_cache_key(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    tokens: list[str],
    *,
    question: str,
    focus_terms: list[str] | None,
    trace_stage: str,
) -> str:
    index_path = self._index_path(repo_path)
    payload = {
        "repo": entry.display_name,
        "url": entry.url,
        "repo_path": str(repo_path),
        "index": self._index_fingerprint(index_path),
        "tokens": list(tokens),
        "question": question,
        "focus_terms": list(focus_terms or []),
        "trace_stage": trace_stage,
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _ensure_repo_index_cached(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    *,
    request_cache: dict[str, Any] | None = None,
) -> None:
    if request_cache is None:
        self._require_ready_repo_index(key=None, entry=entry, repo_path=repo_path)
        return
    ensured_indexes = request_cache.setdefault("ensured_indexes", set())
    ensured_key = str(repo_path)
    if ensured_key in ensured_indexes:
        self._increment_retrieval_stat(request_cache, "index_ensure_hits")
        return
    self._increment_retrieval_stat(request_cache, "index_ensure_misses")
    self._require_ready_repo_index(key=None, entry=entry, repo_path=repo_path)
    ensured_indexes.add(ensured_key)


def _search_repo(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    tokens: list[str],
    *,
    question: str,
    focus_terms: list[str] | None = None,
    trace_stage: str = "direct",
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        cache_key = self._search_cache_key(
            entry,
            repo_path,
            tokens,
            question=question,
            focus_terms=focus_terms,
            trace_stage=trace_stage,
        )
        if request_cache is not None:
            search_cache = request_cache.setdefault("search", {})
            cached = search_cache.get(cache_key)
            if cached is not None:
                self._increment_retrieval_stat(request_cache, "search_hits")
                return self._clone_jsonish(cached)
            self._increment_retrieval_stat(request_cache, "search_misses")
        matches = self._search_repo_index(
            entry,
            repo_path,
            tokens,
            question=question,
            focus_terms=focus_terms,
            trace_stage=trace_stage,
            request_cache=request_cache,
        )
        if request_cache is not None:
            request_cache.setdefault("search", {})[cache_key] = self._clone_jsonish(matches)
        return matches
    except (OSError, sqlite3.Error):
        return []


def _iter_text_files(self, repo_path: Path):
    for path in repo_path.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(repo_path).parts):
            continue
        if path.stat().st_size > self.max_file_bytes:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {"Dockerfile", "Makefile"}:
            continue
        yield path


def _repo_path(self, key: str, entry: RepositoryEntry) -> Path:
    digest = hashlib.sha1(f"{key}:{entry.url}".encode("utf-8")).hexdigest()[:12]
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", entry.display_name).strip("-")[:48] or "repo"
    return self.repo_root / self._safe_key(key) / f"{slug}-{digest}"


def _safe_key(key: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", key).strip("-")


def _normalize_entry(self, raw: dict[str, Any]) -> RepositoryEntry:
    if not isinstance(raw, dict):
        raise ToolError("Each repository entry must include a display name and HTTPS URL.")
    url = str(raw.get("url") or "").strip()
    if not url:
        raise ToolError("Repository HTTPS URL cannot be empty.")
    if not HTTPS_URL_PATTERN.match(url):
        raise ToolError("Only HTTPS clone URLs are supported, for example https://gitlab.example.com/group/repo.git.")
    display_name = str(raw.get("display_name") or "").strip() or self._derive_display_name(url)
    return RepositoryEntry(display_name=display_name[:80], url=url)


def _entry_to_dict(entry: RepositoryEntry | dict[str, Any]) -> dict[str, str]:
    if isinstance(entry, RepositoryEntry):
        return {"display_name": entry.display_name, "url": entry.url}
    display_name = str(entry.get("display_name") or "").strip()
    url = str(entry.get("url") or "").strip()
    return {"display_name": display_name or SourceCodeQAService._derive_display_name(url), "url": url}


def _derive_display_name(url: str) -> str:
    tail = url.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]
    return tail.removesuffix(".git") or "Repository"


def _authenticated_git_url(self, url: str) -> str:
    parts = urlsplit(url)
    netloc = f"{self.gitlab_username}:{self.gitlab_token}@{parts.netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _sanitize_error_detail(self, detail: str) -> str:
    sanitized = str(detail or "")
    if self.gitlab_token:
        sanitized = sanitized.replace(self.gitlab_token, "***")
    return re.sub(r"https://[^:@/\s]+:[^@/\s]+@", "https://***:***@", sanitized)


def _remove_incomplete_repo_dir(repo_path: Path) -> None:
    shutil.rmtree(repo_path, ignore_errors=True)


def _question_tokens(question: str) -> list[str]:
    return list(SourceCodeQAService._question_tokens_cached(str(question or "")))


@functools.lru_cache(maxsize=4096)
def _question_tokens_cached(question: str) -> tuple[str, ...]:
    lowered_question = question.lower()
    raw_tokens = re.findall(r"[a-zA-Z0-9_./:-]{1,}", lowered_question)
    raw_tokens.extend(re.findall(r"[\u4e00-\u9fff]{2,}", lowered_question))
    tokens = []
    for token in raw_tokens:
        token = token.strip("./:-_")
        if (len(token) < 2 and not token.isdigit()) or token in STOPWORDS:
            continue
        if token not in tokens:
            tokens.append(token)

    # Business questions often use spaced phrases while code uses camelCase or
    # suffix numbers, e.g. "Term Loan Pre Check 1" -> "termloanprecheck1".
    words = [token for token in tokens if re.fullmatch(r"[a-z0-9]+", token)]
    phrase_variants: list[str] = []
    for size in (2, 3, 4, 5):
        for index in range(0, max(0, len(words) - size + 1)):
            compact = "".join(words[index : index + size])
            if len(compact) >= 4:
                phrase_variants.append(compact)
    if words:
        compact_all = "".join(words[:8])
        if len(compact_all) >= 4:
            phrase_variants.append(compact_all)
    for variant in phrase_variants:
        if variant not in tokens:
            tokens.append(variant)
    return tuple(tokens[:28])


def _match_reason(
    tokens: list[str],
    path_text: str,
    snippet: str,
    *,
    file_symbols: set[str],
    question: str,
    focus_terms: list[str] | None = None,
    trace_stage: str = "direct",
) -> str:
    snippet_text = snippet.lower()
    path_hits = [token for token in tokens if token in path_text]
    content_hits = [token for token in tokens if token in snippet_text]
    symbol_hits = [token for token in tokens if token in file_symbols]
    focus_hits = [term for term in (focus_terms or []) if term in snippet_text or term in path_text or term in file_symbols]
    parts = []
    if trace_stage == "dependency":
        parts.append("dependency trace")
    elif trace_stage == "two_hop":
        parts.append("two-hop trace")
    elif trace_stage == "query_decomposition":
        parts.append("query decomposition")
    elif trace_stage.startswith(TOOL_LOOP_TRACE_PREFIX):
        parts.append("planner tool trace")
    elif trace_stage.startswith("agent_trace"):
        parts.append("agent trace")
    elif trace_stage.startswith("agent_plan"):
        parts.append("agent plan trace")
    elif trace_stage == QUALITY_GATE_TRACE_STAGE:
        parts.append("quality-gate trace")
    if path_hits:
        parts.append(f"path matched: {', '.join(path_hits[:4])}")
    if symbol_hits:
        parts.append(f"symbol matched: {', '.join(symbol_hits[:4])}")
    if focus_hits:
        parts.append(f"downstream hit: {', '.join(focus_hits[:4])}")
    if content_hits:
        parts.append(f"content matched: {', '.join(content_hits[:4])}")
    if not parts and question:
        parts.append("best semantic filename/content similarity")
    return "; ".join(parts) or "filename/content similarity"


def _is_dependency_question(question: str) -> bool:
    lowered = question.lower()
    return any(term in lowered for term in DEPENDENCY_QUESTION_TERMS)


def _requires_cross_repo_context(question: str) -> bool:
    lowered = f" {str(question or '').lower()} "
    return any(
        term in lowered
        for term in (
            " which repo",
            " which repository",
            " which service",
            " cross repo",
            " cross-repo",
            " downstream",
            " upstream",
            " consume",
            " consumes",
            " producer",
            " publisher",
            " after it is written",
            " after written",
            " read ",
            " written ",
            " v2 ",
            " report ",
            " failed ",
            " failure ",
            " root cause ",
            " 调用链",
            " 链路",
            " 上游",
            " 下游",
            " 失败",
            " 报错",
            " 为什么",
            " 什么意思",
            " 区别",
        )
    )


def _is_test_file_path(path: str) -> bool:
    normalized = str(path or "").replace("\\", "/").lower()
    return any(marker in normalized for marker in TEST_PATH_MARKERS) or normalized.endswith(
        ("test.java", "tests.java", "spec.java", "test.py", "spec.py", "test.ts", "spec.ts", "test.js", "spec.js", "test.tsx", "spec.tsx", "test.jsx", "spec.jsx")
    )


def _expand_dependency_matches(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    base_matches: list[dict[str, Any]],
    limit: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    focus_terms = self._dependency_focus_terms(question, base_matches)
    if not focus_terms:
        return []
    matches: list[dict[str, Any]] = []
    expanded_tokens = []
    for term in focus_terms:
        expanded_tokens.extend(self._question_tokens(term))
    expanded_tokens = list(dict.fromkeys(expanded_tokens))[:16]
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        matches.extend(
            self._search_repo(
                entry,
                repo_path,
                expanded_tokens,
                question=question,
                focus_terms=focus_terms,
                trace_stage="dependency",
                request_cache=request_cache,
            )
        )
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(4, min(int(limit or 12), 16))]


def _expand_two_hop_matches(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    base_matches: list[dict[str, Any]],
    limit: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    trace_terms = self._two_hop_trace_terms(question, base_matches)
    if not trace_terms:
        return []
    expanded_tokens: list[str] = []
    for term in trace_terms:
        expanded_tokens.extend(self._question_tokens(term))
    expanded_tokens = list(dict.fromkeys(expanded_tokens))[:24]

    matches: list[dict[str, Any]] = []
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        matches.extend(
            self._search_repo(
                entry,
                repo_path,
                expanded_tokens,
                question=question,
                focus_terms=trace_terms,
                trace_stage="two_hop",
                request_cache=request_cache,
            )
        )
    matches.sort(key=lambda item: item["score"], reverse=True)
    return matches[: max(4, min(int(limit or 12), 18))]


def _expand_agent_trace_matches(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    base_matches: list[dict[str, Any]],
    limit: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    all_matches: list[dict[str, Any]] = []
    frontier = list(base_matches[:10])
    seen_terms: set[str] = set()
    seen_match_keys = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in base_matches}
    max_rounds = 3
    for round_index in range(1, max_rounds + 1):
        trace_terms = self._agent_trace_terms(question, frontier, seen_terms)
        if not trace_terms:
            break
        seen_terms.update(trace_terms)
        expanded_tokens: list[str] = []
        for term in trace_terms:
            expanded_tokens.extend(self._question_tokens(term))
        expanded_tokens = list(dict.fromkeys(expanded_tokens))[:24]
        round_matches: list[dict[str, Any]] = []
        for entry in entries:
            repo_path = self._repo_path(key, entry)
            if not (repo_path / ".git").exists():
                continue
            round_matches.extend(
                self._search_repo(
                    entry,
                    repo_path,
                    expanded_tokens,
                    question=question,
                    focus_terms=trace_terms,
                    trace_stage=f"agent_trace_{round_index}",
                    request_cache=request_cache,
                )
            )
        round_matches.sort(key=lambda item: item["score"], reverse=True)
        next_frontier: list[dict[str, Any]] = []
        for item in round_matches[: max(6, min(int(limit or 12), 18))]:
            item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
            if item_key in seen_match_keys:
                continue
            all_matches.append(item)
            next_frontier.append(item)
            seen_match_keys.add(item_key)
        frontier = next_frontier
        if not frontier:
            break
    all_matches.sort(key=lambda item: item["score"], reverse=True)
    return all_matches[: max(6, min(int(limit or 12) * 2, 24))]


def _run_planner_tool_loop(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    base_matches: list[dict[str, Any]],
    limit: int,
    tool_trace: list[dict[str, Any]] | None = None,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    current_matches = list(base_matches)
    seen = {(item["repo"], item["path"], item["line_start"], item["line_end"]) for item in current_matches}
    executed_steps: set[str] = set()
    empty_rounds = 0
    max_rounds = 5
    for step_index in range(1, max_rounds + 1):
        evidence_summary = self._compress_evidence_cached(question, current_matches, request_cache=request_cache)
        quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
        step = self._choose_next_tool_step(
            question=question,
            matches=current_matches,
            evidence_summary=evidence_summary,
            quality_gate=quality_gate,
            executed_steps=executed_steps,
        )
        if not step:
            if tool_trace is not None:
                tool_trace.append(
                    {
                        "phase": "tool_loop",
                        "round": step_index,
                        "tool": "stop",
                        "reason": "no_next_tool",
                        "matches_before": len(current_matches),
                    }
                )
            break
        step_signature = self._tool_step_signature(step, current_matches)
        executed_steps.add(step_signature)
        step_matches = self._execute_tool_loop_step(
            entries=entries,
            key=key,
            question=question,
            matches=current_matches,
            step=step,
            step_index=step_index,
            request_cache=request_cache,
        )
        step_matches.sort(key=lambda item: item["score"], reverse=True)
        added = 0
        for item in step_matches[: max(5, min(int(limit or 12), 18))]:
            item_key = (item["repo"], item["path"], item["line_start"], item["line_end"])
            if item_key in seen:
                self._annotate_duplicate_tool_match(current_matches, item)
                continue
            collected.append(item)
            current_matches.append(item)
            seen.add(item_key)
            added += 1
        current_matches.sort(key=lambda item: item["score"], reverse=True)
        current_matches = self._select_result_matches(current_matches, self._query_result_limit(limit), question=question)
        empty_rounds = empty_rounds + 1 if added == 0 else 0
        should_stop = self._should_stop_tool_loop(question, current_matches, step_index, empty_rounds, request_cache=request_cache)
        if tool_trace is not None:
            tool_trace.append(
                {
                    "phase": "tool_loop",
                    "round": step_index,
                    "tool": str(step.get("tool") or "search_code"),
                    "terms": [str(term) for term in step.get("terms") or []][:10],
                    "matches_found": len(step_matches),
                    "matches_added": added,
                    "matches_after": len(current_matches),
                    "stop_reason": "quality_sufficient" if should_stop else "",
                }
            )
        if should_stop:
            break
    collected.sort(key=lambda item: item["score"], reverse=True)
    return collected[: max(6, min(int(limit or 12) * 2, 24))]


def _annotate_duplicate_tool_match(existing_matches: list[dict[str, Any]], duplicate: dict[str, Any]) -> None:
    duplicate_key = (
        duplicate.get("repo"),
        duplicate.get("path"),
        duplicate.get("line_start"),
        duplicate.get("line_end"),
    )
    for existing in existing_matches:
        existing_key = (
            existing.get("repo"),
            existing.get("path"),
            existing.get("line_start"),
            existing.get("line_end"),
        )
        if existing_key != duplicate_key:
            continue
        duplicate_retrieval = str(duplicate.get("retrieval") or "")
        existing_retrieval = str(existing.get("retrieval") or "file_scan")
        if duplicate_retrieval and duplicate_retrieval != existing_retrieval:
            chain = existing.setdefault("retrieval_chain", [])
            for retrieval in (existing_retrieval, duplicate_retrieval):
                if retrieval and retrieval not in chain:
                    chain.append(retrieval)
            if duplicate_retrieval in {
                "flow_graph",
                "code_graph",
                "entity_graph",
                "static_qa",
                "test_coverage",
                "operational_boundary",
            }:
                existing["retrieval"] = duplicate_retrieval
            for payload_key in ("static_qa", "test_coverage", "operational_boundary"):
                if duplicate.get(payload_key):
                    existing[payload_key] = duplicate.get(payload_key)
        duplicate_reason = str(duplicate.get("reason") or "")
        if duplicate_reason and duplicate_reason not in str(existing.get("reason") or ""):
            existing["reason"] = f"{existing.get('reason')}; corroborated by {duplicate_reason}"
        duplicate_stage = str(duplicate.get("trace_stage") or "")
        existing_stage = str(existing.get("trace_stage") or "")
        if duplicate_stage and duplicate_stage not in {"direct", "query_decomposition"} and existing_stage in {"direct", "query_decomposition"}:
            existing["trace_stage"] = duplicate_stage
        existing["score"] = max(int(existing.get("score") or 0), int(duplicate.get("score") or 0))
        return


def _choose_next_tool_step(
    self,
    *,
    question: str,
    matches: list[dict[str, Any]],
    evidence_summary: dict[str, Any],
    quality_gate: dict[str, Any],
    executed_steps: set[str],
) -> dict[str, Any] | None:
    terms = self._tool_loop_terms(question, matches)
    quality_terms = self._quality_gate_trace_terms(question, evidence_summary, quality_gate, matches)
    intent = evidence_summary.get("intent") or self._question_intent(question)
    candidates: list[dict[str, Any]] = []

    if intent.get("data_source"):
        candidates.extend(
            [
                {"tool": "find_tables", "terms": [*terms[:10], *quality_terms[:8]]},
                {"tool": "trace_entity", "terms": terms[:12]},
                {"tool": "trace_flow", "terms": terms[:12]},
                {"tool": "trace_graph", "terms": terms[:12]},
                {"tool": "find_callees", "terms": terms[:12]},
                {"tool": "find_callers", "terms": terms[:12]},
                {"tool": "search_code", "terms": [*quality_terms, "repository", "mapper", "dao", "select", "from", "client"]},
            ]
        )
    if intent.get("api"):
        candidates.extend(
            [
                {"tool": "find_api_routes", "terms": [*terms[:10], *quality_terms[:8]]},
                {"tool": "trace_entity", "terms": terms[:12]},
                {"tool": "trace_flow", "terms": terms[:12]},
                {"tool": "find_references", "terms": [*terms[:10], "RequestMapping", "PostMapping", "GetMapping"]},
                {"tool": "find_callees", "terms": terms[:12]},
                {"tool": "search_code", "terms": [*terms[:8], "controller", "endpoint", "client"]},
            ]
        )
    if intent.get("config"):
        candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "properties", "yaml", "configuration"]})
    if intent.get("module_dependency"):
        candidates.extend(
            [
                {"tool": "trace_flow", "terms": [*terms[:10], "module_dependency", "maven", "gradle"]},
                {"tool": "search_code", "terms": [*terms[:8], *quality_terms, "pom.xml", "build.gradle", "package.json", "artifactId", "dependency"]},
            ]
        )
    if intent.get("rule_logic") or intent.get("error"):
        candidates.append({"tool": "search_code", "terms": [*terms[:8], *quality_terms, "validate", "rule", "exception"]})
    if intent.get("static_qa"):
        candidates.extend(
            [
                {"tool": "find_static_findings", "terms": [*terms[:10], *quality_terms[:8]]},
                {"tool": "search_code", "terms": [*terms[:8], *quality_terms, "TODO", "FIXME", "secret", "catch", "Exception"]},
            ]
        )
    if intent.get("impact_analysis"):
        candidates.extend(
            [
                {"tool": "find_references", "terms": [*terms[:12], *quality_terms[:6]]},
                {"tool": "find_callers", "terms": terms[:12]},
                {"tool": "find_callees", "terms": terms[:12]},
                {"tool": "trace_flow", "terms": terms[:12]},
                {"tool": "trace_entity", "terms": terms[:12]},
                {"tool": "search_code", "terms": [*terms[:8], "controller", "service", "repository", "client", "handler"]},
            ]
        )
    if intent.get("test_coverage"):
        candidates.extend(
            [
                {"tool": "find_test_coverage", "terms": [*terms[:12], *quality_terms[:6]]},
                {"tool": "search_code", "terms": [*terms[:8], "test", "assert", "verify", "mock"]},
            ]
        )
    if intent.get("operational_boundary"):
        candidates.extend(
            [
                {"tool": "find_operational_boundaries", "terms": [*terms[:12], *quality_terms[:8]]},
                {"tool": "trace_entity", "terms": [*terms[:12], "operational_boundary"]},
                {"tool": "search_code", "terms": [*terms[:8], "Transactional", "Cacheable", "Async", "Retryable", "CircuitBreaker"]},
            ]
        )

    candidates.extend(self._build_tool_loop_plan(question, matches))
    if matches:
        candidates.append({"tool": "open_file_window", "terms": terms[:8]})
    if terms:
        candidates.append({"tool": "trace_flow", "terms": terms[:12]})

    for candidate in candidates:
        normalized_terms = list(
            dict.fromkeys(str(term).strip() for term in candidate.get("terms") or [] if str(term).strip())
        )
        tool = str(candidate.get("tool") or "")
        if tool not in {
            "find_definition",
            "find_references",
            "find_callers",
            "find_callees",
            "open_file_window",
            "find_tables",
            "find_api_routes",
            "trace_graph",
            "trace_flow",
            "trace_entity",
            "find_static_findings",
            "find_test_coverage",
            "find_operational_boundaries",
            "search_code",
        }:
            continue
        if tool in {"find_definition", "find_references", "find_callers", "find_tables", "find_api_routes", "search_code"} and not normalized_terms:
            continue
        step = {"tool": tool, "terms": normalized_terms[:18]}
        signature = self._tool_step_signature(step, matches)
        if signature not in executed_steps:
            return step
    return None


def _tool_step_signature(step: dict[str, Any], matches: list[dict[str, Any]]) -> str:
    tool = str(step.get("tool") or "")
    terms = ",".join(str(term).lower() for term in (step.get("terms") or [])[:10])
    if tool in {"trace_flow", "trace_graph", "trace_entity"}:
        seed_paths = ",".join(str(match.get("path") or "") for match in matches[:10])
        return f"{tool}:{seed_paths}:{terms}"
    return f"{tool}:{terms}"


def _execute_tool_loop_step(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    matches: list[dict[str, Any]],
    step: dict[str, Any],
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    tool = str(step.get("tool") or "")
    terms = [str(term) for term in (step.get("terms") or []) if str(term)]
    step_matches: list[dict[str, Any]] = []
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        if tool == "find_definition":
            step_matches.extend(self._tool_find_definition(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_references":
            step_matches.extend(self._tool_find_references(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_callers":
            step_matches.extend(self._tool_find_callers(entry, repo_path, matches, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_callees":
            step_matches.extend(self._tool_find_callees(entry, repo_path, matches, terms, question, step_index, request_cache=request_cache))
        elif tool == "open_file_window":
            step_matches.extend(self._tool_open_file_window(entry, repo_path, matches, question, step_index, request_cache=request_cache))
        elif tool == "find_tables":
            step_matches.extend(self._tool_find_tables(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_api_routes":
            step_matches.extend(self._tool_find_api_routes(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "trace_graph":
            step_matches.extend(self._tool_trace_graph(entry, repo_path, matches, question, step_index, request_cache=request_cache))
        elif tool == "trace_flow":
            step_matches.extend(self._tool_trace_flow(entry, repo_path, matches, question, step_index, request_cache=request_cache))
        elif tool == "trace_entity":
            step_matches.extend(self._tool_trace_entity(entry, repo_path, matches, question, step_index, request_cache=request_cache))
        elif tool == "find_static_findings":
            step_matches.extend(self._tool_find_static_findings(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_test_coverage":
            step_matches.extend(self._tool_find_test_coverage(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "find_operational_boundaries":
            step_matches.extend(self._tool_find_operational_boundaries(entry, repo_path, terms, question, step_index, request_cache=request_cache))
        elif tool == "search_code":
            expanded_tokens: list[str] = []
            for term in terms:
                expanded_tokens.extend(self._question_tokens(term))
            step_matches.extend(
                self._search_repo(
                    entry,
                    repo_path,
                    list(dict.fromkeys(expanded_tokens))[:30],
                    question=question,
                    focus_terms=terms,
                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                    request_cache=request_cache,
                )
            )
    return step_matches


def _should_stop_tool_loop(
    self,
    question: str,
    matches: list[dict[str, Any]],
    step_index: int,
    empty_rounds: int,
    request_cache: dict[str, Any] | None = None,
) -> bool:
    if empty_rounds >= 2:
        return True
    evidence_summary = self._compress_evidence_cached(question, matches, request_cache=request_cache)
    quality_gate = self._quality_gate_cached(question, evidence_summary, request_cache=request_cache)
    if quality_gate.get("status") != "sufficient" or step_index < 2:
        return False
    if evidence_summary.get("intent", {}).get("data_source"):
        return bool(evidence_summary.get("data_sources"))
    return True


def _build_tool_loop_plan(self, question: str, base_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    intent = self._question_intent(question)
    terms = self._tool_loop_terms(question, base_matches)
    plan: list[dict[str, Any]] = []
    if terms:
        plan.append({"tool": "find_definition", "terms": terms[:12]})
        plan.append({"tool": "find_references", "terms": terms[:12]})
    if intent.get("data_source") or intent.get("api") or intent.get("rule_logic"):
        if intent.get("data_source"):
            plan.append({"tool": "find_tables", "terms": terms[:12]})
        if intent.get("api"):
            plan.append({"tool": "find_api_routes", "terms": terms[:12]})
        plan.append({"tool": "trace_entity", "terms": terms[:12]})
        plan.append({"tool": "trace_flow", "terms": terms[:12]})
        plan.append({"tool": "trace_graph", "terms": terms[:12]})
        plan.append({"tool": "find_callees", "terms": terms[:12]})
    if intent.get("config"):
        plan.append({"tool": "search_code", "terms": [*terms[:8], "properties", "configuration", "yaml", "feature"]})
    if intent.get("data_source"):
        plan.append({"tool": "search_code", "terms": [*terms[:8], "repository", "mapper", "select", "from", "client"]})
    if intent.get("static_qa"):
        plan.append({"tool": "find_static_findings", "terms": [*terms[:8], "secret", "catch", "exception", "todo", "sql"]})
    if intent.get("test_coverage"):
        plan.append({"tool": "find_test_coverage", "terms": [*terms[:8], "test", "assert", "verify", "mock"]})
    if intent.get("operational_boundary"):
        plan.append({"tool": "find_operational_boundaries", "terms": [*terms[:8], "Transactional", "Cacheable", "Async", "Retryable", "CircuitBreaker"]})
    if intent.get("impact_analysis"):
        plan.extend(
            [
                {"tool": "find_references", "terms": terms[:12]},
                {"tool": "find_callers", "terms": terms[:12]},
                {"tool": "find_callees", "terms": terms[:12]},
                {"tool": "trace_flow", "terms": terms[:12]},
            ]
        )
    return plan[:5]


def _tool_loop_terms(self, question: str, base_matches: list[dict[str, Any]]) -> list[str]:
    terms = list(self._question_tokens(question))
    for match in base_matches[:8]:
        terms.extend(IDENTIFIER_PATTERN.findall(str(match.get("path") or "")))
        terms.extend(self._extract_downstream_symbols(str(match.get("snippet") or "")))
        terms.extend(self._extract_assignment_sources(str(match.get("snippet") or "")))
    deduped = []
    for term in terms:
        lowered = str(term or "").strip().lower()
        if len(lowered) < 3 or lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
            continue
        if lowered not in deduped:
            deduped.append(lowered)
    return deduped[:20]


def _tool_find_definition(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return self._tool_lookup_structure(
        entry,
        repo_path,
        terms,
        question=question,
        table="definitions",
        name_column="name",
        lower_column="lower_name",
        line_column="line_no",
        kind_column="kind",
        trace_stage=f"tool_loop_{step_index}",
        retrieval="planner_definition",
        request_cache=request_cache,
    )


def _tool_find_references(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return self._tool_lookup_structure(
        entry,
        repo_path,
        terms,
        question=question,
        table="references_index",
        name_column="target",
        lower_column="lower_target",
        line_column="line_no",
        kind_column="kind",
        trace_stage=f"tool_loop_{step_index}",
        retrieval="planner_reference",
        request_cache=request_cache,
    )


def _tool_find_tables(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    lookup_terms = list(dict.fromkeys([*terms, "select", "from", "join", "update", "insert"]))
    return self._tool_lookup_references_by_kind(
        entry,
        repo_path,
        lookup_terms,
        kinds={"sql_table"},
        question=question,
        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
        retrieval="planner_table",
        score=184,
        request_cache=request_cache,
    )


def _tool_find_api_routes(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    lookup_terms = list(dict.fromkeys([*terms, "requestmapping", "postmapping", "getmapping", "endpoint", "api", "http"]))
    return self._tool_lookup_references_by_kind(
        entry,
        repo_path,
        lookup_terms,
        kinds={"route", "http_endpoint", "downstream_api"},
        question=question,
        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
        retrieval="planner_api_route",
        score=178,
        request_cache=request_cache,
    )


def _tool_find_static_findings(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index_path = self._index_path(repo_path)
    lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            index_rows = self._cached_index_rows(connection, index_path, request_cache=request_cache)
            lines_by_path = index_rows.get("lines_by_path") or {}
            seen: set[tuple[str, int, str]] = set()
            for file_path, lines in lines_by_path.items():
                path_lower = str(file_path).lower()
                if any(part in path_lower for part in ("/node_modules/", "/dist/", "/build/", "/target/", ".min.js")):
                    continue
                for line_index, line_text in enumerate(lines, start=1):
                    findings = self._static_qa_findings_for_line(str(line_text))
                    if not findings:
                        continue
                    haystack = f"{file_path} {line_text}".lower()
                    for finding in findings:
                        key = (str(file_path), line_index, str(finding["kind"]))
                        if key in seen:
                            continue
                        seen.add(key)
                        term_boost = 18 if lowered_terms and any(term in haystack or term in str(finding["kind"]).lower() for term in lowered_terms) else 0
                        match = self._match_from_index_location(
                            entry,
                            connection,
                            str(file_path),
                            line_index,
                            score=int(finding["score"]) + term_boost,
                            reason=f"static QA finding: {finding['severity']} {finding['kind']} - {finding['reason']}",
                            question=question,
                            trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "static_qa",
                            retrieval="static_qa",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                        if match:
                            match["static_qa"] = {
                                "kind": finding["kind"],
                                "severity": finding["severity"],
                                "reason": finding["reason"],
                            }
                            matches.append(match)
            matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    except (OSError, sqlite3.Error):
        return []
    return matches[:80]


def _tool_find_test_coverage(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    noise_terms = set(self._question_tokens(" ".join(TEST_COVERAGE_HINTS))) | {"covered", "coverage", "unit", "integration", "junit", "pytest", "jest"}
    lookup_terms = [
        term.lower()
        for term in [*terms, *self._question_tokens(question)]
        if len(str(term).strip()) >= 3 and term.lower() not in noise_terms and term.lower() not in STOPWORDS
    ]
    lookup_terms = list(dict.fromkeys(lookup_terms))[:18]
    if not lookup_terms:
        return []
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    index_path = self._index_path(repo_path)
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            index_rows = self._cached_index_rows(connection, index_path, request_cache=request_cache)
            lines_by_path = index_rows.get("lines_by_path") or {}
            for file_path, lines in lines_by_path.items():
                if not self._is_test_file_path(str(file_path)):
                    continue
                path_lower = str(file_path).lower()
                file_term_hit = any(term in path_lower for term in lookup_terms)
                best_line = 1
                best_score = 0
                best_reasons: list[str] = []
                for line_index, line_text in enumerate(lines, start=1):
                    lowered_line = str(line_text).lower()
                    term_hits = [term for term in lookup_terms if term in lowered_line]
                    if not term_hits and not file_term_hit:
                        continue
                    score = 172 + (28 if term_hits else 0) + (16 if file_term_hit else 0)
                    if TEST_ASSERTION_PATTERN.search(str(line_text)):
                        score += 24
                        best_reasons.append("assertion/verify present")
                    if TEST_ANNOTATION_PATTERN.search(str(line_text)) or re.search(r"\b(?:test|should)[A-Za-z0-9_]*\s*\(", str(line_text)):
                        score += 20
                        best_reasons.append("test case present")
                    if term_hits:
                        best_reasons.append(f"target terms: {', '.join(term_hits[:4])}")
                    if score > best_score:
                        best_score = score
                        best_line = line_index
                if best_score:
                    key = (str(file_path), best_line, "test_coverage")
                    if key in seen:
                        continue
                    seen.add(key)
                    match = self._match_from_index_location(
                        entry,
                        connection,
                        str(file_path),
                        best_line,
                        score=best_score,
                        reason="test coverage evidence: " + "; ".join(list(dict.fromkeys(best_reasons))[:5]),
                        question=question,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "test_coverage",
                        retrieval="test_coverage",
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                    if match:
                        match["test_coverage"] = {
                            "terms": lookup_terms[:8],
                            "has_assertion": "assertion/verify present" in best_reasons,
                            "has_test_case": "test case present" in best_reasons,
                        }
                        matches.append(match)
            for term in lookup_terms[:10]:
                like_term = f"%{term}%"
                rows = connection.execute(
                    """
                    select * from references_index
                    where kind in ('test_subject', 'test_reference', 'test_assertion', 'test_case', 'call', 'import')
                      and lower_target like ?
                    limit 80
                    """,
                    (like_term,),
                ).fetchall()
                for row in rows:
                    file_path = str(row["file_path"])
                    if not self._is_test_file_path(file_path):
                        continue
                    line_no = int(row["line_no"] or 1)
                    key = (file_path, line_no, str(row["kind"] or ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    kind = str(row["kind"] or "")
                    score = 218 if kind in {"test_assertion", "test_subject", "test_reference"} else 196
                    if str(row["lower_target"] or "") == term:
                        score += 24
                    match = self._match_from_index_location(
                        entry,
                        connection,
                        file_path,
                        line_no,
                        score=score,
                        reason=f"test coverage evidence: {kind} {row['target']}",
                        question=question,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "test_coverage",
                        retrieval="test_coverage",
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                    if match:
                        match["test_coverage"] = {"kind": kind, "target": str(row["target"] or term)}
                        matches.append(match)
    except (OSError, sqlite3.Error):
        return []
    matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    return matches[:80]


def _tool_find_operational_boundaries(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    boundary_terms = [
        "transactional",
        "transaction",
        "rollback",
        "cacheable",
        "cacheevict",
        "cacheput",
        "cache",
        "async",
        "retryable",
        "retry",
        "circuitbreaker",
        "ratelimiter",
        "bulkhead",
        "timelimiter",
        "schedulerlock",
        "lock",
        "preauthorize",
        "postauthorize",
    ]
    lowered_terms = list(
        dict.fromkeys(
            term.lower()
            for term in [*terms, *self._question_tokens(question), *boundary_terms]
            if len(str(term).strip()) >= 3 and str(term).lower() not in STOPWORDS
        )
    )[:32]
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    index_path = self._index_path(repo_path)
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                select * from references_index
                where kind in ('operational_boundary', 'framework_binding', 'scheduled_job', 'web_interceptor', 'bean_condition')
                limit 240
                """
            ).fetchall()
            for row in rows:
                target = str(row["target"] or "")
                context = str(row["context"] or "")
                haystack = f"{target} {context} {row['file_path']}".lower()
                if lowered_terms and not any(term in haystack for term in lowered_terms):
                    continue
                line_no = int(row["line_no"] or 1)
                key = (str(row["file_path"]), line_no, str(row["kind"] or ""))
                if key in seen:
                    continue
                seen.add(key)
                kind = str(row["kind"] or "")
                score = 226 if kind == "operational_boundary" else 186
                if any(term in target.lower() for term in ("transactional", "cache", "async", "retry", "circuit", "rate", "lock", "authorize")):
                    score += 24
                match = self._match_from_index_location(
                    entry,
                    connection,
                    str(row["file_path"]),
                    line_no,
                    score=score,
                    reason=f"operational boundary evidence: {kind} {target}",
                    question=question,
                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}" if step_index else "operational_boundary",
                    retrieval="operational_boundary",
                    index_path=index_path,
                    request_cache=request_cache,
                )
                if match:
                    match["operational_boundary"] = {"kind": kind, "target": target}
                    matches.append(match)
    except (OSError, sqlite3.Error):
        return []
    matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    return matches[:80]


def _expand_impact_matches(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    question: str,
    base_matches: list[dict[str, Any]],
    limit: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    question_tokens = set(self._question_tokens(question))
    upstream_matches: list[dict[str, Any]] = []
    downstream_matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int, str, str]] = set()
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        repo_seed_matches = [match for match in base_matches if match.get("repo") == entry.display_name and match.get("path")]
        repo_seed_matches.sort(
            key=lambda item: (
                Path(str(item.get("path") or "")).stem.lower() in question_tokens,
                int(item.get("score") or 0),
            ),
            reverse=True,
        )
        seed_paths = list(dict.fromkeys(str(match.get("path") or "") for match in repo_seed_matches))[:8]
        if not seed_paths:
            continue
        index_path = self._index_path(repo_path)
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                def resolve_target_location(target_name: str) -> tuple[str, int] | None:
                    normalized = str(target_name or "").strip()
                    if not normalized:
                        return None
                    candidates = list(dict.fromkeys(
                        item.lower()
                        for item in (
                            normalized,
                            normalized.split(".")[-1],
                            re.sub(r"\b(get|set|find|create|update|delete|read|write)\b", "", normalized, flags=re.IGNORECASE).strip("."),
                        )
                        if item
                    ))
                    for candidate in candidates:
                        row = connection.execute(
                            """
                            select file_path, line_no from definitions
                            where lower_name = ?
                            order by
                                case
                                    when kind like '%class%' or kind in ('class', 'interface') then 0
                                    when kind like '%method%' then 1
                                    else 2
                                end,
                                line_no
                            limit 1
                            """,
                            (candidate,),
                        ).fetchone()
                        if row:
                            return str(row["file_path"] or ""), int(row["line_no"] or 1)
                        row = connection.execute(
                            """
                            select file_path, line_no from code_entities
                            where lower_name = ?
                            order by
                                case
                                    when kind like '%class%' or kind in ('class', 'interface') then 0
                                    when kind like '%method%' then 1
                                    else 2
                                end,
                                line_no
                            limit 1
                            """,
                            (candidate,),
                        ).fetchone()
                        if row:
                            return str(row["file_path"] or ""), int(row["line_no"] or 1)
                    return None

                for seed_path in seed_paths:
                    seed_exact = Path(seed_path).stem.lower() in question_tokens
                    rows = connection.execute(
                        """
                        select * from flow_edges
                        where from_file = ? or to_file = ?
                        order by
                            case
                                when to_file = ? and from_kind in ('controller', 'handler', 'consumer') then 0
                                when to_file = ? then 1
                                when from_file = ? and edge_kind in ('repository', 'mapper', 'dao', 'sql_table', 'db_read', 'db_write', 'client', 'route') then 2
                                when from_file = ? then 3
                                else 4
                            end,
                            from_line
                        limit 80
                        """,
                        (seed_path, seed_path, seed_path, seed_path, seed_path, seed_path),
                    ).fetchall()
                    for row in rows:
                        raw_upstream = str(row["to_file"] or "") == seed_path and str(row["from_file"] or "") != seed_path
                        from_kind = str(row["from_kind"] or "").lower()
                        upstream = raw_upstream and from_kind not in {
                            "repository",
                            "mapper",
                            "dao",
                            "client",
                            "integration",
                            "gateway",
                        }
                        if raw_upstream and not upstream:
                            file_path = str(row["from_file"] or "")
                        else:
                            file_path = str(row["from_file"] if upstream else row["to_file"] or row["from_file"])
                        if not file_path:
                            continue
                        line_no = int(row["from_line"] if upstream or (raw_upstream and not upstream) else row["to_line"] or row["from_line"] or 1)
                        if not upstream and not str(row["to_file"] or "").strip():
                            resolved = resolve_target_location(str(row["to_name"] or ""))
                            if resolved and resolved[0]:
                                file_path, line_no = resolved
                        edge_key = (
                            entry.display_name,
                            file_path,
                            line_no,
                            str(row["edge_kind"] or ""),
                            str(row["to_name"] or ""),
                        )
                        if edge_key in seen:
                            continue
                        seen.add(edge_key)
                        role = "upstream caller" if upstream else "downstream dependency"
                        path_lower = file_path.lower()
                        score = 214 if seed_exact else 190
                        if upstream and any(marker in path_lower for marker in ("controller", "handler", "consumer", "job")):
                            score += 20
                        if not upstream and any(marker in path_lower for marker in ("repository", "mapper", "dao", "client")):
                            score += 20
                        match = self._match_from_index_location(
                            entry,
                            connection,
                            file_path,
                            line_no,
                            score=score,
                            reason=f"impact {role}: {row['edge_kind']} {row['from_name']} -> {row['to_name']}",
                            question=question,
                            trace_stage="impact_analysis",
                            retrieval="planner_caller" if upstream else "planner_callee",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                        if match:
                            if upstream:
                                upstream_matches.append(match)
                            else:
                                downstream_matches.append(match)
        except (OSError, sqlite3.Error):
            continue
    upstream_matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    downstream_matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    max_items = max(6, min(int(limit or 12), 24))
    balanced: list[dict[str, Any]] = []
    seen_result: set[tuple[Any, Any, Any, Any]] = set()

    def add_result(match: dict[str, Any]) -> None:
        if len(balanced) >= max_items:
            return
        key_value = (match.get("repo"), match.get("path"), match.get("line_start"), match.get("line_end"))
        if key_value in seen_result:
            return
        balanced.append(match)
        seen_result.add(key_value)

    for bucket_limit, bucket in ((max_items // 2, upstream_matches), (max_items // 2, downstream_matches)):
        added = 0
        for match in bucket:
            before = len(balanced)
            add_result(match)
            if len(balanced) > before:
                added += 1
            if added >= max(1, bucket_limit):
                break
    for match in [*upstream_matches, *downstream_matches]:
        add_result(match)
        if len(balanced) >= max_items:
            break
    balanced.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    return balanced


def _tool_lookup_references_by_kind(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    *,
    kinds: set[str],
    question: str,
    trace_stage: str,
    retrieval: str,
    score: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index_path = self._index_path(repo_path)
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            placeholders = ",".join("?" for _ in kinds)
            kind_values = tuple(sorted(kinds))
            rows = connection.execute(
                f"select * from references_index where kind in ({placeholders}) limit 120",
                kind_values,
            ).fetchall()
            lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
            for row in rows:
                haystack = f"{row['target']} {row['context']} {row['file_path']}".lower()
                if lowered_terms and not any(term in haystack for term in lowered_terms):
                    if str(row["kind"]) == "sql_table" and not any(keyword in haystack for keyword in ("select", "from", "join", "update", "insert")):
                        continue
                matches.append(
                    self._match_from_index_location(
                        entry,
                        connection,
                        str(row["file_path"]),
                        int(row["line_no"]),
                        score=score,
                        reason=f"planner {retrieval}: {row['kind']} {row['target']}",
                        question=question,
                        trace_stage=trace_stage,
                        retrieval=retrieval,
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _tool_find_callers(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
    return self._tool_lookup_flow_edges(
        entry,
        repo_path,
        terms=terms,
        seed_paths=seed_paths,
        direction="callers",
        question=question,
        step_index=step_index,
        request_cache=request_cache,
    )


def _tool_find_callees(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    terms: list[str],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
    return self._tool_lookup_flow_edges(
        entry,
        repo_path,
        terms=terms,
        seed_paths=seed_paths,
        direction="callees",
        question=question,
        step_index=step_index,
        request_cache=request_cache,
    )


def _tool_lookup_flow_edges(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    *,
    terms: list[str],
    seed_paths: list[str],
    direction: str,
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index_path = self._index_path(repo_path)
    lowered_terms = [str(term).lower() for term in terms if len(str(term).strip()) >= 3]
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            rows: list[sqlite3.Row] = []
            if direction == "callers" and seed_paths:
                for path in seed_paths:
                    rows.extend(
                        connection.execute(
                            """
                            select * from flow_edges
                            where to_file = ?
                            order by
                                case edge_kind
                                    when 'route' then 0
                                    when 'controller' then 1
                                    when 'service' then 2
                                    when 'client' then 3
                                    when 'runtime_call' then 4
                                    else 5
                                end,
                                from_line
                            limit 80
                            """,
                            (path,),
                        ).fetchall()
                    )
            if direction == "callees" and seed_paths:
                for path in seed_paths:
                    rows.extend(
                        connection.execute(
                            """
                            select * from flow_edges
                            where from_file = ?
                            order by
                                case edge_kind
                                    when 'sql_table' then 0
                                    when 'repository' then 1
                                    when 'mapper' then 2
                                    when 'dao' then 3
                                    when 'client' then 4
                                    else 5
                                end,
                                from_line
                            limit 60
                            """,
                            (path,),
                        ).fetchall()
                    )
            if lowered_terms:
                for term in lowered_terms[:16]:
                    rows.extend(
                        connection.execute(
                            """
                            select * from flow_edges
                            where lower(to_name) like ? or lower(from_name) like ? or lower(evidence) like ?
                            limit 40
                            """,
                            (f"%{term}%", f"%{term}%", f"%{term}%"),
                        ).fetchall()
                    )
            seen_rows: set[tuple[Any, ...]] = set()
            for row in rows:
                row_key = (row["from_file"], row["from_line"], row["edge_kind"], row["to_name"], row["to_file"], row["to_line"])
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)
                if direction == "callers":
                    file_path = str(row["from_file"])
                    line_no = int(row["from_line"] or 1)
                    retrieval = "planner_caller"
                    score = 192 if str(row["from_kind"] or "").lower() in {"controller", "handler", "consumer"} else 184 if any(
                        marker in file_path.lower() for marker in ("controller", "handler", "consumer", "job", "scheduler")
                    ) else 176
                else:
                    file_path = str(row["to_file"] or row["from_file"])
                    line_no = int(row["to_line"] or row["from_line"] or 1)
                    retrieval = "planner_callee"
                    score = 176 if row["edge_kind"] in {"sql_table", "repository", "mapper", "dao", "client"} else 150
                matches.append(
                    self._match_from_index_location(
                        entry,
                        connection,
                        file_path,
                        line_no,
                        score=score,
                        reason=f"planner {direction}: {row['edge_kind']} {row['from_name']} -> {row['to_name']}",
                        question=question,
                        trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                        retrieval=retrieval,
                        index_path=index_path,
                        request_cache=request_cache,
                    )
                )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _tool_open_file_window(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index_path = self._index_path(repo_path)
    seeds = [match for match in base_matches if match.get("repo") == entry.display_name][:6]
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for seed in seeds:
                line_no = int(seed.get("line_start") or 1)
                match = self._match_from_index_location(
                    entry,
                    connection,
                    str(seed.get("path") or ""),
                    line_no,
                    score=max(120, int(seed.get("score") or 0) - 5),
                    reason=f"planner open file window: {seed.get('reason') or 'seed evidence'}",
                    question=question,
                    trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                    retrieval="open_file_window",
                    index_path=index_path,
                    request_cache=request_cache,
                )
                if match:
                    matches.append(match)
    except (OSError, sqlite3.Error):
        return []
    return matches


def _tool_lookup_structure(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    terms: list[str],
    *,
    question: str,
    table: str,
    name_column: str,
    lower_column: str,
    line_column: str,
    kind_column: str,
    trace_stage: str,
    retrieval: str,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    index_path = self._index_path(repo_path)
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for term in terms[:16]:
                lowered = str(term).lower()
                if len(lowered) < 3:
                    continue
                for row in connection.execute(
                    f"select * from {table} where {lower_column} like ? limit 20",
                    (f"%{lowered}%",),
                ):
                    matches.append(
                        self._match_from_index_location(
                            entry,
                            connection,
                            str(row["file_path"]),
                            int(row[line_column]),
                            score=170 if str(row[lower_column]) == lowered else 132,
                            reason=f"planner {retrieval}: {row[kind_column]} {row[name_column]}",
                            question=question,
                            trace_stage=trace_stage,
                            retrieval=retrieval,
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                    )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _tool_trace_graph(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    index_path = self._index_path(repo_path)
    seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:8]
    matches: list[dict[str, Any]] = []
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for path in seed_paths:
                for row in connection.execute(
                    """
                    select * from graph_edges
                    where from_file = ? or to_file = ?
                    limit 30
                    """,
                    (path, path),
                ):
                    target_path = str(row["to_file"] if row["from_file"] == path else row["from_file"])
                    target_line = int(row["to_line"] if row["from_file"] == path else row["from_line"])
                    matches.append(
                        self._match_from_index_location(
                            entry,
                            connection,
                            target_path,
                            target_line,
                            score=150,
                            reason=f"planner graph trace: {row['edge_kind']} {row['symbol']}",
                            question=question,
                            trace_stage=f"tool_loop_{step_index}",
                            retrieval="code_graph",
                            index_path=index_path,
                            request_cache=request_cache,
                        )
                    )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _tool_trace_flow(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    index_path = self._index_path(repo_path)
    seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
    matches: list[dict[str, Any]] = []
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for path in seed_paths:
                for row in connection.execute(
                    """
                    select * from flow_edges
                    where from_file = ? or to_file = ?
                    order by
                        case edge_kind
                            when 'sql_table' then 0
                            when 'repository' then 1
                            when 'mapper' then 2
                            when 'dao' then 3
                            when 'field_population' then 4
                            when 'client' then 5
                            when 'service' then 6
                            else 6
                        end,
                        from_line
                    limit 40
                    """,
                    (path, path),
                ):
                    target_path = str(row["to_file"] or "")
                    target_line = int(row["to_line"] or 0)
                    if target_path and target_path != path:
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                target_path,
                                target_line,
                                score=172 if row["edge_kind"] in {"repository", "mapper", "dao", "sql_table"} else 158,
                                reason=f"planner flow trace: {row['edge_kind']} {row['to_name']}",
                                question=question,
                                trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                retrieval="flow_graph",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                        )
                    else:
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                str(row["from_file"]),
                                int(row["from_line"]),
                                score=166 if row["edge_kind"] == "sql_table" else 148,
                                reason=f"planner flow trace: {row['edge_kind']} {row['to_name']}",
                                question=question,
                                trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                retrieval="flow_graph",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                        )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _tool_trace_entity(
    self,
    entry: RepositoryEntry,
    repo_path: Path,
    base_matches: list[dict[str, Any]],
    question: str,
    step_index: int,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    index_path = self._index_path(repo_path)
    seed_paths = [str(match.get("path") or "") for match in base_matches if match.get("repo") == entry.display_name][:12]
    matches: list[dict[str, Any]] = []
    try:
        self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            for path in seed_paths:
                for row in connection.execute(
                    """
                    select * from entity_edges
                    where from_file = ? or to_file = ?
                    order by
                        case edge_kind
                            when 'sql_table' then 0
                            when 'route' then 1
                            when 'data_flow' then 2
                            when 'injects' then 3
                            when 'call' then 4
                            else 4
                        end,
                        from_line
                    limit 50
                    """,
                    (path, path),
                ):
                    target_path = str(row["to_file"] or "")
                    target_line = int(row["to_line"] or 0)
                    if target_path and target_path != path:
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                target_path,
                                target_line,
                                score=176 if row["edge_kind"] in {"sql_table", "injects", "call"} else 154,
                                reason=f"planner entity trace: {row['edge_kind']} {row['to_name']}",
                                question=question,
                                trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                retrieval="entity_graph",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                        )
                    else:
                        matches.append(
                            self._match_from_index_location(
                                entry,
                                connection,
                                str(row["from_file"]),
                                int(row["from_line"]),
                                score=156,
                                reason=f"planner entity trace: {row['edge_kind']} {row['to_name']}",
                                question=question,
                                trace_stage=f"{TOOL_LOOP_TRACE_PREFIX}{step_index}",
                                retrieval="entity_graph",
                                index_path=index_path,
                                request_cache=request_cache,
                            )
                        )
    except (OSError, sqlite3.Error):
        return []
    return [match for match in matches if match]


def _match_from_index_location(
    self,
    entry: RepositoryEntry,
    connection: sqlite3.Connection,
    file_path: str,
    line_no: int,
    *,
    score: int,
    reason: str,
    question: str,
    trace_stage: str,
    retrieval: str,
    index_path: Path | None = None,
    request_cache: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    lines: list[str] = []
    if index_path is not None and request_cache is not None:
        lines = self._cached_file_lines(
            connection,
            index_path,
            file_path,
            request_cache=request_cache,
        )
    if not lines:
        rows = connection.execute(
            "select line_text from lines where file_path = ? order by line_no",
            (file_path,),
        ).fetchall()
        lines = [str(row["line_text"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows]
    if not lines:
        return None
    start, end = self._best_snippet_window(lines, max(1, min(line_no, len(lines))))
    return {
        "repo": entry.display_name,
        "path": file_path,
        "line_start": start,
        "line_end": end,
        "score": score,
        "snippet": "\n".join(lines[start - 1 : end]).strip()[:2400],
        "reason": reason,
        "trace_stage": trace_stage,
        "retrieval": retrieval,
    }


def _trace_paths_cache_key(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    matches: list[dict[str, Any]],
    limit: int,
) -> str:
    repo_fingerprints = []
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        repo_fingerprints.append(
            {
                "name": entry.display_name,
                "url": entry.url,
                "path": str(repo_path),
                "index": self._index_fingerprint(self._index_path(repo_path)),
            }
        )
    payload = {
        "repos": repo_fingerprints,
        "matches": self._match_cache_signature(matches),
        "limit": max(1, int(limit or 6)),
    }
    return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _build_trace_paths(
    self,
    *,
    entries: list[RepositoryEntry],
    key: str,
    matches: list[dict[str, Any]],
    question: str,
    limit: int = 6,
    request_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    del question
    if not matches:
        return []
    cache_key = self._trace_paths_cache_key(entries=entries, key=key, matches=matches, limit=limit)
    if request_cache is not None:
        trace_cache = request_cache.setdefault("trace_paths", {})
        cached = trace_cache.get(cache_key)
        if cached is not None:
            self._increment_retrieval_stat(request_cache, "trace_paths_hits")
            return self._clone_jsonish(cached)
        self._increment_retrieval_stat(request_cache, "trace_paths_misses")
    paths: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        seed_paths = [str(match.get("path") or "") for match in matches if match.get("repo") == entry.display_name][:10]
        if not seed_paths:
            continue
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                edge_cache: dict[str, list[dict[str, Any]]] = {}

                def edges_for(seed_path: str) -> list[dict[str, Any]]:
                    cached_edges = edge_cache.get(seed_path)
                    if cached_edges is not None:
                        return cached_edges
                    edge_cache[seed_path] = self._trace_path_edges_for_seed(connection, seed_path)
                    return edge_cache[seed_path]

                for seed in seed_paths:
                    first_hops = edges_for(seed)
                    for first in first_hops[:10]:
                        path = self._trace_path_from_edges(entry.display_name, seed, [first])
                        signature = json.dumps(path.get("edges") or [], sort_keys=True)
                        if signature not in seen_signatures:
                            paths.append(path)
                            seen_signatures.add(signature)
                        next_seed = str(first.get("to_file") or "")
                        if not next_seed:
                            continue
                        for second in edges_for(next_seed)[:6]:
                            if second.get("from_file") == first.get("from_file") and second.get("to_file") == first.get("to_file"):
                                continue
                            extended = self._trace_path_from_edges(entry.display_name, seed, [first, second])
                            signature = json.dumps(extended.get("edges") or [], sort_keys=True)
                            if signature not in seen_signatures:
                                paths.append(extended)
                                seen_signatures.add(signature)
        except (OSError, sqlite3.Error):
            continue
    paths.sort(key=lambda item: item.get("confidence", 0), reverse=True)
    result = paths[: max(1, int(limit or 6))]
    if request_cache is not None:
        request_cache.setdefault("trace_paths", {})[cache_key] = self._clone_jsonish(result)
    return result


def _build_repo_dependency_graph(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    request_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    nodes = [{"name": entry.display_name, "url": entry.url} for entry in entries]
    edge_rows: list[dict[str, Any]] = []
    route_index = self._repo_route_index(key=key, entries=entries, request_cache=request_cache)
    config_index = self._repo_config_index(key=key, entries=entries, request_cache=request_cache)
    message_index = self._repo_message_index(key=key, entries=entries, config_index=config_index, request_cache=request_cache)
    artifact_index = self._repo_artifact_index(key=key, entries=entries, request_cache=request_cache)
    table_index = self._repo_table_index(key=key, entries=entries, request_cache=request_cache)
    for source in entries:
        source_path = self._repo_path(key, source)
        if not (source_path / ".git").exists():
            continue
        try:
            self._ensure_repo_index_cached(source, source_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(source_path)) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line, to_file, to_line
                    from flow_edges
                    where edge_kind in (
                        'client', 'route', 'framework', 'call', 'import', 'module_dependency',
                        'message_publish', 'event_publish', 'db_write'
                    )
                    limit 300
                    """
                ).fetchall()
        except (OSError, sqlite3.Error):
            continue
        for row in rows:
            candidate = self._match_repo_dependency_candidate(
                row=dict(row),
                entries=entries,
                source_name=source.display_name,
                route_index=route_index,
                message_index=message_index,
                artifact_index=artifact_index,
                table_index=table_index,
                source_config=config_index.get(source.display_name) or {},
            )
            if not candidate:
                continue
            edge_rows.append(
                {
                    "from_repo": source.display_name,
                    "to_repo": candidate["target"].display_name,
                    "edge_kind": candidate.get("edge_kind") or str(row["edge_kind"] or "dependency"),
                    "confidence": candidate["confidence"],
                    "match_reason": candidate["match_reason"],
                    "evidence": str(row["evidence"] or row["to_name"] or "")[:300],
                    "from_file": str(row["from_file"] or ""),
                    "from_line": int(row["from_line"] or 0),
                    "to_file": str(candidate.get("to_file") or row["to_file"] or ""),
                    "to_line": int(candidate.get("to_line") or row["to_line"] or 0),
                }
            )
    by_signature: dict[str, dict[str, Any]] = {}
    for edge in edge_rows:
        signature = json.dumps(
            {
                "from_repo": edge.get("from_repo"),
                "to_repo": edge.get("to_repo"),
                "edge_kind": edge.get("edge_kind"),
                "from_file": edge.get("from_file"),
                "from_line": edge.get("from_line"),
                "to_file": edge.get("to_file"),
                "to_line": edge.get("to_line"),
            },
            sort_keys=True,
        )
        existing = by_signature.get(signature)
        if existing is None or float(edge.get("confidence") or 0) > float(existing.get("confidence") or 0):
            by_signature[signature] = edge
    deduped = sorted(by_signature.values(), key=lambda item: float(item.get("confidence") or 0), reverse=True)
    return {"version": 2, "nodes": nodes, "edges": deduped[:80]}


def _repo_message_index(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    config_index: dict[str, dict[str, list[str]]] | None = None,
    request_cache: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    message_index: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        messages: list[dict[str, Any]] = []
        repo_config = (config_index or {}).get(entry.display_name) or {}
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind in ('message_consume', 'event_consume')
                    limit 300
                    """
                ):
                    for message_name in self._message_values_from_config(str(row["to_name"] or ""), repo_config):
                        messages.append(
                            {
                                "message": message_name,
                                "edge_kind": str(row["edge_kind"] or ""),
                                "file": str(row["from_file"] or ""),
                                "line": int(row["from_line"] or 0),
                                "evidence": str(row["evidence"] or ""),
                            }
                        )
        except (OSError, sqlite3.Error):
            messages = []
        message_index[entry.display_name] = messages
    return message_index


def _repo_artifact_index(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    request_cache: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    artifact_index: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        artifacts: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind = 'module_dependency'
                      and (evidence = to_name or from_file like '%pom.xml' or from_file like '%package.json')
                    limit 300
                    """
                ):
                    artifacts.append(
                        {
                            "artifact": str(row["to_name"] or ""),
                            "file": str(row["from_file"] or ""),
                            "line": int(row["from_line"] or 0),
                            "evidence": str(row["evidence"] or ""),
                        }
                    )
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind = 'module_dependency'
                    limit 300
                    """
                ):
                    if str(row["evidence"] or "") != str(row["to_name"] or ""):
                        continue
                    artifacts.append(
                        {
                            "artifact": str(row["to_name"] or ""),
                            "file": str(row["from_file"] or ""),
                            "line": int(row["from_line"] or 0),
                            "evidence": str(row["evidence"] or ""),
                        }
                    )
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind = 'module_dependency'
                       or edge_kind = 'module_artifact'
                    limit 300
                    """
                ):
                    artifacts.append(
                        {
                            "artifact": str(row["to_name"] or ""),
                            "file": str(row["from_file"] or ""),
                            "line": int(row["from_line"] or 0),
                            "evidence": str(row["evidence"] or ""),
                        }
                    )
        except (OSError, sqlite3.Error):
            artifacts = []
        by_key: dict[str, dict[str, Any]] = {}
        for item in artifacts:
            normalized = self._normalize_artifact_name(str(item.get("artifact") or ""))
            if normalized and normalized not in by_key:
                by_key[normalized] = item
        artifact_index[entry.display_name] = list(by_key.values())
    return artifact_index


def _repo_table_index(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    request_cache: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    table_index: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        tables: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind in ('db_read', 'db_write')
                    limit 300
                    """
                ):
                    tables.append(
                        {
                            "table": str(row["to_name"] or ""),
                            "edge_kind": str(row["edge_kind"] or ""),
                            "file": str(row["from_file"] or ""),
                            "line": int(row["from_line"] or 0),
                            "evidence": str(row["evidence"] or ""),
                        }
                    )
        except (OSError, sqlite3.Error):
            tables = []
        table_index[entry.display_name] = tables
    return table_index


def _repo_route_index(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    request_cache: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    route_index: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        routes: list[dict[str, Any]] = []
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    """
                    select edge_kind, to_name, evidence, from_file, from_line
                    from flow_edges
                    where edge_kind = 'route'
                    limit 300
                    """
                ):
                    routes.append(
                        {
                            "route": str(row["to_name"] or ""),
                            "file": str(row["from_file"] or ""),
                            "line": int(row["from_line"] or 0),
                            "evidence": str(row["evidence"] or ""),
                        }
                    )
        except (OSError, sqlite3.Error):
            routes = []
        route_index[entry.display_name] = self._prefer_specific_routes(routes)
    return route_index


def _repo_config_index(
    self,
    *,
    key: str,
    entries: list[RepositoryEntry],
    request_cache: dict[str, Any] | None = None,
) -> dict[str, dict[str, list[str]]]:
    config_index: dict[str, dict[str, list[str]]] = {}
    for entry in entries:
        repo_path = self._repo_path(key, entry)
        if not (repo_path / ".git").exists():
            continue
        values: dict[str, list[str]] = {}
        yaml_stacks: dict[str, list[tuple[int, str]]] = {}
        try:
            self._ensure_repo_index_cached(entry, repo_path, request_cache=request_cache)
            with sqlite3.connect(self._index_path(repo_path)) as connection:
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    """
                    select file_path, line_no, line_text
                    from lines
                    where lower(file_path) glob '*.properties'
                       or lower(file_path) glob '*.yml'
                       or lower(file_path) glob '*.yaml'
                       or lower(file_path) glob '*.conf'
                       or lower(file_path) glob '*.toml'
                    order by file_path, line_no
                    limit 1000
                    """
                ):
                    file_path = str(row["file_path"] or "")
                    suffix = Path(file_path).suffix.lower()
                    yaml_stack = yaml_stacks.setdefault(file_path, [])
                    pair = (
                        self._extract_yaml_config_assignment(str(row["line_text"] or ""), yaml_stack)
                        if suffix in {".yaml", ".yml"}
                        else self._extract_config_assignment(str(row["line_text"] or ""))
                    )
                    if not pair:
                        continue
                    key_name, value = pair
                    if not value:
                        continue
                    values.setdefault(key_name, [])
                    if value not in values[key_name]:
                        values[key_name].append(value)
        except (OSError, sqlite3.Error):
            values = {}
        config_index[entry.display_name] = values
    return config_index


def _match_repo_dependency_candidate(
    self,
    *,
    row: dict[str, Any],
    entries: list[RepositoryEntry],
    source_name: str,
    route_index: dict[str, list[dict[str, Any]]],
    message_index: dict[str, list[dict[str, Any]]] | None = None,
    artifact_index: dict[str, list[dict[str, Any]]] | None = None,
    table_index: dict[str, list[dict[str, Any]]] | None = None,
    source_config: dict[str, list[str]] | None = None,
) -> dict[str, Any] | None:
    value = str(row.get("to_name") or row.get("evidence") or "")
    evidence = str(row.get("evidence") or "")
    search_text = f"{value} {evidence}"
    best: dict[str, Any] | None = None
    source_role = self._flow_role_for_path(str(row.get("from_file") or ""))
    row_kind = str(row.get("edge_kind") or "")
    lowered_search_text = search_text.lower()

    http_client_like = source_role == "client" or any(
        marker in lowered_search_text for marker in ("feignclient", "fetch", "axios", "resttemplate", "webclient")
    )
    resolved_config_values = self._resolve_config_placeholders(search_text, source_config or {})
    if http_client_like:
        resolved_config_values.extend(self._candidate_dependency_config_values(source_config or {}))
        resolved_config_values = list(dict.fromkeys(resolved_config_values))[:20]
    if resolved_config_values:
        search_text = " ".join([search_text, *resolved_config_values, *self._join_config_routes(search_text, resolved_config_values)])
        lowered_search_text = search_text.lower()
    if http_client_like:
        for route in self._extract_route_literals(search_text):
            for entry in entries:
                if entry.display_name == source_name:
                    continue
                for target_route in route_index.get(entry.display_name) or []:
                    score = self._route_overlap_score(route, str(target_route.get("route") or ""))
                    if score <= 0:
                        continue
                    candidate = {
                        "target": entry,
                        "edge_kind": "http_path",
                        "confidence": score,
                        "match_reason": f"http path overlap: {route} -> {target_route.get('route')}",
                        "from_route": route,
                        "target_route": target_route.get("route") or "",
                        "to_file": target_route.get("file") or "",
                        "to_line": int(target_route.get("line") or 0),
                    }
                    best = self._better_repo_dependency_candidate(best, candidate)

    if row_kind in {"module_dependency"}:
        source_artifacts = [self._normalize_artifact_name(item) for item in self._artifact_values_from_text(search_text)]
        source_artifacts = [item for item in dict.fromkeys(source_artifacts) if item]
        for source_artifact in source_artifacts:
            for entry in entries:
                if entry.display_name == source_name:
                    continue
                for target_artifact in (artifact_index or {}).get(entry.display_name) or []:
                    target_name = str(target_artifact.get("artifact") or "")
                    if source_artifact != self._normalize_artifact_name(target_name):
                        continue
                    candidate = {
                        "target": entry,
                        "edge_kind": "module_dependency",
                        "confidence": 0.97,
                        "match_reason": f"exact build artifact match: {source_artifact}",
                        "to_file": target_artifact.get("file") or "",
                        "to_line": int(target_artifact.get("line") or 0),
                    }
                    best = self._better_repo_dependency_candidate(best, candidate)

    if row_kind in {"db_write"}:
        source_table = self._normalize_table_name(value)
        if source_table:
            for entry in entries:
                if entry.display_name == source_name:
                    continue
                for target_table in (table_index or {}).get(entry.display_name) or []:
                    if str(target_table.get("edge_kind") or "") != "db_read":
                        continue
                    target_name = str(target_table.get("table") or "")
                    if source_table != self._normalize_table_name(target_name):
                        continue
                    candidate = {
                        "target": entry,
                        "edge_kind": "shared_table",
                        "confidence": 0.86,
                        "match_reason": f"db write/read table overlap: {value} -> {target_name}",
                        "to_file": target_table.get("file") or "",
                        "to_line": int(target_table.get("line") or 0),
                    }
                    best = self._better_repo_dependency_candidate(best, candidate)

    if row_kind in {"message_publish", "event_publish"}:
        source_messages = self._message_values_from_config(value, source_config or {})
        for source_message_value in source_messages:
            source_message = self._normalize_message_name(source_message_value)
            if not source_message:
                continue
            for entry in entries:
                if entry.display_name == source_name:
                    continue
                for target_message in (message_index or {}).get(entry.display_name) or []:
                    target_name = str(target_message.get("message") or "")
                    if source_message != self._normalize_message_name(target_name):
                        continue
                    candidate = {
                        "target": entry,
                        "edge_kind": "message_topic" if row_kind == "message_publish" else "event_flow",
                        "confidence": 0.93,
                        "match_reason": f"{row_kind} matches consumer: {source_message_value} -> {target_name}",
                        "to_file": target_message.get("file") or "",
                        "to_line": int(target_message.get("line") or 0),
                    }
                    best = self._better_repo_dependency_candidate(best, candidate)

    alias_client_like = http_client_like or row_kind in {"import", "module_dependency"}
    if alias_client_like:
        for entry in entries:
            if entry.display_name == source_name:
                continue
            alias_score = self._repo_alias_match_score(search_text, entry)
            if alias_score <= 0:
                continue
            candidate = {
                "target": entry,
                "edge_kind": str(row.get("edge_kind") or "dependency"),
                "confidence": alias_score,
                "match_reason": "build dependency alias match" if row_kind == "module_dependency" else "service/import alias match",
                "to_file": "",
                "to_line": 0,
            }
            best = self._better_repo_dependency_candidate(best, candidate)

    return best


def _normalize_message_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    normalized = re.sub(r"^\$\{([^}:]+).*$", r"\1", normalized)
    return re.sub(r"[^a-z0-9_.:-]+", "", normalized)


def _message_values_from_config(value: str, config_values: dict[str, list[str]]) -> list[str]:
    raw_value = str(value or "").strip()
    values: list[str] = []
    for key in CONFIG_PLACEHOLDER_PATTERN.findall(str(value or "")):
        values.extend(config_values.get(key, [])[:5])
    values.extend(config_values.get(raw_value, [])[:5])
    values.append(raw_value)
    return list(dict.fromkeys(item for item in values if item))[:8]


def _artifact_values_from_text(value: str) -> list[str]:
    text = str(value or "")
    artifacts: list[str] = []
    for coordinate in re.findall(r"([A-Za-z0-9_.@/-]+:[A-Za-z0-9_.@/-]+)", text):
        artifacts.append(coordinate)
        artifacts.append(coordinate.rsplit(":", 1)[-1])
    for package_name in re.findall(r"@[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", text):
        artifacts.append(package_name)
    for token in re.findall(r"\b[A-Za-z0-9_.-]+(?:-api|-sdk|-client|-service)\b", text):
        artifacts.append(token)
    return list(dict.fromkeys(item.strip() for item in artifacts if item.strip()))[:12]


def _normalize_artifact_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if ":" in normalized:
        normalized = normalized.rsplit(":", 1)[-1]
    if "/" in normalized:
        normalized = normalized.rsplit("/", 1)[-1]
    return re.sub(r"[^a-z0-9_.-]+", "", normalized)


def _normalize_table_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.rsplit(".", 1)[-1]
    return re.sub(r"[^a-z0-9_]+", "", normalized)


def _better_repo_dependency_candidate(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if current is None:
        return candidate
    candidate_confidence = float(candidate.get("confidence") or 0)
    current_confidence = float(current.get("confidence") or 0)
    if candidate_confidence > current_confidence:
        return candidate
    if candidate_confidence == current_confidence:
        candidate_specificity = SourceCodeQAService._route_segment_count(str(candidate.get("from_route") or "")) + SourceCodeQAService._route_segment_count(str(candidate.get("target_route") or ""))
        current_specificity = SourceCodeQAService._route_segment_count(str(current.get("from_route") or "")) + SourceCodeQAService._route_segment_count(str(current.get("target_route") or ""))
        if candidate_specificity > current_specificity:
            return candidate
    return current


def _extract_route_literals(value: str) -> list[str]:
    routes = []
    for route in re.findall(r"https?://[^\s\"'<>),]+", str(value or "")):
        if route not in routes:
            routes.append(route)
    for route in HTTP_LITERAL_PATTERN.findall(str(value or "")):
        if route not in routes:
            routes.append(route)
    for route in re.findall(r"(?<![A-Za-z0-9_])/[A-Za-z0-9_./{}:-]{2,}", str(value or "")):
        if route not in routes:
            routes.append(route)
    return routes[:12]


def _prefer_specific_routes(routes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    preferred: list[dict[str, Any]] = []
    route_values = [str(route.get("route") or "") for route in routes]
    for route in routes:
        value = str(route.get("route") or "")
        normalized = "/" + value.strip("/")
        if SourceCodeQAService._route_segment_count(normalized) <= 1 and any(
            SourceCodeQAService._route_segment_count(other) > 1
            and ("/" + str(other).strip("/")).endswith(normalized)
            for other in route_values
            if str(other or "") != value
        ):
            continue
        preferred.append(route)
    return preferred


def _route_segment_count(route: str) -> int:
    return len([part for part in SourceCodeQAService._route_path(route).split("/") if part])


def _join_config_routes(search_text: str, config_values: list[str]) -> list[str]:
    joined: list[str] = []
    relative_routes = [
        route
        for route in SourceCodeQAService._extract_route_literals(search_text)
        if route.startswith("/") and SourceCodeQAService._route_segment_count(route) <= 2
    ]
    for value in config_values:
        parsed = urlsplit(str(value or ""))
        base_path = parsed.path if parsed.scheme else str(value or "")
        if not base_path:
            continue
        for route in relative_routes:
            combined = SourceCodeQAService._join_routes(base_path, route)
            if combined and combined not in joined:
                joined.append(combined)
    return joined[:12]


def _resolve_config_placeholders(value: str, config_values: dict[str, list[str]]) -> list[str]:
    resolved: list[str] = []
    for key in CONFIG_PLACEHOLDER_PATTERN.findall(str(value or "")):
        for candidate in config_values.get(key, [])[:5]:
            if candidate and candidate not in resolved:
                resolved.append(candidate)
    return resolved[:12]


def _candidate_dependency_config_values(config_values: dict[str, list[str]]) -> list[str]:
    values: list[str] = []
    for key, candidates in config_values.items():
        lowered_key = str(key or "").lower()
        if not any(marker in lowered_key for marker in ("url", "uri", "endpoint", "host", "service", "client")):
            continue
        for candidate in candidates[:5]:
            lowered = str(candidate or "").lower()
            if candidate and ("http" in lowered or "/" in lowered or "-service" in lowered):
                values.append(candidate)
    return values[:20]


def _route_overlap_score(left: str, right: str) -> float:
    left_norm = SourceCodeQAService._route_path(left)
    right_norm = SourceCodeQAService._route_path(right)
    if len(left_norm) < 3 or len(right_norm) < 3:
        return 0.0
    if left_norm == right_norm:
        return 0.96
    if left_norm.endswith(right_norm) or right_norm.endswith(left_norm):
        return 0.88
    left_parts = {part for part in left_norm.lower().split("/") if part and not part.startswith("{")}
    right_parts = {part for part in right_norm.lower().split("/") if part and not part.startswith("{")}
    if not left_parts or not right_parts:
        return 0.0
    overlap = left_parts & right_parts
    if len(overlap) >= 2:
        return 0.78
    return 0.0


def _route_path(route: str) -> str:
    value = str(route or "").split("?", 1)[0].strip()
    parsed = urlsplit(value)
    if parsed.scheme and parsed.path:
        value = parsed.path
    return "/" + value.strip("/")


def _join_routes(prefix: str, suffix: str) -> str:
    prefix = str(prefix or "").split("?", 1)[0].strip()
    suffix = str(suffix or "").split("?", 1)[0].strip()
    if not prefix:
        return suffix
    if not suffix:
        return prefix
    if prefix.startswith("http") or suffix.startswith("http"):
        return suffix if suffix.startswith("http") else prefix.rstrip("/") + "/" + suffix.lstrip("/")
    return "/" + "/".join(part.strip("/") for part in (prefix, suffix) if part.strip("/"))


def _repo_alias_match_score(value: str, entry: RepositoryEntry) -> float:
    normalized_value = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
    if not normalized_value:
        return 0.0
    aliases = {
        re.sub(r"[^a-z0-9]+", "", entry.display_name.lower()),
        re.sub(r"[^a-z0-9]+", "", SourceCodeQAService._derive_display_name(entry.url).lower()),
    }
    ignored_parts = {"service", "repo", "repository", "portal", "client", "api", "team"}
    for raw_part in re.split(r"[^A-Za-z0-9]+", entry.display_name):
        normalized_part = raw_part.lower()
        if len(normalized_part) >= 8 and normalized_part not in ignored_parts:
            aliases.add(normalized_part)
    for candidate in aliases:
        if len(candidate) >= 4 and candidate in normalized_value:
            return 0.84
        if len(candidate) >= 6 and normalized_value in candidate:
            return 0.72
    return 0.0


def _trace_path_edges_for_seed(connection: sqlite3.Connection, seed_path: str) -> list[dict[str, Any]]:
    edge_rank = {
        "sql_table": 0,
        "client": 1,
        "mapper": 2,
        "dao": 3,
        "repository": 4,
        "field_population": 5,
        "service": 6,
        "route": 7,
    }
    rows: list[sqlite3.Row] = []
    seen: set[tuple[str, int, str, int, str]] = set()
    for clause, params in (
        ("from_file = ?", (seed_path,)),
        ("to_file = ? and from_file <> ?", (seed_path, seed_path)),
    ):
        for row in connection.execute(
            f"select * from flow_edges where {clause} limit 30",
            params,
        ).fetchall():
            key = (
                str(row["from_file"] or ""),
                int(row["from_line"] or 0),
                str(row["to_file"] or ""),
                int(row["to_line"] or 0),
                str(row["edge_kind"] or ""),
            )
            if key in seen:
                continue
            rows.append(row)
            seen.add(key)
    rows.sort(key=lambda row: (edge_rank.get(str(row["edge_kind"] or ""), 7), int(row["from_line"] or 0)))
    rows = rows[:30]
    edges: list[dict[str, Any]] = []
    for row in rows:
        edge = dict(row)
        if edge.get("to_file") == seed_path and edge.get("from_file") != seed_path:
            edge = {
                **edge,
                "from_file": edge.get("to_file"),
                "from_line": edge.get("to_line"),
                "to_file": edge.get("from_file"),
                "to_line": edge.get("from_line"),
                "to_name": edge.get("from_name"),
                "evidence": f"reverse trace: {edge.get('evidence')}",
            }
        edges.append(edge)
    return edges


def _trace_path_from_edges(repo_name: str, seed_path: str, edges: list[dict[str, Any]]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = [{"path": seed_path, "kind": SourceCodeQAService._flow_role_for_path(seed_path), "name": SourceCodeQAService._flow_name_for_path(seed_path)}]
    normalized_edges: list[dict[str, Any]] = []
    confidence = 0
    for edge in edges:
        edge_kind = str(edge.get("edge_kind") or "call")
        to_file = str(edge.get("to_file") or "")
        to_name = str(edge.get("to_name") or "")
        node = {
            "path": to_file,
            "line": int(edge.get("to_line") or 0),
            "kind": SourceCodeQAService._flow_role_for_path(to_file) if to_file else edge_kind,
            "name": to_name or SourceCodeQAService._flow_name_for_path(to_file),
        }
        nodes.append(node)
        normalized_edges.append(
            {
                "from_file": edge.get("from_file"),
                "from_line": edge.get("from_line"),
                "edge_kind": edge_kind,
                "to_name": to_name,
                "to_file": to_file,
                "to_line": edge.get("to_line"),
                "evidence": edge.get("evidence"),
            }
        )
        confidence += {
            "route": 20,
            "service": 18,
            "repository": 24,
            "mapper": 24,
            "dao": 24,
            "sql_table": 70,
            "client": 55,
            "field_population": 26,
            "framework": 16,
        }.get(edge_kind, 10)
    return {
        "repo": repo_name,
        "seed_path": seed_path,
        "nodes": nodes,
        "edges": normalized_edges,
        "confidence": confidence,
        "missing_hop": "" if edges else "no graph edge found from seed",
    }


_CLASS_METHODS = {
    "_exact_lookup_miss_should_stop",
}

_STATIC_METHODS = {
    "_annotate_duplicate_tool_match",
    "_artifact_values_from_text",
    "_better_repo_dependency_candidate",
    "_candidate_dependency_config_values",
    "_derive_display_name",
    "_entry_to_dict",
    "_exact_lookup_is_sufficient",
    "_extract_exact_lookup_terms",
    "_extract_route_literals",
    "_is_dependency_question",
    "_is_large_index_file",
    "_is_strict_exact_lookup_term",
    "_is_test_file_path",
    "_join_config_routes",
    "_join_routes",
    "_match_reason",
    "_message_values_from_config",
    "_normalize_artifact_name",
    "_normalize_message_name",
    "_normalize_table_name",
    "_prefer_specific_routes",
    "_question_tokens",
    "_question_tokens_cached",
    "_remove_incomplete_repo_dir",
    "_repo_alias_match_score",
    "_requires_cross_repo_context",
    "_resolve_config_placeholders",
    "_route_overlap_score",
    "_route_path",
    "_route_segment_count",
    "_safe_key",
    "_structure_lookup_query_terms",
    "_tool_step_signature",
    "_trace_path_edges_for_seed",
    "_trace_path_from_edges",
}

def attach_retrieval_tool_helpers(cls: type, global_context: dict[str, object]) -> None:
    helpers = {
        "_search_repo_index": _search_repo_index,
        "_persistent_index_matches_from_hits": _persistent_index_matches_from_hits,
        "_cached_index_rows": _cached_index_rows,
        "_is_large_index_file": _is_large_index_file,
        "_structure_lookup_query_terms": _structure_lookup_query_terms,
        "_targeted_index_rows": _targeted_index_rows,
        "_targeted_semantic_rows_by_id": _targeted_semantic_rows_by_id,
        "_cached_file_lines": _cached_file_lines,
        "_file_fts_search_rows": _file_fts_search_rows,
        "_fts_search_rows": _fts_search_rows,
        "_semantic_fts_search_rows": _semantic_fts_search_rows,
        "_cached_structure_like_rows": _cached_structure_like_rows,
        "_semantic_chunk_matches": _semantic_chunk_matches,
        "_semantic_query_terms": _semantic_query_terms,
        "_extract_exact_lookup_terms": _extract_exact_lookup_terms,
        "_exact_lookup_is_sufficient": _exact_lookup_is_sufficient,
        "_exact_lookup_miss_should_stop": _exact_lookup_miss_should_stop,
        "_is_strict_exact_lookup_term": _is_strict_exact_lookup_term,
        "_exact_table_path_lookup_repo": _exact_table_path_lookup_repo,
        "_index_fingerprint": _index_fingerprint,
        "_search_cache_key": _search_cache_key,
        "_ensure_repo_index_cached": _ensure_repo_index_cached,
        "_search_repo": _search_repo,
        "_iter_text_files": _iter_text_files,
        "_repo_path": _repo_path,
        "_safe_key": _safe_key,
        "_normalize_entry": _normalize_entry,
        "_entry_to_dict": _entry_to_dict,
        "_derive_display_name": _derive_display_name,
        "_authenticated_git_url": _authenticated_git_url,
        "_sanitize_error_detail": _sanitize_error_detail,
        "_remove_incomplete_repo_dir": _remove_incomplete_repo_dir,
        "_question_tokens": _question_tokens,
        "_question_tokens_cached": _question_tokens_cached,
        "_match_reason": _match_reason,
        "_is_dependency_question": _is_dependency_question,
        "_requires_cross_repo_context": _requires_cross_repo_context,
        "_is_test_file_path": _is_test_file_path,
        "_expand_dependency_matches": _expand_dependency_matches,
        "_expand_two_hop_matches": _expand_two_hop_matches,
        "_expand_agent_trace_matches": _expand_agent_trace_matches,
        "_run_planner_tool_loop": _run_planner_tool_loop,
        "_annotate_duplicate_tool_match": _annotate_duplicate_tool_match,
        "_choose_next_tool_step": _choose_next_tool_step,
        "_tool_step_signature": _tool_step_signature,
        "_execute_tool_loop_step": _execute_tool_loop_step,
        "_should_stop_tool_loop": _should_stop_tool_loop,
        "_build_tool_loop_plan": _build_tool_loop_plan,
        "_tool_loop_terms": _tool_loop_terms,
        "_tool_find_definition": _tool_find_definition,
        "_tool_find_references": _tool_find_references,
        "_tool_find_tables": _tool_find_tables,
        "_tool_find_api_routes": _tool_find_api_routes,
        "_tool_find_static_findings": _tool_find_static_findings,
        "_tool_find_test_coverage": _tool_find_test_coverage,
        "_tool_find_operational_boundaries": _tool_find_operational_boundaries,
        "_expand_impact_matches": _expand_impact_matches,
        "_tool_lookup_references_by_kind": _tool_lookup_references_by_kind,
        "_tool_find_callers": _tool_find_callers,
        "_tool_find_callees": _tool_find_callees,
        "_tool_lookup_flow_edges": _tool_lookup_flow_edges,
        "_tool_open_file_window": _tool_open_file_window,
        "_tool_lookup_structure": _tool_lookup_structure,
        "_tool_trace_graph": _tool_trace_graph,
        "_tool_trace_flow": _tool_trace_flow,
        "_tool_trace_entity": _tool_trace_entity,
        "_match_from_index_location": _match_from_index_location,
        "_trace_paths_cache_key": _trace_paths_cache_key,
        "_build_trace_paths": _build_trace_paths,
        "_build_repo_dependency_graph": _build_repo_dependency_graph,
        "_repo_message_index": _repo_message_index,
        "_repo_artifact_index": _repo_artifact_index,
        "_repo_table_index": _repo_table_index,
        "_repo_route_index": _repo_route_index,
        "_repo_config_index": _repo_config_index,
        "_match_repo_dependency_candidate": _match_repo_dependency_candidate,
        "_normalize_message_name": _normalize_message_name,
        "_message_values_from_config": _message_values_from_config,
        "_artifact_values_from_text": _artifact_values_from_text,
        "_normalize_artifact_name": _normalize_artifact_name,
        "_normalize_table_name": _normalize_table_name,
        "_better_repo_dependency_candidate": _better_repo_dependency_candidate,
        "_extract_route_literals": _extract_route_literals,
        "_prefer_specific_routes": _prefer_specific_routes,
        "_route_segment_count": _route_segment_count,
        "_join_config_routes": _join_config_routes,
        "_resolve_config_placeholders": _resolve_config_placeholders,
        "_candidate_dependency_config_values": _candidate_dependency_config_values,
        "_route_overlap_score": _route_overlap_score,
        "_route_path": _route_path,
        "_join_routes": _join_routes,
        "_repo_alias_match_score": _repo_alias_match_score,
        "_trace_path_edges_for_seed": _trace_path_edges_for_seed,
        "_trace_path_from_edges": _trace_path_from_edges,
    }
    _bind_source_code_qa_globals(list(helpers.values()), global_context)
    for name, helper in helpers.items():
        setattr(cls, name, classmethod(helper) if name in _CLASS_METHODS else staticmethod(helper) if name in _STATIC_METHODS else helper)
