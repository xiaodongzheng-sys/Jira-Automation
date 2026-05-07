"""Index build, schema, reuse, token, graph, and Spring config helpers for Source Code QA."""
from __future__ import annotations


def _bind_source_code_qa_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _index_path(self, repo_path: Path) -> Path:
    digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
    return self.index_root / f"{digest}.sqlite3"


def _index_lock_path(self, repo_path: Path) -> Path:
    digest = hashlib.sha1(str(repo_path).encode("utf-8")).hexdigest()[:16]
    return self.lock_root / f"{digest}.lock"


def _index_lock_is_stale(self, lock_path: Path) -> bool:
    stale_seconds = float(os.getenv("SOURCE_CODE_QA_INDEX_LOCK_STALE_SECONDS", str(DEFAULT_INDEX_LOCK_STALE_SECONDS)))
    if stale_seconds <= 0:
        return False
    timestamp = 0.0
    try:
        raw_timestamp = lock_path.read_text(encoding="utf-8").splitlines()[0].strip()
        timestamp = datetime.fromisoformat(raw_timestamp).timestamp()
    except (IndexError, OSError, ValueError):
        try:
            timestamp = lock_path.stat().st_mtime
        except OSError:
            return True
    return (time.time() - timestamp) > stale_seconds


def _acquire_index_lock(self, repo_path: Path) -> Path:
    self.lock_root.mkdir(parents=True, exist_ok=True)
    lock_path = self._index_lock_path(repo_path)
    for attempt in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError as error:
            if attempt == 0 and self._index_lock_is_stale(lock_path):
                lock_path.unlink(missing_ok=True)
                continue
            raise ToolError("This repository is already being indexed. Please wait for the current sync to finish.") from error
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(self._now_iso())
    return lock_path


def _require_ready_repo_index(
    self,
    *,
    key: str | None,
    entry: RepositoryEntry,
    repo_path: Path,
) -> dict[str, Any]:
    info = self._repo_index_info(key, entry, repo_path)
    if info.get("state") == "ready" or (info.get("state") == "stale" and info.get("queryable")):
        return info
    raise SourceCodeQAIndexUnavailable(
        f"Code index for {entry.display_name} is {info.get('state') or 'unavailable'}; run Sync / Refresh first."
    )


def _repo_index_info(self, key: str | None, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
    del key, entry
    index_path = self._index_path(repo_path)
    if not index_path.exists():
        return {"state": "missing", "path": str(index_path), "schema_compatible": False, "queryable": False}
    git_revision = self._repo_git_revision(repo_path)
    try:
        with sqlite3.connect(index_path) as connection:
            metadata = dict(connection.execute("select key, value from metadata").fetchall())
    except sqlite3.Error:
        return {"state": "stale", "path": str(index_path), "schema_compatible": False, "queryable": False}
    index_version = int(str(metadata.get("version") or "0")) if str(metadata.get("version") or "0").isdigit() else 0
    schema_compatible = index_version == CODE_INDEX_VERSION
    queryable = index_version >= 28
    state = "stale"
    if (
        schema_compatible
        and git_revision
        and metadata.get("git_revision") == git_revision
        and self._repo_worktree_clean(repo_path)
    ):
        state = "ready"
    else:
        fingerprint = self._repo_fingerprint(repo_path)
        expected = {
            "version": str(CODE_INDEX_VERSION),
            "file_count": str(fingerprint["file_count"]),
            "latest_mtime_ns": str(fingerprint["latest_mtime_ns"]),
            "total_size": str(fingerprint["total_size"]),
        }
        state = "ready" if all(metadata.get(key) == value for key, value in expected.items()) else "stale"
    return {
        "state": state,
        "path": str(index_path),
        "schema_compatible": schema_compatible,
        "queryable": queryable,
        "index_version": index_version,
        "files": int(metadata.get("indexed_files") or 0),
        "lines": int(metadata.get("indexed_lines") or 0),
        "definitions": int(metadata.get("indexed_definitions") or 0),
        "references": int(metadata.get("indexed_references") or 0),
        "entities": int(metadata.get("indexed_entities") or 0),
        "entity_edges": int(metadata.get("indexed_entity_edges") or 0),
        "edges": int(metadata.get("indexed_edges") or 0),
        "flow_edges": int(metadata.get("indexed_flow_edges") or 0),
        "semantic_chunks": int(metadata.get("indexed_semantic_chunks") or 0),
        "reused_files": int(metadata.get("reused_files") or 0),
        "reparsed_files": int(metadata.get("reparsed_files") or 0),
        "index_refresh_strategy": metadata.get("index_refresh_strategy") or "",
        "parser_backend": metadata.get("parser_backend") or "regex",
        "parser_languages": [
            item
            for item in str(metadata.get("parser_languages") or "").split(",")
            if item
        ],
        "tree_sitter_files": int(metadata.get("tree_sitter_files") or 0),
        "tree_sitter_errors": int(metadata.get("tree_sitter_errors") or 0),
        "semantic_index_model": metadata.get("semantic_index_model") or DEFAULT_SEMANTIC_INDEX_MODEL,
        "git_revision": metadata.get("git_revision") or git_revision,
        "file_fts_enabled": metadata.get("file_fts_enabled") == "1",
        "fts_enabled": metadata.get("fts_enabled") == "1",
        "semantic_fts_enabled": metadata.get("semantic_fts_enabled") == "1",
        "updated_at": metadata.get("updated_at"),
    }


def _repo_worktree_clean(repo_path: Path) -> bool:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_path), "status", "--porcelain", "--untracked-files=all"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0 and not (completed.stdout or "").strip()


def _repo_fingerprint(self, repo_path: Path) -> dict[str, int]:
    file_count = 0
    latest_mtime_ns = 0
    total_size = 0
    for file_path in self._iter_text_files(repo_path):
        try:
            stat = file_path.stat()
        except OSError:
            continue
        file_count += 1
        latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
        total_size += stat.st_size
    return {
        "file_count": file_count,
        "latest_mtime_ns": latest_mtime_ns,
        "total_size": total_size,
    }


def _repo_git_revision(self, repo_path: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--short=12", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if completed.returncode != 0:
        return ""
    return (completed.stdout or "").strip()


def _build_repo_index(self, key: str, entry: RepositoryEntry, repo_path: Path) -> dict[str, Any]:
    del key, entry
    lock_path = self._acquire_index_lock(repo_path)
    self.index_root.mkdir(parents=True, exist_ok=True)
    index_path = self._index_path(repo_path)
    tmp_path = index_path.with_suffix(".tmp")
    tmp_path.unlink(missing_ok=True)
    fingerprint = self._repo_fingerprint(repo_path)
    git_revision = self._repo_git_revision(repo_path)
    indexed_files = 0
    indexed_lines = 0
    indexed_definitions = 0
    indexed_references = 0
    indexed_entities = 0
    indexed_entity_edges = 0
    indexed_semantic_chunks = 0
    tree_sitter_files = 0
    tree_sitter_errors = 0
    reused_files = 0
    reparsed_files = 0
    parser_languages: set[str] = set()
    reusable_index = self._open_reusable_index(index_path)
    try:
        with sqlite3.connect(tmp_path) as connection:
            file_fts_enabled, fts_enabled, semantic_fts_enabled = self._create_repo_index_schema(connection)
            for file_path in self._iter_text_files(repo_path):
                relative_path = str(file_path.relative_to(repo_path))
                try:
                    stat = file_path.stat()
                except OSError:
                    continue
                copied_counts = self._copy_unchanged_index_file(
                    reusable_index,
                    connection,
                    relative_path,
                    stat,
                    file_fts_enabled=file_fts_enabled,
                    fts_enabled=fts_enabled,
                    semantic_fts_enabled=semantic_fts_enabled,
                )
                if copied_counts is not None:
                    indexed_files += 1
                    indexed_lines += copied_counts["lines"]
                    indexed_definitions += copied_counts["definitions"]
                    indexed_references += copied_counts["references"]
                    indexed_entities += copied_counts["entities"]
                    indexed_entity_edges += copied_counts["entity_edges"]
                    indexed_semantic_chunks += copied_counts["semantic_chunks"]
                    reused_files += 1
                    reused_language = self._tree_sitter_language_for_suffix(Path(relative_path).suffix.lower())
                    if reused_language:
                        parser_languages.add(reused_language)
                        tree_sitter_files += 1
                    continue
                try:
                    lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                except OSError:
                    continue
                reparsed_files += 1
                file_symbols = self._collect_symbols(lines)
                connection.execute(
                    "insert into files(path, lower_path, size, mtime_ns, line_count, symbols) values (?, ?, ?, ?, ?, ?)",
                    (
                        relative_path,
                        relative_path.lower(),
                        stat.st_size,
                        stat.st_mtime_ns,
                        len(lines),
                        json.dumps(sorted(file_symbols), separators=(",", ":")),
                    ),
                )
                if file_fts_enabled:
                    connection.execute(
                        "insert into files_fts(path, content) values (?, ?)",
                        (
                            relative_path,
                            "\n".join(
                                [
                                    relative_path,
                                    relative_path.replace("/", " ").replace(".", " "),
                                    " ".join(sorted(file_symbols)),
                                ]
                            ),
                        ),
                    )
                self._insert_file_tokens(connection, relative_path, relative_path, file_symbols)
                line_rows = []
                line_token_rows: list[tuple[str, str, int]] = []
                for index, line in enumerate(lines, start=1):
                    lowered = line.lower()
                    line_symbols = self._line_symbols(lowered)
                    line_rows.append(
                        (
                            relative_path,
                            index,
                            line,
                            lowered,
                            json.dumps(sorted(line_symbols), separators=(",", ":")),
                            1 if self._is_declaration_line(line) else 0,
                            1 if PATHISH_PATTERN.search(line) else 0,
                        )
                    )
                    line_token_rows.extend(
                        (token, relative_path, index)
                        for token in self._index_tokens_for_text(line, line_symbols)
                    )
                connection.executemany(
                    """
                    insert into lines(file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish)
                    values (?, ?, ?, ?, ?, ?, ?)
                    """,
                    line_rows,
                )
                connection.executemany(
                    "insert or ignore into line_tokens(token, file_path, line_no) values (?, ?, ?)",
                    line_token_rows,
                )
                if fts_enabled:
                    connection.executemany(
                        "insert into lines_fts(file_path, line_no, content) values (?, ?, ?)",
                        [(relative_path, row[1], row[2]) for row in line_rows],
                    )
                structure = self._extract_structure_rows(relative_path, lines)
                if structure.get("tree_sitter_used"):
                    tree_sitter_files += 1
                    parser_language = str(structure.get("tree_sitter_language") or "")
                    if parser_language:
                        parser_languages.add(parser_language)
                if structure.get("tree_sitter_error"):
                    tree_sitter_errors += 1
                connection.executemany(
                    """
                    insert into definitions(name, lower_name, kind, file_path, line_no, signature)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    structure["definitions"],
                )
                connection.executemany(
                    """
                    insert into references_index(target, lower_target, kind, file_path, line_no, context)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    structure["references"],
                )
                connection.executemany(
                    """
                    insert or ignore into code_entities(entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    structure["entities"],
                )
                semantic_chunks = self._build_semantic_chunks(relative_path, lines) if self.semantic_index_enabled else []
                connection.executemany(
                    """
                    insert into semantic_chunks(chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols)
                    values (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    semantic_chunks,
                )
                if semantic_fts_enabled and semantic_chunks:
                    connection.executemany(
                        "insert into semantic_chunks_fts(chunk_id, file_path, content) values (?, ?, ?)",
                        [(chunk[0], chunk[1], f"{chunk[4]}\n{chunk[6]}\n{chunk[7]}") for chunk in semantic_chunks],
                    )
                self._insert_semantic_chunk_tokens(connection, semantic_chunks)
                connection.executemany(
                    """
                    insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    structure["entity_edges"],
                )
                indexed_files += 1
                indexed_lines += len(lines)
                indexed_definitions += len(structure["definitions"])
                indexed_references += len(structure["references"])
                indexed_entities += len(structure["entities"])
                indexed_entity_edges += len(structure["entity_edges"])
                indexed_semantic_chunks += len(semantic_chunks)
            indexed_edges = self._build_graph_edges(connection)
            resolved_entity_edges = self._resolve_entity_edges(connection)
            indexed_entity_edges += resolved_entity_edges
            implementation_edges = self._build_implementation_edges(connection)
            indexed_entity_edges += implementation_edges
            aop_edges = self._build_aop_edges(connection)
            indexed_entity_edges += aop_edges
            indexed_flow_edges = self._build_flow_edges(connection)
            metadata = {
                "version": str(CODE_INDEX_VERSION),
                "file_count": str(fingerprint["file_count"]),
                "latest_mtime_ns": str(fingerprint["latest_mtime_ns"]),
                "total_size": str(fingerprint["total_size"]),
                "indexed_files": str(indexed_files),
                "indexed_lines": str(indexed_lines),
                "indexed_definitions": str(indexed_definitions),
                "indexed_references": str(indexed_references),
                "indexed_entities": str(indexed_entities),
                "indexed_entity_edges": str(indexed_entity_edges),
                "indexed_edges": str(indexed_edges),
                "indexed_flow_edges": str(indexed_flow_edges),
                "indexed_semantic_chunks": str(indexed_semantic_chunks),
                "reused_files": str(reused_files),
                "reparsed_files": str(reparsed_files),
                "index_refresh_strategy": "delta_row_reuse" if reusable_index is not None else "full_rebuild",
                "parser_backend": "tree_sitter+regex" if tree_sitter_files else "regex",
                "parser_languages": ",".join(sorted(parser_languages)),
                "tree_sitter_files": str(tree_sitter_files),
                "tree_sitter_errors": str(tree_sitter_errors),
                "semantic_index_model": self.semantic_index_model,
                "git_revision": git_revision,
                "file_fts_enabled": "1" if file_fts_enabled else "0",
                "fts_enabled": "1" if fts_enabled else "0",
                "semantic_fts_enabled": "1" if semantic_fts_enabled else "0",
                "updated_at": self._now_iso(),
            }
            connection.executemany("insert into metadata(key, value) values (?, ?)", metadata.items())
        tmp_path.replace(index_path)
        return {
            "state": "ready",
            "path": str(index_path),
            "files": indexed_files,
            "lines": indexed_lines,
            "definitions": indexed_definitions,
            "references": indexed_references,
            "entities": indexed_entities,
            "entity_edges": indexed_entity_edges,
            "edges": indexed_edges,
            "flow_edges": indexed_flow_edges,
            "semantic_chunks": indexed_semantic_chunks,
            "reused_files": reused_files,
            "reparsed_files": reparsed_files,
            "index_refresh_strategy": metadata["index_refresh_strategy"],
            "parser_backend": metadata["parser_backend"],
            "parser_languages": sorted(parser_languages),
            "tree_sitter_files": tree_sitter_files,
            "tree_sitter_errors": tree_sitter_errors,
            "semantic_index_model": self.semantic_index_model,
            "git_revision": git_revision,
            "file_fts_enabled": file_fts_enabled,
            "fts_enabled": fts_enabled,
            "semantic_fts_enabled": semantic_fts_enabled,
            "updated_at": metadata["updated_at"],
        }
    finally:
        if reusable_index is not None:
            reusable_index.close()
        lock_path.unlink(missing_ok=True)


def _create_repo_index_schema(self, connection: sqlite3.Connection) -> tuple[bool, bool, bool]:
    connection.execute("pragma journal_mode=off")
    connection.execute("pragma synchronous=off")
    connection.executescript(
        """
        create table metadata (key text primary key, value text not null);
        create table files (
            path text primary key,
            lower_path text not null,
            size integer not null,
            mtime_ns integer not null,
            line_count integer not null,
            symbols text not null
        );
        create table lines (
            file_path text not null,
            line_no integer not null,
            line_text text not null,
            lower_text text not null,
            symbols text not null,
            is_declaration integer not null,
            has_pathish integer not null,
            primary key (file_path, line_no)
        );
        create index idx_lines_file_path on lines(file_path);
        create table file_tokens (
            token text not null,
            file_path text not null,
            primary key (token, file_path)
        );
        create index idx_file_tokens_path on file_tokens(file_path);
        create table line_tokens (
            token text not null,
            file_path text not null,
            line_no integer not null,
            primary key (token, file_path, line_no)
        );
        create index idx_line_tokens_location on line_tokens(file_path, line_no);
        create table definitions (
            name text not null,
            lower_name text not null,
            kind text not null,
            file_path text not null,
            line_no integer not null,
            signature text not null
        );
        create index idx_definitions_lower_name on definitions(lower_name);
        create index idx_definitions_file_path on definitions(file_path);
        create table references_index (
            target text not null,
            lower_target text not null,
            kind text not null,
            file_path text not null,
            line_no integer not null,
            context text not null
        );
        create index idx_references_lower_target on references_index(lower_target);
        create index idx_references_file_path on references_index(file_path);
        create table code_entities (
            entity_id text primary key,
            name text not null,
            lower_name text not null,
            kind text not null,
            language text not null,
            file_path text not null,
            line_no integer not null,
            parent text not null,
            signature text not null
        );
        create index idx_entities_lower_name on code_entities(lower_name);
        create index idx_entities_file_path on code_entities(file_path);
        create index idx_entities_kind on code_entities(kind);
        create table entity_edges (
            from_entity_id text not null,
            from_file text not null,
            from_line integer not null,
            edge_kind text not null,
            to_name text not null,
            lower_to_name text not null,
            to_entity_id text not null,
            to_file text not null,
            to_line integer not null,
            evidence text not null
        );
        create index idx_entity_edges_from on entity_edges(from_entity_id);
        create index idx_entity_edges_from_file on entity_edges(from_file);
        create index idx_entity_edges_lower_to_name on entity_edges(lower_to_name);
        create index idx_entity_edges_to_file on entity_edges(to_file);
        create index idx_entity_edges_kind on entity_edges(edge_kind);
        create table graph_edges (
            from_file text not null,
            from_line integer not null,
            symbol text not null,
            lower_symbol text not null,
            edge_kind text not null,
            to_file text not null,
            to_line integer not null
        );
        create index idx_graph_from_file on graph_edges(from_file);
        create index idx_graph_lower_symbol on graph_edges(lower_symbol);
        create index idx_graph_to_file on graph_edges(to_file);
        create table flow_edges (
            from_file text not null,
            from_line integer not null,
            from_kind text not null,
            from_name text not null,
            edge_kind text not null,
            to_name text not null,
            to_file text not null,
            to_line integer not null,
            evidence text not null
        );
        create index idx_flow_from_file on flow_edges(from_file);
        create index idx_flow_to_file on flow_edges(to_file);
        create index idx_flow_to_name on flow_edges(to_name);
        create index idx_flow_edge_kind on flow_edges(edge_kind);
        create table semantic_chunks (
            chunk_id text primary key,
            file_path text not null,
            start_line integer not null,
            end_line integer not null,
            chunk_text text not null,
            lower_text text not null,
            tokens text not null,
            symbols text not null
        );
        create index idx_semantic_chunks_file_path on semantic_chunks(file_path);
        create table semantic_chunk_tokens (
            token text not null,
            chunk_id text not null,
            file_path text not null,
            primary key (token, chunk_id)
        );
        create index idx_semantic_chunk_tokens_chunk on semantic_chunk_tokens(chunk_id);
        create index idx_semantic_chunk_tokens_file on semantic_chunk_tokens(file_path);
        """
    )
    file_fts_enabled = self._try_create_file_fts(connection)
    fts_enabled = self._try_create_fts(connection)
    semantic_fts_enabled = self._try_create_semantic_fts(connection)
    return file_fts_enabled, fts_enabled, semantic_fts_enabled


def _open_reusable_index(self, index_path: Path) -> sqlite3.Connection | None:
    if not index_path.exists():
        return None
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(index_path)
        connection.row_factory = sqlite3.Row
        metadata = dict(connection.execute("select key, value from metadata").fetchall())
        if metadata.get("version") != str(CODE_INDEX_VERSION):
            connection.close()
            return None
        return connection
    except sqlite3.Error:
        if connection is not None:
            connection.close()
        return None


def _copy_reused_index_rows(
    old_connection: sqlite3.Connection | None,
    new_connection: sqlite3.Connection,
    relative_path: str,
    select_sql: str,
    insert_sql: str,
) -> list[sqlite3.Row]:
    if old_connection is None:
        return []
    rows = old_connection.execute(select_sql, (relative_path,)).fetchall()
    new_connection.executemany(insert_sql, [tuple(row) for row in rows])
    return rows


def _insert_reused_file_row(
    new_connection: sqlite3.Connection,
    relative_path: str,
    *,
    file_fts_enabled: bool,
    file_row: sqlite3.Row,
) -> None:
    new_connection.execute(
        "insert into files(path, lower_path, size, mtime_ns, line_count, symbols) values (?, ?, ?, ?, ?, ?)",
        (
            file_row["path"],
            file_row["lower_path"],
            file_row["size"],
            file_row["mtime_ns"],
            file_row["line_count"],
            file_row["symbols"],
        ),
    )
    if not file_fts_enabled:
        return
    try:
        symbols = " ".join(json.loads(file_row["symbols"] or "[]"))
    except (TypeError, json.JSONDecodeError):
        symbols = ""
    new_connection.execute(
        "insert into files_fts(path, content) values (?, ?)",
        (
            relative_path,
            "\n".join(
                [
                    relative_path,
                    relative_path.replace("/", " ").replace(".", " "),
                    symbols,
                ]
            ),
        ),
    )


def _copy_reused_line_rows(
    cls,
    old_connection: sqlite3.Connection | None,
    new_connection: sqlite3.Connection,
    relative_path: str,
    *,
    fts_enabled: bool,
) -> list[sqlite3.Row]:
    line_rows = cls._copy_reused_index_rows(
        old_connection,
        new_connection,
        relative_path,
        """
        select file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish
        from lines
        where file_path = ?
        order by line_no
        """,
        """
        insert into lines(file_path, line_no, line_text, lower_text, symbols, is_declaration, has_pathish)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
    )
    if fts_enabled and line_rows:
        new_connection.executemany(
            "insert into lines_fts(file_path, line_no, content) values (?, ?, ?)",
            [(row["file_path"], row["line_no"], row["line_text"]) for row in line_rows],
        )
    return line_rows


def _copy_reused_semantic_rows(
    cls,
    old_connection: sqlite3.Connection | None,
    new_connection: sqlite3.Connection,
    relative_path: str,
    *,
    semantic_fts_enabled: bool,
) -> list[sqlite3.Row]:
    semantic_rows = cls._copy_reused_index_rows(
        old_connection,
        new_connection,
        relative_path,
        """
        select chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols
        from semantic_chunks
        where file_path = ?
        """,
        """
        insert into semantic_chunks(chunk_id, file_path, start_line, end_line, chunk_text, lower_text, tokens, symbols)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
    )
    if semantic_fts_enabled and semantic_rows:
        new_connection.executemany(
            "insert into semantic_chunks_fts(chunk_id, file_path, content) values (?, ?, ?)",
            [
                (
                    row["chunk_id"],
                    row["file_path"],
                    f"{row['chunk_text']}\n{row['tokens']}\n{row['symbols']}",
                )
                for row in semantic_rows
            ],
        )
    cls._copy_reused_index_rows(
        old_connection,
        new_connection,
        relative_path,
        "select token, chunk_id, file_path from semantic_chunk_tokens where file_path = ?",
        "insert or ignore into semantic_chunk_tokens(token, chunk_id, file_path) values (?, ?, ?)",
    )
    return semantic_rows


def _copy_unchanged_index_file(
    cls,
    old_connection: sqlite3.Connection | None,
    new_connection: sqlite3.Connection,
    relative_path: str,
    stat: os.stat_result,
    *,
    file_fts_enabled: bool,
    fts_enabled: bool,
    semantic_fts_enabled: bool,
) -> dict[str, int] | None:
    if old_connection is None:
        return None
    try:
        file_row = old_connection.execute("select * from files where path = ?", (relative_path,)).fetchone()
        if file_row is None:
            return None
        if int(file_row["size"]) != int(stat.st_size) or int(file_row["mtime_ns"]) != int(stat.st_mtime_ns):
            return None

        new_connection.execute("savepoint reuse_index_file")
        cls._insert_reused_file_row(
            new_connection,
            relative_path,
            file_fts_enabled=file_fts_enabled,
            file_row=file_row,
        )
        line_rows = cls._copy_reused_line_rows(
            old_connection,
            new_connection,
            relative_path,
            fts_enabled=fts_enabled,
        )
        cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            "select token, file_path from file_tokens where file_path = ?",
            "insert or ignore into file_tokens(token, file_path) values (?, ?)",
        )
        cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            "select token, file_path, line_no from line_tokens where file_path = ?",
            "insert or ignore into line_tokens(token, file_path, line_no) values (?, ?, ?)",
        )
        definition_rows = cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            "select name, lower_name, kind, file_path, line_no, signature from definitions where file_path = ?",
            "insert into definitions(name, lower_name, kind, file_path, line_no, signature) values (?, ?, ?, ?, ?, ?)",
        )
        reference_rows = cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            """
            select target, lower_target, kind, file_path, line_no, context
            from references_index
            where file_path = ?
            """,
            """
            insert into references_index(target, lower_target, kind, file_path, line_no, context)
            values (?, ?, ?, ?, ?, ?)
            """,
        )
        entity_rows = cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            """
            select entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature
            from code_entities
            where file_path = ?
            """,
            """
            insert or ignore into code_entities(entity_id, name, lower_name, kind, language, file_path, line_no, parent, signature)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
        )
        raw_edge_rows = cls._copy_reused_index_rows(
            old_connection,
            new_connection,
            relative_path,
            """
            select from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence
            from entity_edges
            where from_file = ? and (to_entity_id = '' or to_file = '')
            """,
            """
            insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
        )
        semantic_rows = cls._copy_reused_semantic_rows(
            old_connection,
            new_connection,
            relative_path,
            semantic_fts_enabled=semantic_fts_enabled,
        )

        new_connection.execute("release savepoint reuse_index_file")
        return {
            "lines": len(line_rows),
            "definitions": len(definition_rows),
            "references": len(reference_rows),
            "entities": len(entity_rows),
            "entity_edges": len(raw_edge_rows),
            "semantic_chunks": len(semantic_rows),
        }
    except (sqlite3.Error, KeyError, TypeError, ValueError):
        try:
            new_connection.execute("rollback to savepoint reuse_index_file")
            new_connection.execute("release savepoint reuse_index_file")
        except sqlite3.Error:
            pass
        return None


def _index_tokens_for_text(text: str, symbols: set[str] | None = None) -> list[str]:
    tokens: set[str] = set(symbols or set())
    lowered = str(text or "").lower()
    tokens.update(match.group(0).lower() for match in FTS_TOKEN_PATTERN.finditer(lowered))
    tokens.update(match.group(0).lower() for match in IDENTIFIER_PATTERN.finditer(lowered))
    cleaned = {
        token.strip("._/-:")
        for token in tokens
        if len(token.strip("._/-:")) >= 3
        and token.strip("._/-:") not in STOPWORDS
        and token.strip("._/-:") not in LOW_VALUE_CALL_SYMBOLS
    }
    return sorted(cleaned)[:160]


def _insert_file_tokens(
    cls,
    connection: sqlite3.Connection,
    relative_path: str,
    content: str,
    symbols: set[str],
) -> None:
    rows = [
        (token, relative_path)
        for token in cls._index_tokens_for_text(f"{relative_path}\n{content}", symbols)
    ]
    connection.executemany(
        "insert or ignore into file_tokens(token, file_path) values (?, ?)",
        rows,
    )


def _insert_semantic_chunk_tokens(
    cls,
    connection: sqlite3.Connection,
    chunks: list[tuple[str, str, int, int, str, str, str, str]],
) -> None:
    rows: list[tuple[str, str, str]] = []
    for chunk in chunks:
        chunk_id, file_path, _start, _end, chunk_text, _lower, tokens_json, symbols_json = chunk
        symbols: set[str] = set()
        try:
            symbols.update(str(item).lower() for item in json.loads(tokens_json or "[]"))
            symbols.update(str(item).lower() for item in json.loads(symbols_json or "[]"))
        except (TypeError, json.JSONDecodeError):
            pass
        rows.extend(
            (token, chunk_id, file_path)
            for token in cls._index_tokens_for_text(chunk_text, symbols)
        )
    connection.executemany(
        "insert or ignore into semantic_chunk_tokens(token, chunk_id, file_path) values (?, ?, ?)",
        rows,
    )


def _build_semantic_chunks(relative_path: str, lines: list[str]) -> list[tuple[str, str, int, int, str, str, str, str]]:
    chunks: list[tuple[str, str, int, int, str, str, str, str]] = []
    if not lines:
        return chunks
    window = 32
    overlap = 8
    step = max(1, window - overlap)
    for start_index in range(0, len(lines), step):
        window_lines = lines[start_index : start_index + window]
        if not window_lines:
            continue
        chunk_text = "\n".join(window_lines).strip()
        if not chunk_text:
            continue
        lower_text = chunk_text.lower()
        tokens = SourceCodeQAService._semantic_tokens(f"{relative_path}\n{chunk_text}")
        symbols = sorted(SourceCodeQAService._line_symbols(lower_text))
        start_line = start_index + 1
        end_line = min(len(lines), start_index + len(window_lines))
        chunk_id = hashlib.sha1(f"{relative_path}:{start_line}:{end_line}:{chunk_text[:120]}".encode("utf-8")).hexdigest()[:16]
        chunks.append(
            (
                chunk_id,
                relative_path,
                start_line,
                end_line,
                chunk_text[:6000],
                lower_text[:6000],
                json.dumps(tokens[:160], separators=(",", ":")),
                json.dumps(symbols[:160], separators=(",", ":")),
            )
        )
        if end_line >= len(lines):
            break
    return chunks


def _semantic_tokens(text: str) -> list[str]:
    raw_tokens = re.findall(r"[A-Za-z0-9_./:-]{2,}", str(text or "").lower())
    tokens: list[str] = []
    for token in raw_tokens:
        token = token.strip("./:-_")
        if len(token) < 3 or token in STOPWORDS or token in LOW_VALUE_CALL_SYMBOLS:
            continue
        for part in re.split(r"[/_.:-]+", token):
            if len(part) >= 3 and part not in STOPWORDS and part not in tokens:
                tokens.append(part)
        if token not in tokens:
            tokens.append(token)
    return tokens[:220]


def _try_create_file_fts(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute(
            "create virtual table files_fts using fts5(path unindexed, content)"
        )
        return True
    except sqlite3.Error:
        return False


def _try_create_fts(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute(
            "create virtual table lines_fts using fts5(file_path unindexed, line_no unindexed, content)"
        )
        return True
    except sqlite3.Error:
        return False


def _try_create_semantic_fts(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute(
            "create virtual table semantic_chunks_fts using fts5(chunk_id unindexed, file_path unindexed, content)"
        )
        return True
    except sqlite3.Error:
        return False


def _build_graph_edges(connection: sqlite3.Connection) -> int:
    definitions = connection.execute(
        "select lower_name, file_path, line_no from definitions"
    ).fetchall()
    definition_by_name: dict[str, list[tuple[str, int]]] = {}
    for lower_name, file_path, line_no in definitions:
        definition_by_name.setdefault(str(lower_name), []).append((str(file_path), int(line_no)))
    rows = []
    for target, lower_target, kind, file_path, line_no, _context in connection.execute(
        "select target, lower_target, kind, file_path, line_no, context from references_index"
    ):
        for to_file, to_line in definition_by_name.get(str(lower_target), [])[:8]:
            if to_file == file_path:
                continue
            rows.append((file_path, int(line_no), target, lower_target, kind, to_file, to_line))
    rows = list(dict.fromkeys(rows))
    connection.executemany(
        """
        insert into graph_edges(from_file, from_line, symbol, lower_symbol, edge_kind, to_file, to_line)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _resolve_entity_edges(connection: sqlite3.Connection) -> int:
    entities = connection.execute(
        "select entity_id, lower_name, file_path, line_no from code_entities"
    ).fetchall()
    by_name: dict[str, list[tuple[str, str, int]]] = {}
    for entity_id, lower_name, file_path, line_no in entities:
        by_name.setdefault(str(lower_name), []).append((str(entity_id), str(file_path), int(line_no)))
    updates: list[tuple[str, str, int, int]] = []
    for rowid, lower_to_name, from_file in connection.execute(
        """
        select rowid, lower_to_name, from_file
        from entity_edges
        where to_file = ''
        """
    ):
        candidates = by_name.get(str(lower_to_name)) or []
        if not candidates:
            short_name = str(lower_to_name).rsplit(".", 1)[-1]
            candidates = by_name.get(short_name) or []
        if not candidates:
            continue
        candidates = sorted(candidates, key=lambda item: 0 if item[1] != str(from_file) else 1)
        to_entity_id, to_file, to_line = candidates[0]
        updates.append((to_entity_id, to_file, to_line, int(rowid)))
    connection.executemany(
        """
        update entity_edges
        set to_entity_id = ?, to_file = ?, to_line = ?
        where rowid = ?
        """,
        updates,
    )
    return len(updates)


def _entity_edge_values_by_class(
    cls,
    connection: sqlite3.Connection,
    edge_kind: str,
    *,
    normalize_value: Any = None,
) -> dict[str, set[str]]:
    values_by_class: dict[str, set[str]] = {}
    for class_name, target_name in connection.execute(
        """
        select c.name, e.to_name
        from entity_edges e
        join code_entities c on c.entity_id = e.from_entity_id
        where e.edge_kind = ?
        """,
        (edge_kind,),
    ):
        value = str(target_name)
        if normalize_value is not None:
            value = normalize_value(value)
        for key in cls._symbol_lookup_keys(str(class_name)):
            values_by_class.setdefault(key, set()).add(value)
    return values_by_class


def _primary_classes_by_lookup_key(cls, connection: sqlite3.Connection) -> set[str]:
    primary_classes: set[str] = set()
    for class_name in connection.execute(
        """
        select c.name
        from entity_edges e
        join code_entities c on c.entity_id = e.from_entity_id
        where e.edge_kind = 'bean_primary'
        """
    ):
        for key in cls._symbol_lookup_keys(str(class_name[0])):
            primary_classes.add(key)
    return primary_classes


def _bean_qualifier_lookups(
    connection: sqlite3.Connection,
) -> tuple[dict[str, set[str]], dict[tuple[str, str], set[str]]]:
    qualifiers_by_file: dict[str, set[str]] = {}
    for from_file, qualifier in connection.execute(
        "select from_file, to_name from entity_edges where edge_kind = 'bean_qualifier'"
    ):
        qualifiers_by_file.setdefault(str(from_file), set()).add(str(qualifier))
    qualifiers_by_variable: dict[tuple[str, str], set[str]] = {}
    for from_file, target in connection.execute(
        "select from_file, to_name from entity_edges where edge_kind = 'bean_qualifier_target'"
    ):
        variable_name, separator, qualifier = str(target).partition("=")
        if separator and variable_name and qualifier:
            qualifiers_by_variable.setdefault((str(from_file), variable_name), set()).add(qualifier)
    return qualifiers_by_file, qualifiers_by_variable


def _build_implementation_edges(cls, connection: sqlite3.Connection) -> int:
    bean_names_by_class = cls._entity_edge_values_by_class(connection, "bean_name")
    primary_classes = cls._primary_classes_by_lookup_key(connection)
    profiles_by_class = cls._entity_edge_values_by_class(
        connection,
        "spring_profile",
        normalize_value=lambda value: value.strip().lower(),
    )
    active_profiles = cls._active_spring_profiles(connection)
    config_values = cls._spring_config_values(connection)
    conditions_by_class = cls._entity_edge_values_by_class(
        connection,
        "bean_condition",
        normalize_value=lambda value: value.strip(),
    )
    qualifiers_by_file, qualifiers_by_variable = cls._bean_qualifier_lookups(connection)

    implementors: dict[str, list[dict[str, Any]]] = {}
    for impl_name, from_file, from_line, interface_name in connection.execute(
        """
        select c.name, e.from_file, e.from_line, e.to_name
        from entity_edges e
        join code_entities c on c.entity_id = e.from_entity_id
        where e.edge_kind in ('implements', 'extends')
        """
    ):
        for key in cls._symbol_lookup_keys(str(interface_name)):
            impl_keys = cls._symbol_lookup_keys(str(impl_name))
            bean_names: set[str] = set()
            for impl_key in impl_keys:
                bean_names.update(bean_names_by_class.get(impl_key) or set())
            profiles: set[str] = set()
            for impl_key in impl_keys:
                profiles.update(profiles_by_class.get(impl_key) or set())
            conditions: set[str] = set()
            for impl_key in impl_keys:
                conditions.update(conditions_by_class.get(impl_key) or set())
            implementors.setdefault(key, []).append(
                {
                    "name": str(impl_name),
                    "file": str(from_file),
                    "line": int(from_line),
                    "bean_names": bean_names,
                    "profiles": profiles,
                    "conditions": conditions,
                    "primary": any(impl_key in primary_classes for impl_key in impl_keys),
                }
            )

    definitions: dict[str, list[tuple[str, int]]] = {}
    for lower_name, file_path, line_no in connection.execute(
        "select lower_name, file_path, line_no from definitions"
    ):
        definitions.setdefault(str(lower_name), []).append((str(file_path), int(line_no)))

    rows: list[tuple[str, str, int, str, str, str, str, str, int, str]] = []
    for from_entity_id, from_file, from_line, to_name, evidence in connection.execute(
        """
        select from_entity_id, from_file, from_line, to_name, evidence
        from entity_edges
        where edge_kind = 'call' and instr(to_name, '.') > 0
        """
    ):
        owner, method_name = str(to_name).rsplit(".", 1)
        call_variable = cls._member_call_variable(str(evidence or ""), method_name)
        for owner_key in cls._symbol_lookup_keys(owner):
            candidates = implementors.get(owner_key, [])[:8]
            qualifiers = qualifiers_by_variable.get((str(from_file), call_variable), set()) if call_variable else set()
            if not qualifiers:
                qualifiers = qualifiers_by_file.get(str(from_file)) or set()
            qualified_candidates = [
                item
                for item in candidates
                if qualifiers and (item.get("bean_names") or set()) & qualifiers
            ]
            profile_candidates = [
                item
                for item in candidates
                if active_profiles and (item.get("profiles") or set()) & active_profiles
            ]
            condition_candidates = [
                item
                for item in candidates
                if any(
                    cls._spring_condition_matches(str(condition), config_values)
                    for condition in (item.get("conditions") or set())
                )
            ]
            primary_candidates = [item for item in candidates if item.get("primary")]
            selected_candidates = qualified_candidates or profile_candidates or condition_candidates or primary_candidates or candidates
            for item in selected_candidates:
                impl_name = str(item.get("name") or "")
                impl_call = f"{impl_name}.{method_name}"
                targets = definitions.get(impl_call.lower()) or []
                if not targets:
                    continue
                to_file, to_line = targets[0]
                matched_qualifiers = sorted((item.get("bean_names") or set()) & qualifiers)
                qualifier_note = f" qualifier={matched_qualifiers[0]};" if matched_qualifiers else ""
                matched_profiles = sorted((item.get("profiles") or set()) & active_profiles)
                profile_note = f" profile={matched_profiles[0]};" if matched_profiles else ""
                matched_conditions = sorted(
                    str(condition)
                    for condition in (item.get("conditions") or set())
                    if cls._spring_condition_matches(str(condition), config_values)
                )
                condition_note = f" condition={matched_conditions[0]};" if matched_conditions else ""
                primary_note = " primary=true;" if item.get("primary") else ""
                rows.append(
                    (
                        str(from_entity_id),
                        str(from_file),
                        int(from_line),
                        "implementation_call",
                        impl_call,
                        impl_call.lower(),
                        "",
                        str(to_file),
                        int(to_line),
                        f"resolved implementation for {to_name}: {impl_call};{qualifier_note}{profile_note}{condition_note}{primary_note} {evidence or ''}"[:500],
                    )
                )
    rows = list(dict.fromkeys(rows))
    connection.executemany(
        """
        insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _symbol_lookup_keys(name: str) -> list[str]:
    value = str(name or "").strip().lower()
    if not value:
        return []
    keys = [value]
    short = value.rsplit(".", 1)[-1]
    if short and short not in keys:
        keys.append(short)
    return keys


def _build_aop_edges(connection: sqlite3.Connection) -> int:
    definitions: dict[str, list[tuple[str, int, str]]] = {}
    for name, lower_name, file_path, line_no in connection.execute(
        "select name, lower_name, file_path, line_no from definitions"
    ):
        definitions.setdefault(str(lower_name), []).append((str(file_path), int(line_no), str(name)))

    implementors: dict[str, set[str]] = {}
    for impl_name, interface_name in connection.execute(
        """
        select c.name, e.to_name
        from entity_edges e
        join code_entities c on c.entity_id = e.from_entity_id
        where e.edge_kind in ('implements', 'extends')
        """
    ):
        for key in SourceCodeQAService._symbol_lookup_keys(str(interface_name)):
            implementors.setdefault(key, set()).add(str(impl_name))

    raw_aop_edges = list(connection.execute(
        """
        select c.entity_id, c.name, e.from_file, e.from_line, e.edge_kind, e.to_name, e.evidence
        from entity_edges e
        join code_entities c on c.entity_id = e.from_entity_id
        where e.edge_kind in ('aop_pointcut', 'aop_advice')
        """
    ))
    pointcuts_by_name: dict[str, str] = {}
    for _entity_id, entity_name, _from_file, _from_line, edge_kind, to_name, _evidence in raw_aop_edges:
        if str(edge_kind) == "aop_pointcut" and str(entity_name):
            pointcuts_by_name[str(entity_name).lower()] = SourceCodeQAService._aop_pointcut_expression(str(to_name), {})

    aop_edges: list[tuple[str, str, int, str, str, str]] = []
    for entity_id, _entity_name, from_file, from_line, _edge_kind, to_name, evidence in raw_aop_edges:
        pointcut_expression = SourceCodeQAService._aop_pointcut_expression(str(to_name), pointcuts_by_name)
        aop_edges.append((str(entity_id), str(from_file), int(from_line), str(to_name), pointcut_expression, str(evidence or "")))

    rows: list[tuple[str, str, int, str, str, str, str, str, int, str]] = []
    for from_entity_id, from_file, from_line, source_name, pointcut_expression, evidence in aop_edges:
        for target_name in SourceCodeQAService._aop_execution_target_names(pointcut_expression, implementors):
            for to_file, to_line, definition_name in SourceCodeQAService._definition_matches_for_aop_target(definitions, target_name):
                rows.append(
                    (
                        from_entity_id,
                        from_file,
                        int(from_line),
                        "aop_applies_to",
                        definition_name,
                        definition_name.lower(),
                        "",
                        to_file,
                        int(to_line),
                        f"resolved AOP pointcut {source_name} -> {definition_name}; {evidence}"[:500],
                    )
                )

    rows = list(dict.fromkeys(rows))
    connection.executemany(
        """
        insert into entity_edges(from_entity_id, from_file, from_line, edge_kind, to_name, lower_to_name, to_entity_id, to_file, to_line, evidence)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _aop_pointcut_expression(to_name: str, pointcuts_by_name: dict[str, str]) -> str:
    raw = str(to_name or "")
    _kind, separator, payload = raw.partition(":")
    value = payload if separator else raw
    normalized_reference = re.sub(r"\(\s*\)$", "", value.strip()).lower()
    return pointcuts_by_name.get(normalized_reference, value.strip())


def _aop_execution_target_names(pointcut_expression: str, implementors: dict[str, set[str]]) -> list[str]:
    targets: list[str] = []
    text = str(pointcut_expression or "")
    for owner_name, method_name in re.findall(
        r"(?:execution|within|call)\s*\([^)]*?([A-Z][A-Za-z0-9_.$]*|\*)\.([A-Za-z_][A-Za-z0-9_]+)\s*\(",
        text,
    ):
        if owner_name == "*":
            targets.append(method_name)
            continue
        owner = owner_name.rsplit(".", 1)[-1]
        targets.append(f"{owner}.{method_name}")
        for owner_key in SourceCodeQAService._symbol_lookup_keys(owner):
            for impl_name in sorted(implementors.get(owner_key, set())):
                targets.append(f"{impl_name}.{method_name}")
    for owner_name, method_name in re.findall(r"\*\s+\*+\.\.([A-Z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]+)\s*\(", text):
        targets.append(f"{owner_name}.{method_name}")
        for owner_key in SourceCodeQAService._symbol_lookup_keys(owner_name):
            for impl_name in sorted(implementors.get(owner_key, set())):
                targets.append(f"{impl_name}.{method_name}")
    return list(dict.fromkeys(targets))


def _definition_matches_for_aop_target(
    definitions: dict[str, list[tuple[str, int, str]]],
    target_name: str,
) -> list[tuple[str, int, str]]:
    normalized = str(target_name or "").strip().lower()
    if not normalized:
        return []
    matches: list[tuple[str, int, str]] = []
    if "." in normalized:
        candidates = definitions.get(normalized, [])
        suffix = f".{normalized}"
        for lower_name, rows in definitions.items():
            if lower_name.endswith(suffix):
                candidates.extend(rows)
        matches.extend(candidates)
    else:
        matches.extend(definitions.get(normalized, []))
    return list(dict.fromkeys(matches))[:12]


def _member_call_variable(context: str, method_name: str) -> str:
    for variable_name, called_method in MEMBER_CALL_PATTERN.findall(str(context or "")):
        if called_method == method_name:
            return variable_name
    return ""


def _qualified_variable_targets(line: str) -> dict[str, list[str]]:
    targets: dict[str, list[str]] = {}
    for qualifier, variable_name in SPRING_QUALIFIED_VARIABLE_PATTERN.findall(str(line or "")):
        if qualifier and variable_name:
            targets.setdefault(variable_name, []).append(qualifier)
    return {variable: list(dict.fromkeys(qualifiers)) for variable, qualifiers in targets.items()}


def _service_like_types_from_generic(generic_text: str) -> list[str]:
    return list(dict.fromkeys(SERVICE_LIKE_TYPE_PATTERN.findall(str(generic_text or ""))))


def _active_spring_profiles(connection: sqlite3.Connection) -> set[str]:
    return SourceCodeQAService._active_spring_profiles_from_rows(
        SourceCodeQAService._spring_config_rows(connection)
    )


def _active_spring_profiles_from_rows(rows: list[tuple[str, str, str, str]]) -> set[str]:
    profiles: set[str] = set()
    for file_path, key, value, doc_profile in rows:
        if SourceCodeQAService._spring_profile_from_config_path(file_path) or doc_profile:
            continue
        if str(key).lower() in {"spring.profiles.active", "spring.profiles.include"}:
            for profile in re.split(r"[,;\s]+", str(value or "")):
                normalized = profile.strip().lower()
                if normalized:
                    profiles.add(normalized)
    return profiles


def _spring_config_values(connection: sqlite3.Connection) -> dict[str, set[str]]:
    values: dict[str, set[str]] = {}
    rows = SourceCodeQAService._spring_config_rows(connection)
    active_profiles = SourceCodeQAService._active_spring_profiles_from_rows(rows)
    profile_overrides: set[str] = set()
    for file_path, key, value, doc_profile in rows:
        normalized_key = str(key or "").strip().lower()
        normalized_value = str(value or "").strip().strip("\"'").lower()
        if not normalized_key or not normalized_value:
            continue
        file_profile = doc_profile or SourceCodeQAService._spring_profile_from_config_path(file_path)
        if file_profile and not SourceCodeQAService._spring_profile_matches(file_profile, active_profiles):
            continue
        if file_profile:
            if normalized_key not in profile_overrides:
                values[normalized_key] = set()
                profile_overrides.add(normalized_key)
        elif normalized_key in profile_overrides:
            continue
        values.setdefault(normalized_key, set()).add(normalized_value)
    return values


def _spring_config_rows(connection: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    rows_out: list[tuple[str, str, str, str]] = []
    try:
        rows = connection.execute(
            """
            select file_path, line_text
            from lines
            where lower(file_path) glob '*.properties'
               or lower(file_path) glob '*.yml'
               or lower(file_path) glob '*.yaml'
            order by file_path, line_no
            """
        ).fetchall()
    except sqlite3.Error:
        return rows_out

    current_yaml_file = ""
    yaml_stack: list[tuple[int, str]] = []
    yaml_doc_rows: list[tuple[str, str]] = []
    yaml_doc_profile = ""

    def flush_yaml_doc() -> None:
        nonlocal yaml_doc_rows, yaml_doc_profile
        if not current_yaml_file:
            yaml_doc_rows = []
            yaml_doc_profile = ""
            return
        for key, value in yaml_doc_rows:
            rows_out.append((current_yaml_file, key, value, yaml_doc_profile))
        yaml_doc_rows = []
        yaml_doc_profile = ""

    for file_path, line_text in rows:
        file_path_str = str(file_path)
        suffix = Path(file_path_str).suffix.lower()
        if suffix in {".yaml", ".yml"}:
            stripped = str(line_text or "").strip()
            if current_yaml_file != file_path_str:
                flush_yaml_doc()
                current_yaml_file = file_path_str
                yaml_stack = []
            if stripped.startswith("---"):
                flush_yaml_doc()
                yaml_stack = []
                continue
            pair = SourceCodeQAService._extract_yaml_config_assignment(str(line_text or ""), yaml_stack)
        else:
            if current_yaml_file:
                flush_yaml_doc()
                current_yaml_file = ""
                yaml_stack = []
            pair = SourceCodeQAService._extract_config_assignment(str(line_text or ""))
        if not pair:
            continue
        key, value = pair
        normalized_key = str(key or "").strip().lower()
        normalized_value = str(value or "").strip().strip("\"'")
        if normalized_key and normalized_value:
            if suffix in {".yaml", ".yml"}:
                if normalized_key in {"spring.config.activate.on-profile", "spring.profiles"}:
                    yaml_doc_profile = normalized_value.strip().lower()
                yaml_doc_rows.append((normalized_key, normalized_value))
            else:
                rows_out.append((file_path_str, normalized_key, normalized_value, ""))
    flush_yaml_doc()
    return rows_out


def _spring_profile_from_config_path(file_path: str) -> str:
    name = Path(str(file_path or "")).name.lower()
    match = re.match(r"(?:application|bootstrap)-([a-z0-9_.-]+)\.(?:properties|ya?ml)$", name)
    return match.group(1) if match else ""


def _spring_profile_matches(profile_spec: str, active_profiles: set[str]) -> bool:
    spec = str(profile_spec or "").strip().lower()
    if not spec:
        return False
    candidates = [item.strip().lstrip("!") for item in re.split(r"[,;|\s&()]+", spec) if item.strip()]
    return any(candidate in active_profiles for candidate in candidates)


def _spring_condition_matches(condition: str, config_values: dict[str, set[str]]) -> bool:
    if "=" not in str(condition or ""):
        return False
    key, expected_value = str(condition).split("=", 1)
    normalized_key = key.strip().lower()
    normalized_expected = expected_value.strip().strip("\"'").lower()
    values = config_values.get(normalized_key) or set()
    if normalized_expected == "<missing:true>":
        return not values
    if normalized_expected == "<present>":
        return bool(values) and "false" not in values
    return normalized_expected in values


def _build_flow_edges(connection: sqlite3.Connection) -> int:
    rows: list[tuple[str, int, str, str, str, str, str, int, str]] = []

    for target, kind, file_path, line_no, context in connection.execute(
        """
        select target, kind, file_path, line_no, context
        from references_index
        where kind in ('route', 'sql_table', 'db_read', 'db_write', 'message_publish', 'message_consume', 'event_publish', 'event_consume')
        """
    ):
        edge_kind = str(kind)
        rows.append(
            (
                str(file_path),
                int(line_no),
                SourceCodeQAService._flow_role_for_path(str(file_path)),
                SourceCodeQAService._flow_name_for_path(str(file_path)),
                edge_kind,
                str(target),
                "",
                0,
                str(context or "")[:500],
            )
        )

    for from_file, from_line, symbol, edge_kind, to_file, to_line in connection.execute(
        """
        select from_file, from_line, symbol, edge_kind, to_file, to_line
        from graph_edges
        """
    ):
        classified = SourceCodeQAService._classify_flow_edge(
            str(edge_kind),
            str(from_file),
            str(to_file),
            str(symbol),
        )
        rows.append(
            (
                str(from_file),
                int(from_line),
                SourceCodeQAService._flow_role_for_path(str(from_file)),
                SourceCodeQAService._flow_name_for_path(str(from_file)),
                classified,
                str(symbol),
                str(to_file),
                int(to_line),
                f"{edge_kind} {symbol}".strip()[:500],
            )
        )

    for from_file, from_line, edge_kind, to_name, to_file, to_line, evidence in connection.execute(
        """
        select from_file, from_line, edge_kind, to_name, to_file, to_line, evidence
        from entity_edges
        where edge_kind in (
            'route', 'sql_table', 'injects', 'call', 'import', 'symbol_reference',
            'mapper_statement', 'downstream_api', 'http_endpoint', 'framework_binding',
            'data_flow', 'mapper_interface', 'implements', 'extends', 'implementation_call', 'route_prefix',
            'config_value', 'package', 'module_dependency', 'module_artifact', 'gradle_module', 'gradle_project_dependency',
            'db_read', 'db_write', 'message_publish', 'message_consume',
            'event_publish', 'event_consume', 'bean_qualifier', 'bean_qualifier_target', 'bean_name',
            'bean_primary', 'spring_profile', 'bean_condition',
            'aop_advice', 'aop_pointcut', 'aop_applies_to', 'scheduled_job', 'web_interceptor',
            'runtime_call', 'runtime_route', 'runtime_sql', 'runtime_message', 'runtime_config'
        )
        """
    ):
        rows.append(
            (
                str(from_file),
                int(from_line),
                SourceCodeQAService._flow_role_for_path(str(from_file)),
                SourceCodeQAService._flow_name_for_path(str(from_file)),
                SourceCodeQAService._classify_flow_edge(str(edge_kind), str(from_file), str(to_file), str(to_name)),
                str(to_name),
                str(to_file),
                int(to_line or 0),
                str(evidence or "")[:500],
            )
        )

    rows = list(dict.fromkeys(rows))
    connection.executemany(
        """
        insert into flow_edges(from_file, from_line, from_kind, from_name, edge_kind, to_name, to_file, to_line, evidence)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _flow_name_for_path(path: str) -> str:
    return Path(str(path or "unknown")).stem or "unknown"


def _flow_role_for_path(path: str) -> str:
    lowered = str(path or "").lower()
    if any(term in lowered for term in ("controller", "/api/", "/web/")):
        return "controller"
    if "service" in lowered:
        return "service"
    if "repository" in lowered:
        return "repository"
    if "mapper" in lowered:
        return "mapper"
    if "dao" in lowered:
        return "dao"
    if any(term in lowered for term in ("client", "integration", "gateway", "adapter")):
        return "client"
    if any(term in lowered for term in ("config", "properties", ".yml", ".yaml")):
        return "config"
    return "code"


def _classify_flow_edge(reference_kind: str, from_file: str, to_file: str, symbol: str) -> str:
    del from_file
    lowered_symbol = str(symbol or "").lower()
    to_role = SourceCodeQAService._flow_role_for_path(to_file)
    if str(reference_kind) == "route":
        return "route"
    if str(reference_kind) == "runtime_route":
        return "route"
    if str(reference_kind) == "runtime_sql":
        return "db_runtime"
    if str(reference_kind) == "runtime_message":
        return "message_runtime"
    if str(reference_kind) == "runtime_config":
        return "config"
    if str(reference_kind) == "runtime_call":
        return "runtime"
    if str(reference_kind) == "sql_table":
        return "sql_table"
    if str(reference_kind) in {"db_read", "db_write"}:
        return str(reference_kind)
    if str(reference_kind) in {"message_publish", "message_consume", "event_publish", "event_consume"}:
        return str(reference_kind)
    if str(reference_kind) == "mapper_statement":
        return "mapper"
    if str(reference_kind) == "mapper_interface":
        return "mapper"
    if str(reference_kind) in {"implements", "extends"}:
        return "type_hierarchy"
    if str(reference_kind) == "implementation_call":
        if to_role in {"service", "repository", "mapper", "dao", "controller", "client"}:
            return to_role
        return "implementation"
    if str(reference_kind) in {"downstream_api", "http_endpoint"}:
        return "client"
    if str(reference_kind) == "aop_applies_to":
        if to_role in {"service", "repository", "mapper", "dao", "controller", "client"}:
            return to_role
        return "framework"
    if str(reference_kind) in {"framework_binding", "aop_advice", "aop_pointcut", "scheduled_job", "web_interceptor"}:
        return "framework"
    if str(reference_kind) == "data_flow":
        return "field_population"
    if str(reference_kind) == "config_value":
        return "config"
    if str(reference_kind) == "package":
        return "type_hierarchy"
    if str(reference_kind) in {"module_dependency", "module_artifact", "gradle_module", "gradle_project_dependency"}:
        return "module_dependency"
    if str(reference_kind) in {"bean_qualifier", "bean_qualifier_target", "bean_name", "bean_primary", "spring_profile", "bean_condition"}:
        return "framework"
    if to_role in {"repository", "mapper", "dao"}:
        return to_role
    if to_role == "service":
        return "service"
    if to_role == "controller":
        return "controller"
    if to_role == "client" or any(suffix in lowered_symbol for suffix in ("client", "integration", "gateway")):
        return "client"
    if to_role == "config":
        return "config"
    return "call"


def _entity_id(file_path: str, kind: str, name: str, line_no: int) -> str:
    raw = f"{file_path}:{kind}:{name}:{line_no}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


_CLASS_METHODS = {
    "_build_implementation_edges",
    "_copy_reused_line_rows",
    "_copy_reused_semantic_rows",
    "_copy_unchanged_index_file",
    "_entity_edge_values_by_class",
    "_insert_file_tokens",
    "_insert_semantic_chunk_tokens",
    "_primary_classes_by_lookup_key",
}

_STATIC_METHODS = {
    "_active_spring_profiles",
    "_active_spring_profiles_from_rows",
    "_aop_execution_target_names",
    "_aop_pointcut_expression",
    "_bean_qualifier_lookups",
    "_build_aop_edges",
    "_build_flow_edges",
    "_build_graph_edges",
    "_build_semantic_chunks",
    "_classify_flow_edge",
    "_copy_reused_index_rows",
    "_definition_matches_for_aop_target",
    "_entity_id",
    "_flow_name_for_path",
    "_flow_role_for_path",
    "_index_tokens_for_text",
    "_insert_reused_file_row",
    "_member_call_variable",
    "_qualified_variable_targets",
    "_repo_worktree_clean",
    "_resolve_entity_edges",
    "_semantic_tokens",
    "_service_like_types_from_generic",
    "_spring_condition_matches",
    "_spring_config_rows",
    "_spring_config_values",
    "_spring_profile_from_config_path",
    "_spring_profile_matches",
    "_symbol_lookup_keys",
    "_try_create_file_fts",
    "_try_create_fts",
    "_try_create_semantic_fts",
}

def attach_indexing_helpers(cls: type, global_context: dict[str, object]) -> None:
    helpers = {
        "_index_path": _index_path,
        "_index_lock_path": _index_lock_path,
        "_index_lock_is_stale": _index_lock_is_stale,
        "_acquire_index_lock": _acquire_index_lock,
        "_require_ready_repo_index": _require_ready_repo_index,
        "_repo_index_info": _repo_index_info,
        "_repo_worktree_clean": _repo_worktree_clean,
        "_repo_fingerprint": _repo_fingerprint,
        "_repo_git_revision": _repo_git_revision,
        "_build_repo_index": _build_repo_index,
        "_create_repo_index_schema": _create_repo_index_schema,
        "_open_reusable_index": _open_reusable_index,
        "_copy_reused_index_rows": _copy_reused_index_rows,
        "_insert_reused_file_row": _insert_reused_file_row,
        "_copy_reused_line_rows": _copy_reused_line_rows,
        "_copy_reused_semantic_rows": _copy_reused_semantic_rows,
        "_copy_unchanged_index_file": _copy_unchanged_index_file,
        "_index_tokens_for_text": _index_tokens_for_text,
        "_insert_file_tokens": _insert_file_tokens,
        "_insert_semantic_chunk_tokens": _insert_semantic_chunk_tokens,
        "_build_semantic_chunks": _build_semantic_chunks,
        "_semantic_tokens": _semantic_tokens,
        "_try_create_file_fts": _try_create_file_fts,
        "_try_create_fts": _try_create_fts,
        "_try_create_semantic_fts": _try_create_semantic_fts,
        "_build_graph_edges": _build_graph_edges,
        "_resolve_entity_edges": _resolve_entity_edges,
        "_entity_edge_values_by_class": _entity_edge_values_by_class,
        "_primary_classes_by_lookup_key": _primary_classes_by_lookup_key,
        "_bean_qualifier_lookups": _bean_qualifier_lookups,
        "_build_implementation_edges": _build_implementation_edges,
        "_symbol_lookup_keys": _symbol_lookup_keys,
        "_build_aop_edges": _build_aop_edges,
        "_aop_pointcut_expression": _aop_pointcut_expression,
        "_aop_execution_target_names": _aop_execution_target_names,
        "_definition_matches_for_aop_target": _definition_matches_for_aop_target,
        "_member_call_variable": _member_call_variable,
        "_qualified_variable_targets": _qualified_variable_targets,
        "_service_like_types_from_generic": _service_like_types_from_generic,
        "_active_spring_profiles": _active_spring_profiles,
        "_active_spring_profiles_from_rows": _active_spring_profiles_from_rows,
        "_spring_config_values": _spring_config_values,
        "_spring_config_rows": _spring_config_rows,
        "_spring_profile_from_config_path": _spring_profile_from_config_path,
        "_spring_profile_matches": _spring_profile_matches,
        "_spring_condition_matches": _spring_condition_matches,
        "_build_flow_edges": _build_flow_edges,
        "_flow_name_for_path": _flow_name_for_path,
        "_flow_role_for_path": _flow_role_for_path,
        "_classify_flow_edge": _classify_flow_edge,
        "_entity_id": _entity_id,
    }
    _bind_source_code_qa_globals(list(helpers.values()), global_context)
    for name, helper in helpers.items():
        setattr(cls, name, classmethod(helper) if name in _CLASS_METHODS else staticmethod(helper) if name in _STATIC_METHODS else helper)
