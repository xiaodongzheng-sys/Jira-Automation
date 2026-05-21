"""Java, Spring, tree-sitter, and structure extraction helpers for Source Code QA."""
from __future__ import annotations


def _bind_source_code_qa_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _tree_sitter_parser_for_language(self, language: str) -> Any | None:
    language = str(language or "").strip().lower()
    if not language:
        return None
    if language in self._tree_sitter_parsers:
        return self._tree_sitter_parsers[language]
    try:
        from tree_sitter import Language, Parser

        if language == "java":
            import tree_sitter_java as grammar

            tree_language = Language(grammar.language())
        elif language == "python":
            import tree_sitter_python as grammar

            tree_language = Language(grammar.language())
        elif language == "javascript":
            import tree_sitter_javascript as grammar

            tree_language = Language(grammar.language())
        elif language == "typescript":
            import tree_sitter_typescript as grammar

            tree_language = Language(grammar.language_typescript())
        elif language == "tsx":
            import tree_sitter_typescript as grammar

            tree_language = Language(grammar.language_tsx())
        else:
            self._tree_sitter_parsers[language] = None
            return None
        parser = Parser(tree_language)
        self._tree_sitter_parsers[language] = parser
        return parser
    except Exception as error:
        self._tree_sitter_load_errors[language] = str(error)[:240]
        self._tree_sitter_parsers[language] = None
        return None


def _tree_sitter_language_for_suffix(suffix: str) -> str:
    return {
        ".java": "java",
        ".py": "python",
        ".js": "javascript",
        ".jsx": "javascript",
        ".ts": "typescript",
        ".tsx": "tsx",
    }.get(str(suffix or "").lower(), "")


def _node_text(source: bytes, node: Any) -> str:
    try:
        return source[int(node.start_byte) : int(node.end_byte)].decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _node_line(lines: list[str], node: Any) -> str:
    try:
        line_no = int(node.start_point[0]) + 1
    except Exception:
        line_no = 1
    if 1 <= line_no <= len(lines):
        return lines[line_no - 1].strip()
    return ""


def _node_start_line(node: Any) -> int:
    try:
        return int(node.start_point[0]) + 1
    except Exception:
        return 1


def _first_named_child_text(cls, source: bytes, node: Any, types: set[str]) -> str:
    for child in getattr(node, "named_children", []) or []:
        if str(child.type) in types:
            return cls._node_text(source, child).strip()
    return ""


def _tree_sitter_name_for_node(cls, source: bytes, node: Any) -> str:
    name_node = None
    try:
        name_node = node.child_by_field_name("name")
    except Exception:
        name_node = None
    if name_node is not None:
        value = cls._node_text(source, name_node).strip()
        if value:
            return value
    return cls._first_named_child_text(source, node, {"identifier", "type_identifier", "property_identifier"})


def _tree_sitter_call_target(cls, source: bytes, node: Any) -> str:
    target_node = None
    for field in ("function", "name"):
        try:
            target_node = node.child_by_field_name(field)
        except Exception:
            target_node = None
        if target_node is not None:
            break
    target = cls._node_text(source, target_node).strip() if target_node is not None else ""
    if not target:
        raw = cls._node_text(source, node)
        target = raw.split("(", 1)[0].strip()
    target = target.replace("this.", "").strip()
    return target[-180:]


def _tree_sitter_type_text_for_node(cls, source: bytes, node: Any) -> str:
    type_node = None
    try:
        type_node = node.child_by_field_name("type")
    except Exception:
        type_node = None
    if type_node is not None:
        value = cls._node_text(source, type_node).strip()
        if value:
            return value
    return cls._first_named_child_text(source, node, {"type_identifier", "generic_type"})


def _tree_sitter_string_values(cls, source: bytes, node: Any) -> list[str]:
    values: list[str] = []
    raw = cls._node_text(source, node)
    for value in re.findall(r"[\"']([^\"']+)[\"']", raw):
        if value and value not in values:
            values.append(value)
    return values


def _extract_tree_sitter_structure(
    self,
    *,
    relative_path: str,
    lines: list[str],
    language: str,
    add_definition,
    add_reference,
    add_entity,
    add_entity_edge,
    file_entity_id: str,
) -> tuple[bool, str]:
    parser = self._tree_sitter_parser_for_language(language)
    if parser is None:
        return False, self._tree_sitter_load_errors.get(language, "parser unavailable")
    source = "\n".join(lines).encode("utf-8", errors="ignore")
    try:
        tree = parser.parse(source)
    except Exception as error:
        return False, str(error)[:240]
    root = tree.root_node
    if getattr(root, "has_error", False):
        return False, "parse error"

    def add_route_edges(owner_id: str, node: Any, evidence: str) -> None:
        for route in re.findall(r"[\"']([^\"']+)[\"']", evidence):
            if route.startswith("/") or route.startswith("http"):
                add_reference(route, "route", self._node_start_line(node), evidence)
                add_entity_edge(owner_id, "route", route, self._node_start_line(node), evidence)

    def visit(node: Any, class_id: str, method_id: str, class_name: str = "") -> None:
        node_type = str(getattr(node, "type", ""))
        current_class_id = class_id
        current_method_id = method_id
        node_text_lines = self._node_text(source, node).splitlines()
        signature = self._node_line(lines, node) or (node_text_lines[0][:500] if node_text_lines else node_type)
        node_line = self._node_start_line(node)

        if node_type in {"class_declaration", "interface_declaration", "enum_declaration"}:
            name = self._tree_sitter_name_for_node(source, node)
            if name:
                kind = f"{language}_{node_type.replace('_declaration', '')}"
                add_definition(name, kind, node_line, signature)
                current_class_id = add_entity(name, kind, node_line, signature, parent=Path(relative_path).name)
                current_method_id = current_class_id
                class_name = name
                raw = self._node_text(source, node)[:500]
                for inherited in re.findall(r"\b(?:implements|extends)\s+([A-Z][A-Za-z0-9_]*(?:\s*,\s*[A-Z][A-Za-z0-9_]*)*)", raw):
                    for inherited_name in re.findall(r"[A-Z][A-Za-z0-9_]*", inherited):
                        add_reference(inherited_name, "type_hierarchy", node_line, signature)
                        add_entity_edge(current_class_id, "implements" if "implements" in raw else "extends", inherited_name, node_line, signature)
        elif node_type in {"method_declaration", "method_definition", "function_declaration", "function_definition"}:
            name = self._tree_sitter_name_for_node(source, node)
            if name:
                kind = f"{language}_{'method' if 'method' in node_type else 'function'}"
                add_definition(name, kind, node_line, signature)
                current_method_id = add_entity(name, kind, node_line, signature, parent=class_name)
                if class_name and "method" in kind:
                    qualified_name = f"{class_name}.{name}"
                    add_definition(qualified_name, kind, node_line, signature)
                    add_entity(qualified_name, kind, node_line, signature, parent=class_name)
        elif node_type in {"field_declaration", "public_field_definition", "variable_declarator"}:
            raw = self._node_text(source, node)
            type_name = self._tree_sitter_type_text_for_node(source, node) or self._first_named_child_text(source, node, {"type_identifier", "generic_type", "identifier"})
            variable_names = [
                value
                for value in IDENTIFIER_PATTERN.findall(raw)
                if value not in {"private", "public", "protected", "static", "final", type_name}
            ]
            if variable_names:
                add_definition(variable_names[-1], f"{language}_field", node_line, signature)
            if type_name and type_name not in variable_names and len(type_name) >= 3:
                add_reference(type_name, "field_type", node_line, signature)
                add_entity_edge(current_class_id or file_entity_id, "injects", type_name, node_line, signature)
        elif node_type in {"import_statement", "import_from_statement", "import_declaration"}:
            for value in self._tree_sitter_string_values(source, node):
                add_reference(value, "import", node_line, signature)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "import", value, node_line, signature)
            for dotted in re.findall(r"(?:import|from)\s+([A-Za-z0-9_.*{} ,/.-]+)", signature):
                add_reference(dotted.strip(), "import", node_line, signature)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "import", dotted.strip(), node_line, signature)
        elif node_type in {"method_invocation", "call", "call_expression"}:
            target = self._tree_sitter_call_target(source, node)
            if target and target.lower() not in LOW_VALUE_CALL_SYMBOLS:
                add_reference(target, "call", node_line, signature)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", target, node_line, signature)

        if node_type in {"annotation", "marker_annotation", "decorator"} or "@" in signature:
            if "FeignClient" in signature:
                for value in re.findall(r"[\"']([^\"']+)[\"']", signature):
                    add_reference(value, "downstream_api", node_line, signature)
                    add_entity_edge(current_class_id or file_entity_id, "downstream_api", value, node_line, signature)
            if any(marker in signature for marker in ("RestController", "Controller", "Service", "Repository", "Component")):
                for marker in re.findall(r"@([A-Za-z0-9_]+)", signature):
                    add_reference(marker, "framework_binding", node_line, signature)
                    add_entity_edge(current_class_id or file_entity_id, "framework_binding", marker, node_line, signature)
            add_route_edges(current_method_id or current_class_id or file_entity_id, node, signature)

        raw_text = self._node_text(source, node)
        if node_type in {"string", "string_literal"} or any(client in signature.lower() for client in ("fetch", "axios", "resttemplate", "webclient")):
            for endpoint in HTTP_LITERAL_PATTERN.findall(raw_text or signature):
                add_reference(endpoint, "http_endpoint", node_line, signature)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "http_endpoint", endpoint, node_line, signature)

        for child in getattr(node, "named_children", []) or []:
            visit(child, current_class_id, current_method_id, class_name)

    visit(root, file_entity_id, file_entity_id)
    return True, ""


def _extract_structure_rows(self, relative_path: str, lines: list[str]) -> dict[str, list[tuple[Any, ...]]]:
    definitions: list[tuple[Any, ...]] = []
    references: list[tuple[Any, ...]] = []
    entities: list[tuple[Any, ...]] = []
    entity_edges: list[tuple[Any, ...]] = []
    suffix = Path(relative_path).suffix.lower()
    language = self._language_for_suffix(suffix)
    file_entity_id = self._entity_id(relative_path, "file", relative_path, 1)
    entities.append((file_entity_id, relative_path, relative_path.lower(), "file", language, relative_path, 1, "", relative_path))

    def add_definition(name: str, kind: str, line_no: int, signature: str) -> None:
        name = str(name or "").strip()
        if not name:
            return
        definitions.append((name, name.lower(), kind, relative_path, line_no, signature.strip()[:500]))
        add_entity(name, kind, line_no, signature)

    def add_reference(target: str, kind: str, line_no: int, context: str) -> None:
        target = str(target or "").strip().strip("\"'")
        if len(target) < 2:
            return
        references.append((target, target.lower(), kind, relative_path, line_no, context.strip()[:500]))

    def add_entity(name: str, kind: str, line_no: int, signature: str, parent: str = "") -> str:
        normalized = str(name or "").strip()
        if not normalized:
            return file_entity_id
        entity_id = self._entity_id(relative_path, kind, normalized, line_no)
        entities.append(
            (
                entity_id,
                normalized,
                normalized.lower(),
                kind,
                language,
                relative_path,
                int(line_no),
                str(parent or ""),
                str(signature or "").strip()[:500],
            )
        )
        return entity_id

    def add_entity_edge(
        from_entity_id: str,
        edge_kind: str,
        to_name: str,
        line_no: int,
        evidence: str,
    ) -> None:
        target = str(to_name or "").strip().strip("\"'")
        if len(target) < 2:
            return
        entity_edges.append(
            (
                from_entity_id or file_entity_id,
                relative_path,
                int(line_no),
                str(edge_kind or "reference"),
                target,
                target.lower(),
                "",
                "",
                0,
                str(evidence or "").strip()[:500],
            )
        )

    if suffix == ".py":
        self._extract_python_ast_structure(
            relative_path=relative_path,
            lines=lines,
            add_definition=add_definition,
            add_reference=add_reference,
            add_entity=add_entity,
            add_entity_edge=add_entity_edge,
            file_entity_id=file_entity_id,
        )

    self._extract_build_file_structure(
        relative_path=relative_path,
        lines=lines,
        add_definition=add_definition,
        add_reference=add_reference,
        add_entity_edge=add_entity_edge,
        file_entity_id=file_entity_id,
    )
    self._extract_runtime_trace_structure(
        relative_path=relative_path,
        lines=lines,
        add_reference=add_reference,
        add_entity_edge=add_entity_edge,
        file_entity_id=file_entity_id,
    )

    tree_sitter_language = self._tree_sitter_language_for_suffix(suffix)
    tree_sitter_used = False
    tree_sitter_error = ""
    if tree_sitter_language:
        tree_sitter_used, tree_sitter_error = self._extract_tree_sitter_structure(
            relative_path=relative_path,
            lines=lines,
            language=tree_sitter_language,
            add_definition=add_definition,
            add_reference=add_reference,
            add_entity=add_entity,
            add_entity_edge=add_entity_edge,
            file_entity_id=file_entity_id,
        )

    current_class = ""
    current_class_id = file_entity_id
    current_method = ""
    current_method_id = file_entity_id
    class_routes: list[str] = []
    pending_routes: list[tuple[str, int, str, str]] = []
    mapper_namespace = ""
    mapper_namespace_id = file_entity_id
    variable_types: dict[str, str] = {}
    collection_element_types: dict[str, str] = {}
    java_package = ""
    java_imports: dict[str, str] = {}
    pending_bean_names: list[tuple[str, int, str]] = []
    pending_profiles: list[tuple[str, int, str]] = []
    pending_primary: list[tuple[int, str]] = []
    pending_conditions: list[tuple[str, int, str]] = []
    pending_qualifiers: list[tuple[str, int, str]] = []
    pending_class_framework_edges: list[tuple[str, str, int, str]] = []
    pending_method_framework_edges: list[tuple[str, str, int, str]] = []
    variable_qualifiers: dict[str, set[str]] = {}
    yaml_config_stack: list[tuple[int, str]] = []
    is_test_file = self._is_test_file_path(relative_path)
    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        if is_test_file or TEST_ANNOTATION_PATTERN.search(stripped):
            self._extract_test_structure_references(
                stripped=stripped,
                line_no=line_no,
                current_entity_id=current_method_id or current_class_id or file_entity_id,
                add_reference=add_reference,
                add_entity_edge=add_entity_edge,
            )
        package_match = JAVA_PACKAGE_PATTERN.search(stripped)
        if package_match:
            java_package = package_match.group(1)
            add_definition(java_package, "java_package", line_no, stripped)
            add_entity_edge(file_entity_id, "package", java_package, line_no, stripped)
        import_match = JAVA_IMPORT_PATTERN.search(stripped)
        if import_match:
            imported_name = import_match.group(1)
            short_import = imported_name.rsplit(".", 1)[-1]
            if short_import and short_import != "*":
                java_imports[short_import] = imported_name
            add_reference(imported_name, "import", line_no, stripped)
            add_entity_edge(file_entity_id, "import", imported_name, line_no, stripped)
        namespace_match = MYBATIS_NAMESPACE_PATTERN.search(line)
        if namespace_match:
            mapper_namespace = namespace_match.group(1)
            add_definition(mapper_namespace, "mybatis_mapper_namespace", line_no, stripped)
            mapper_namespace_id = self._entity_id(relative_path, "mybatis_mapper_namespace", mapper_namespace, line_no)
            mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
            if mapper_short_name:
                add_reference(mapper_short_name, "mapper_interface", line_no, stripped)
                add_entity_edge(mapper_namespace_id, "mapper_interface", mapper_short_name, line_no, stripped)
        result_map_match = MYBATIS_RESULT_MAP_PATTERN.search(line)
        if result_map_match:
            result_map_name = f"{mapper_namespace}.{result_map_match.group(1)}" if mapper_namespace else result_map_match.group(1)
            add_definition(result_map_name, "mybatis_result_map", line_no, stripped)
            result_map_id = self._entity_id(relative_path, "mybatis_result_map", result_map_name, line_no)
            add_entity_edge(mapper_namespace_id, "result_map", result_map_name, line_no, stripped)
            if mapper_namespace:
                mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
                short_result_map = f"{mapper_short_name}.{result_map_match.group(1)}"
                add_definition(short_result_map, "mybatis_result_map", line_no, stripped)
                add_entity_edge(result_map_id, "result_map_alias", short_result_map, line_no, stripped)
        include_match = MYBATIS_INCLUDE_PATTERN.search(line)
        if include_match:
            include_ref = include_match.group(1)
            add_reference(include_ref, "mybatis_include_refid", line_no, stripped)
            add_entity_edge(current_method_id or mapper_namespace_id, "mybatis_include_refid", include_ref, line_no, stripped)
        for attr_name, attr_value in MYBATIS_ATTR_REFERENCE_PATTERN.findall(line):
            if not attr_value:
                continue
            attr_kind = "mybatis_result_map_ref" if attr_name.lower() == "resultmap" else "mybatis_type_ref"
            add_reference(attr_value, attr_kind, line_no, stripped)
            add_entity_edge(current_method_id or mapper_namespace_id, attr_kind, attr_value, line_no, stripped)
        statement_match = MYBATIS_STATEMENT_PATTERN.search(line)
        if statement_match:
            statement_name = f"{mapper_namespace}.{statement_match.group(2)}" if mapper_namespace else statement_match.group(2)
            add_definition(statement_name, f"mybatis_{statement_match.group(1).lower()}", line_no, stripped)
            statement_id = self._entity_id(relative_path, f"mybatis_{statement_match.group(1).lower()}", statement_name, line_no)
            add_entity_edge(mapper_namespace_id, "mapper_statement", statement_name, line_no, stripped)
            if mapper_namespace:
                mapper_short_name = mapper_namespace.rsplit(".", 1)[-1]
                qualified_statement = f"{mapper_short_name}.{statement_match.group(2)}"
                add_definition(qualified_statement, f"mybatis_{statement_match.group(1).lower()}", line_no, stripped)
                add_entity_edge(mapper_namespace_id, "mapper_statement", qualified_statement, line_no, stripped)
            current_method = statement_name
            current_method_id = statement_id
        feign_match = FEIGN_CLIENT_PATTERN.search(line)
        if feign_match:
            for value in re.findall(r'"([^"]+)"', feign_match.group(1)):
                add_reference(value, "downstream_api", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "downstream_api", value, line_no, stripped)
        for match in CLASS_DEF_PATTERN.finditer(line):
            add_definition(match.group(2), match.group(1).lower(), line_no, stripped)
            current_class = match.group(2)
            if java_package:
                add_definition(f"{java_package}.{current_class}", match.group(1).lower(), line_no, stripped)
            current_class_id = self._entity_id(relative_path, match.group(1).lower(), current_class, line_no)
            for bean_name, bean_line, bean_context in pending_bean_names:
                add_reference(bean_name, "bean_name", bean_line, bean_context)
                add_entity_edge(current_class_id, "bean_name", bean_name, bean_line, bean_context)
            pending_bean_names = []
            for profile_name, profile_line, profile_context in pending_profiles:
                add_reference(profile_name, "spring_profile", profile_line, profile_context)
                add_entity_edge(current_class_id, "spring_profile", profile_name, profile_line, profile_context)
            pending_profiles = []
            for primary_line, primary_context in pending_primary:
                add_reference(current_class, "bean_primary", primary_line, primary_context)
                add_entity_edge(current_class_id, "bean_primary", current_class, primary_line, primary_context)
            pending_primary = []
            for condition, condition_line, condition_context in pending_conditions:
                add_reference(condition, "bean_condition", condition_line, condition_context)
                add_entity_edge(current_class_id, "bean_condition", condition, condition_line, condition_context)
            pending_conditions = []
            for edge_kind, target, edge_line, edge_context in pending_class_framework_edges:
                add_reference(target, edge_kind, edge_line, edge_context)
                add_entity_edge(current_class_id, edge_kind, target, edge_line, edge_context)
            pending_class_framework_edges = []
            current_method = ""
            current_method_id = current_class_id
            class_routes = [route for route, _, _, annotation_name in pending_routes if annotation_name == "RequestMapping"]
            for route, route_line, route_context, annotation_name in pending_routes:
                if annotation_name == "RequestMapping":
                    add_reference(route, "route", route_line, route_context)
                    add_entity_edge(current_class_id, "route", route, route_line, route_context)
            pending_routes = [item for item in pending_routes if item[3] != "RequestMapping"]
            for inherited in re.findall(r"\b(?:implements|extends)\s+([A-Z][A-Za-z0-9_]*(?:\s*,\s*[A-Z][A-Za-z0-9_]*)*)", stripped):
                for inherited_name in re.findall(r"[A-Z][A-Za-z0-9_]*", inherited):
                    edge_kind = "implements" if "implements" in stripped else "extends"
                    add_reference(inherited_name, "type_hierarchy", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, edge_kind, inherited_name, line_no, stripped)
        py_match = PY_DEF_PATTERN.search(line)
        if py_match and suffix != ".py":
            add_definition(py_match.group(2), "python_" + py_match.group(1).lower(), line_no, stripped)
        js_match = JS_DEF_PATTERN.search(line)
        if js_match:
            function_name = js_match.group(1) or js_match.group(2)
            add_definition(function_name, "javascript_function", line_no, stripped)
            current_method = function_name
            current_method_id = self._entity_id(relative_path, "javascript_function", function_name, line_no)
        java_method = JAVA_METHOD_DEF_PATTERN.search(line)
        if java_method and not stripped.startswith(("if ", "for ", "while ", "switch ", "catch ")):
            method_name = java_method.group(1)
            add_definition(method_name, "java_method", line_no, stripped)
            if current_class:
                add_definition(f"{current_class}.{method_name}", "java_method", line_no, stripped)
                if java_package:
                    add_definition(f"{java_package}.{current_class}.{method_name}", "java_method", line_no, stripped)
            current_method = method_name
            current_method_id = self._entity_id(relative_path, "java_method", method_name, line_no)
            for edge_kind, target, edge_line, edge_context in pending_method_framework_edges:
                add_reference(target, edge_kind, edge_line, edge_context)
                add_entity_edge(current_method_id, edge_kind, target, edge_line, edge_context)
            pending_method_framework_edges = []
            for route, route_line, route_context, _annotation_name in pending_routes:
                add_reference(route, "route", route_line, route_context)
                add_entity_edge(current_method_id, "route", route, route_line, route_context)
                for class_route in class_routes:
                    joined_route = self._join_routes(class_route, route)
                    if joined_route and joined_route != route:
                        add_reference(joined_route, "route", route_line, route_context)
                        add_entity_edge(current_method_id, "route", joined_route, route_line, route_context)
                        add_entity_edge(current_class_id or file_entity_id, "route_prefix", joined_route, route_line, route_context)
            if current_class and method_name == current_class:
                for parameter_type in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Service|Repository|Mapper|Client|Gateway|Adapter|Dao))\s+[a-z][A-Za-z0-9_]*", stripped):
                    add_reference(parameter_type, "field_type", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "injects", parameter_type, line_no, stripped)
            pending_routes = []
        for annotation in ANNOTATION_ROUTE_PATTERN.finditer(line):
            add_definition(annotation.group(1), "route_annotation", line_no, stripped)
            for route in re.findall(r'"([^"]+)"', annotation.group(2) or ""):
                add_reference(route, "route", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id, "route", route, line_no, stripped)
                pending_routes.append((route, line_no, stripped, annotation.group(1)))
        spring_value_match = SPRING_VALUE_PATTERN.search(line)
        if spring_value_match:
            config_key = spring_value_match.group(1)
            add_reference(config_key, "config_key", line_no, stripped)
            add_entity_edge(current_method_id or current_class_id or file_entity_id, "config", config_key, line_no, stripped)
        qualifier_match = SPRING_QUALIFIER_PATTERN.search(line)
        line_qualifiers: list[tuple[str, int, str]] = []
        for qualifier_match in SPRING_QUALIFIER_PATTERN.finditer(line):
            qualifier = qualifier_match.group(2) or ""
            if qualifier:
                add_reference(qualifier, "bean_qualifier", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "bean_qualifier", qualifier, line_no, stripped)
                line_qualifiers.append((qualifier, line_no, stripped))
                if stripped.startswith("@") and not FIELD_VAR_TYPE_PATTERN.search(stripped):
                    pending_qualifiers.append((qualifier, line_no, stripped))
        bean_match = SPRING_BEAN_NAME_PATTERN.search(line)
        if bean_match:
            bean_name = bean_match.group(2) or ""
            if bean_name:
                target_id = current_class_id if current_class and not stripped.startswith("@") else file_entity_id
                if target_id == file_entity_id:
                    pending_bean_names.append((bean_name, line_no, stripped))
                else:
                    add_reference(bean_name, "bean_name", line_no, stripped)
                    add_entity_edge(target_id, "bean_name", bean_name, line_no, stripped)
        if SPRING_PRIMARY_PATTERN.search(line):
            if current_class and not stripped.startswith("@"):
                add_reference(current_class, "bean_primary", line_no, stripped)
                add_entity_edge(current_class_id, "bean_primary", current_class, line_no, stripped)
            else:
                pending_primary.append((line_no, stripped))
        if SPRING_ASPECT_PATTERN.search(line):
            if current_class and not stripped.startswith("@"):
                add_reference("Aspect", "framework_binding", line_no, stripped)
                add_entity_edge(current_class_id, "framework_binding", "Aspect", line_no, stripped)
            else:
                pending_class_framework_edges.append(("framework_binding", "Aspect", line_no, stripped))
        if SPRING_INTERCEPTOR_PATTERN.search(line):
            target = current_class or "HandlerInterceptor"
            add_reference(target, "web_interceptor", line_no, stripped)
            add_entity_edge(current_class_id or file_entity_id, "web_interceptor", target, line_no, stripped)
        for boundary_match in OPERATIONAL_BOUNDARY_PATTERN.finditer(line):
            boundary_name = boundary_match.group(1)
            boundary_args = self._annotation_target_text(boundary_match.group(2) or "")
            boundary_target = f"{boundary_name}:{boundary_args}" if boundary_args else boundary_name
            add_reference(boundary_target, "operational_boundary", line_no, stripped)
            if current_method and not stripped.startswith("@"):
                add_entity_edge(current_method_id, "operational_boundary", boundary_target, line_no, stripped)
            else:
                pending_method_framework_edges.append(("operational_boundary", boundary_target, line_no, stripped))
        for aop_match in SPRING_AOP_PATTERN.finditer(line):
            advice_kind = aop_match.group(1)
            pointcut_text = self._annotation_target_text(aop_match.group(2) or stripped) or advice_kind
            pointcut_target = f"{advice_kind}:{pointcut_text}"
            edge_kind = "aop_pointcut" if advice_kind == "Pointcut" else "aop_advice"
            add_reference(pointcut_target, edge_kind, line_no, stripped)
            if current_method and not stripped.startswith("@"):
                add_entity_edge(current_method_id, edge_kind, pointcut_target, line_no, stripped)
            else:
                pending_method_framework_edges.append((edge_kind, pointcut_target, line_no, stripped))
        scheduled_match = SPRING_SCHEDULED_PATTERN.search(line)
        if scheduled_match:
            schedule_target = self._scheduled_target_text(scheduled_match.group(1) or "") or "scheduled"
            add_reference(schedule_target, "scheduled_job", line_no, stripped)
            if current_method and not stripped.startswith("@"):
                add_entity_edge(current_method_id, "scheduled_job", schedule_target, line_no, stripped)
            else:
                pending_method_framework_edges.append(("scheduled_job", schedule_target, line_no, stripped))
        profile_match = SPRING_PROFILE_PATTERN.search(line)
        if profile_match:
            for profile in re.findall(r'"([^"]+)"|\'([^\']+)\'', profile_match.group(1)):
                profile_name = next((item for item in profile if item), "")
                if profile_name:
                    if current_class and not stripped.startswith("@"):
                        add_reference(profile_name, "spring_profile", line_no, stripped)
                        add_entity_edge(current_class_id, "spring_profile", profile_name, line_no, stripped)
                    else:
                        pending_profiles.append((profile_name, line_no, stripped))
        conditional_match = SPRING_CONDITIONAL_ON_PROPERTY_PATTERN.search(line)
        if conditional_match:
            condition_entries = self._spring_conditional_on_property_entries(conditional_match.group(1))
            for condition in condition_entries:
                add_reference(condition, "bean_condition", line_no, stripped)
                if current_class and not stripped.startswith("@"):
                    add_entity_edge(current_class_id, "bean_condition", condition, line_no, stripped)
                else:
                    pending_conditions.append((condition, line_no, stripped))
            for property_name in (
                self._spring_annotation_arg_values(conditional_match.group(1), "name")
                or self._spring_annotation_arg_values(conditional_match.group(1), "value")
            ):
                add_reference(property_name, "config_key", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "config", property_name, line_no, stripped)
            for property_name in self._spring_annotation_arg_values(conditional_match.group(1), "prefix"):
                add_reference(property_name, "config_key", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "config", property_name, line_no, stripped)
        self._extract_message_event_structure(
            line=line,
            stripped=stripped,
            line_no=line_no,
            current_method_id=current_method_id,
            current_class_id=current_class_id,
            file_entity_id=file_entity_id,
            add_reference=add_reference,
            add_entity_edge=add_entity_edge,
        )
        self._extract_sql_http_config_structure(
            line=line,
            stripped=stripped,
            line_no=line_no,
            suffix=suffix,
            yaml_config_stack=yaml_config_stack,
            current_method_id=current_method_id,
            current_class_id=current_class_id,
            file_entity_id=file_entity_id,
            add_definition=add_definition,
            add_reference=add_reference,
            add_entity_edge=add_entity_edge,
        )
        field_match = FIELD_OR_PARAM_TYPE_PATTERN.search(line)
        if field_match:
            add_entity_edge(current_class_id or file_entity_id, "injects", field_match.group(1), line_no, stripped)
        qualified_variable_targets = self._qualified_variable_targets(stripped)
        typed_variables = FIELD_VAR_TYPE_PATTERN.findall(stripped)
        for type_name, variable_name in typed_variables:
            variable_types[variable_name] = type_name
            add_reference(type_name, "field_type", line_no, stripped)
            add_entity_edge(current_class_id or file_entity_id, "injects", type_name, line_no, stripped)
            targeted_qualifiers = [
                (qualifier, line_no, stripped)
                for qualifier in qualified_variable_targets.get(variable_name, [])
            ]
            fallback_line_qualifiers = line_qualifiers if not targeted_qualifiers and len(typed_variables) == 1 else []
            for qualifier, qualifier_line, qualifier_context in pending_qualifiers + targeted_qualifiers + fallback_line_qualifiers:
                add_reference(f"{variable_name}={qualifier}", "bean_qualifier_target", qualifier_line, qualifier_context)
                add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{variable_name}={qualifier}", qualifier_line, qualifier_context)
                variable_qualifiers.setdefault(variable_name, set()).add(qualifier)
            if pending_qualifiers:
                pending_qualifiers = []
            imported_type = java_imports.get(type_name)
            if imported_type:
                add_reference(imported_type, "field_type", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "injects", imported_type, line_no, stripped)
        for field_name, source_variable in THIS_FIELD_ASSIGNMENT_PATTERN.findall(stripped):
            for qualifier in sorted(variable_qualifiers.get(source_variable, set())):
                add_reference(f"{field_name}={qualifier}", "bean_qualifier_target", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{field_name}={qualifier}", line_no, stripped)
                variable_qualifiers.setdefault(field_name, set()).add(qualifier)
        simple_variable_names = {variable_name for _type_name, variable_name in typed_variables}
        for generic_text, variable_name in GENERIC_FIELD_VAR_TYPE_PATTERN.findall(stripped):
            if variable_name in simple_variable_names:
                continue
            inner_types = self._service_like_types_from_generic(generic_text)
            if not inner_types:
                continue
            element_type = inner_types[-1]
            collection_element_types[variable_name] = element_type
            add_reference(element_type, "field_type", line_no, stripped)
            add_entity_edge(current_class_id or file_entity_id, "injects", element_type, line_no, stripped)
            targeted_qualifiers = [
                (qualifier, line_no, stripped)
                for qualifier in qualified_variable_targets.get(variable_name, [])
            ]
            fallback_line_qualifiers = line_qualifiers if not targeted_qualifiers else []
            for qualifier, qualifier_line, qualifier_context in pending_qualifiers + targeted_qualifiers + fallback_line_qualifiers:
                add_reference(f"{variable_name}={qualifier}", "bean_qualifier_target", qualifier_line, qualifier_context)
                add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{variable_name}={qualifier}", qualifier_line, qualifier_context)
                variable_qualifiers.setdefault(variable_name, set()).add(qualifier)
            if pending_qualifiers:
                pending_qualifiers = []
            imported_type = java_imports.get(element_type)
            if imported_type:
                add_reference(imported_type, "field_type", line_no, stripped)
                add_entity_edge(current_class_id or file_entity_id, "injects", imported_type, line_no, stripped)
        for collection_variable, lambda_variable in STREAM_LAMBDA_PATTERN.findall(stripped):
            element_type = collection_element_types.get(collection_variable) or variable_types.get(collection_variable)
            if element_type:
                variable_types[lambda_variable] = element_type
                for qualifier in sorted(variable_qualifiers.get(collection_variable, set())):
                    add_reference(f"{lambda_variable}={qualifier}", "bean_qualifier_target", line_no, stripped)
                    add_entity_edge(current_class_id or file_entity_id, "bean_qualifier_target", f"{lambda_variable}={qualifier}", line_no, stripped)
                    variable_qualifiers.setdefault(lambda_variable, set()).add(qualifier)
        for provider_variable, method_name in PROVIDER_CHAIN_CALL_PATTERN.findall(stripped):
            owner_type = collection_element_types.get(provider_variable) or variable_types.get(provider_variable)
            if owner_type:
                qualified_call = f"{owner_type}.{method_name}"
                add_reference(qualified_call, "call", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", qualified_call, line_no, stripped)
                imported_type = java_imports.get(owner_type)
                if imported_type:
                    imported_call = f"{imported_type}.{method_name}"
                    add_reference(imported_call, "call", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", imported_call, line_no, stripped)
        for variable_name, method_name in MEMBER_CALL_PATTERN.findall(stripped):
            owner_type = variable_types.get(variable_name)
            if owner_type:
                qualified_call = f"{owner_type}.{method_name}"
                add_reference(qualified_call, "call", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", qualified_call, line_no, stripped)
                imported_type = java_imports.get(owner_type)
                if imported_type:
                    imported_call = f"{imported_type}.{method_name}"
                    add_reference(imported_call, "call", line_no, stripped)
                    add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", imported_call, line_no, stripped)
        for target in self._extract_data_flow_targets(stripped):
            add_reference(target, "data_flow", line_no, stripped)
            add_entity_edge(current_method_id or current_class_id or file_entity_id, "data_flow", target, line_no, stripped)
        for symbol in self._extract_downstream_symbols(line):
            add_reference(symbol, "symbol_reference", line_no, stripped)
            add_entity_edge(current_method_id or current_class_id or file_entity_id, "symbol_reference", symbol, line_no, stripped)
        for call in CALL_SYMBOL_PATTERN.findall(line):
            lowered = call.lower()
            if lowered not in LOW_VALUE_CALL_SYMBOLS and lowered not in STOPWORDS:
                add_reference(call, "call", line_no, stripped)
                add_entity_edge(current_method_id or current_class_id or file_entity_id, "call", call, line_no, stripped)

    return {
        "definitions": self._dedupe_structure_rows(definitions),
        "references": self._dedupe_structure_rows(references),
        "entities": self._dedupe_structure_rows(entities),
        "entity_edges": self._dedupe_structure_rows(entity_edges),
        "tree_sitter_used": tree_sitter_used,
        "tree_sitter_language": tree_sitter_language if tree_sitter_used else "",
        "tree_sitter_error": tree_sitter_error,
    }


def _dedupe_structure_rows(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return list(dict.fromkeys(rows))


def _extract_sql_http_config_structure(
    self,
    *,
    line: str,
    stripped: str,
    line_no: int,
    suffix: str,
    yaml_config_stack: list[tuple[int, str]],
    current_method_id: str,
    current_class_id: str,
    file_entity_id: str,
    add_definition: Any,
    add_reference: Any,
    add_entity_edge: Any,
) -> None:
    for table in SQL_TABLE_PATTERN.findall(line):
        add_reference(table, "sql_table", line_no, stripped)
        add_entity_edge(current_method_id or current_class_id, "sql_table", table, line_no, stripped)
    for table in SQL_READ_TABLE_PATTERN.findall(line):
        add_reference(table, "db_read", line_no, stripped)
        add_entity_edge(current_method_id or current_class_id, "db_read", table, line_no, stripped)
    for table in SQL_WRITE_TABLE_PATTERN.findall(line):
        add_reference(table, "db_write", line_no, stripped)
        add_entity_edge(current_method_id or current_class_id, "db_write", table, line_no, stripped)
    for endpoint in HTTP_LITERAL_PATTERN.findall(line):
        if endpoint.startswith("http") or any(
            client in stripped.lower()
            for client in ("resttemplate", "webclient", "feign", "exchange", "postfor", "getfor", "request")
        ):
            add_reference(endpoint, "http_endpoint", line_no, stripped)
            add_entity_edge(current_method_id or current_class_id or file_entity_id, "http_endpoint", endpoint, line_no, stripped)
    if suffix in {".properties", ".yaml", ".yml", ".conf", ".toml"}:
        config_pair = (
            self._extract_yaml_config_assignment(line, yaml_config_stack)
            if suffix in {".yaml", ".yml"}
            else self._extract_config_assignment(stripped)
        )
        key_match = PROPERTIES_KEY_PATTERN.search(line)
        if config_pair:
            config_key, config_value = config_pair
            add_definition(config_key, "config_key", line_no, stripped)
            add_entity_edge(file_entity_id, "config", config_key, line_no, stripped)
            if config_value:
                add_reference(config_value, "config_value", line_no, stripped)
                add_entity_edge(file_entity_id, "config_value", config_value, line_no, stripped)
            for endpoint in HTTP_LITERAL_PATTERN.findall(f"'{config_value}'"):
                add_reference(endpoint, "http_endpoint", line_no, stripped)
                add_entity_edge(file_entity_id, "http_endpoint", endpoint, line_no, stripped)
            if re.search(r"\b[a-z0-9-]+-service\b", config_value, re.IGNORECASE):
                add_reference(config_value, "downstream_api", line_no, stripped)
                add_entity_edge(file_entity_id, "downstream_api", config_value, line_no, stripped)
        elif key_match:
            add_definition(key_match.group(1), "config_key", line_no, stripped)
            add_entity_edge(file_entity_id, "config", key_match.group(1), line_no, stripped)


def _extract_message_event_structure(
    self,
    *,
    line: str,
    stripped: str,
    line_no: int,
    current_method_id: str,
    current_class_id: str,
    file_entity_id: str,
    add_reference: Any,
    add_entity_edge: Any,
) -> None:
    current_entity_id = current_method_id or current_class_id or file_entity_id
    listener_match = MESSAGE_LISTENER_PATTERN.search(line)
    if listener_match:
        for topic in self._extract_message_names(listener_match.group(2)):
            add_reference(topic, "message_consume", line_no, stripped)
            add_entity_edge(current_entity_id, "message_consume", topic, line_no, stripped)
    for send_match in MESSAGE_SEND_PATTERN.finditer(line):
        for topic in self._extract_message_names(send_match.group(1)):
            add_reference(topic, "message_publish", line_no, stripped)
            add_entity_edge(current_entity_id, "message_publish", topic, line_no, stripped)
    for event_match in EVENT_PUBLISH_PATTERN.finditer(line):
        for event_name in self._extract_event_names(event_match.group(1)):
            add_reference(event_name, "event_publish", line_no, stripped)
            add_entity_edge(current_entity_id, "event_publish", event_name, line_no, stripped)
    if "@EventListener" in stripped or "@TransactionalEventListener" in stripped:
        for event_name in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\b", stripped):
            add_reference(event_name, "event_consume", line_no, stripped)
            add_entity_edge(current_entity_id, "event_consume", event_name, line_no, stripped)


def _extract_test_structure_references(
    self,
    *,
    stripped: str,
    line_no: int,
    current_entity_id: str,
    add_reference: Any,
    add_entity_edge: Any,
) -> None:
    if TEST_ANNOTATION_PATTERN.search(stripped) or re.search(r"\b(?:test|should)[A-Za-z0-9_]*\s*\(", stripped):
        add_reference("test_case", "test_case", line_no, stripped)
        add_entity_edge(current_entity_id, "test_case", "test_case", line_no, stripped)
    if TEST_ASSERTION_PATTERN.search(stripped):
        add_reference("assertion", "test_assertion", line_no, stripped)
        add_entity_edge(current_entity_id, "test_assertion", "assertion", line_no, stripped)
    for call_name in CALL_SYMBOL_PATTERN.findall(stripped):
        if call_name.lower() not in LOW_VALUE_CALL_SYMBOLS and len(call_name) >= 3:
            add_reference(call_name, "test_reference", line_no, stripped)
            add_entity_edge(current_entity_id, "test_reference", call_name, line_no, stripped)
    for subject in re.findall(r"\b([A-Z][A-Za-z0-9_]{3,})\b", stripped):
        if subject in {"Test", "BeforeEach", "AfterEach", "Autowired", "MockBean", "Mockito", "Assertions", "Assert"}:
            continue
        if subject.endswith(("Test", "Tests", "Spec")):
            continue
        add_reference(subject, "test_subject", line_no, stripped)
        add_entity_edge(current_entity_id, "test_subject", subject, line_no, stripped)


def _language_for_suffix(suffix: str) -> str:
    return {
        ".java": "java",
        ".py": "python",
        ".js": "javascript",
        ".jsx": "javascript",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".xml": "xml",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".properties": "properties",
        ".sql": "sql",
    }.get(str(suffix or "").lower(), "text")


def _extract_config_assignment(line: str) -> tuple[str, str] | None:
    stripped = str(line or "").strip()
    if not stripped or stripped.startswith(("#", "//", "- ")):
        return None
    match = CONFIG_ASSIGNMENT_PATTERN.search(stripped)
    if not match:
        return None
    key = match.group(1).strip()
    value = match.group(2).strip().strip("\"'")
    if not key or not value:
        return None
    return key, value


def _extract_runtime_trace_structure(
    self,
    *,
    relative_path: str,
    lines: list[str],
    add_reference: Any,
    add_entity_edge: Any,
    file_entity_id: str,
) -> None:
    if not self._is_runtime_trace_file(relative_path):
        return
    for line_no, raw_line in enumerate(lines, start=1):
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        edge_kind, target = self._runtime_trace_edge(payload)
        if not edge_kind or not target:
            continue
        evidence = self._runtime_trace_evidence(payload)
        add_reference(target, edge_kind, line_no, evidence)
        add_entity_edge(file_entity_id, edge_kind, target, line_no, evidence)


def _is_runtime_trace_file(relative_path: str) -> bool:
    path = Path(str(relative_path or ""))
    if path.suffix.lower() != ".jsonl":
        return False
    lowered_name = path.name.lower()
    lowered_parts = {part.lower() for part in path.parts}
    runtime_dirs = {"runtime-traces", "runtime_traces", "source-code-qa-traces", "source_code_qa_traces"}
    return lowered_name in RUNTIME_TRACE_FILENAMES or bool(runtime_dirs & lowered_parts)


def _runtime_trace_edge(self, payload: dict[str, Any]) -> tuple[str, str]:
    kind_text = self._runtime_trace_string(payload, ("kind", "type", "event", "span_kind")).lower()
    if any(token in kind_text for token in ("route", "http", "request", "endpoint")):
        return "runtime_route", self._runtime_trace_target(
            payload, ("route", "path", "url", "http_path", "endpoint", "target", "to", "handler")
        )
    if any(token in kind_text for token in ("sql", "db", "database", "table")):
        return "runtime_sql", self._runtime_trace_sql_target(payload)
    if any(token in kind_text for token in ("message", "kafka", "rabbit", "jms", "topic", "queue", "event")):
        return "runtime_message", self._runtime_trace_target(
            payload, ("topic", "queue", "channel", "message", "event_name", "target", "to")
        )
    if any(token in kind_text for token in ("config", "feature", "flag", "property")):
        return "runtime_config", self._runtime_trace_target(
            payload, ("key", "config", "property", "feature_flag", "flag", "name", "target", "to")
        )
    if any(token in kind_text for token in ("call", "method", "function", "span")):
        return "runtime_call", self._runtime_trace_target(
            payload, ("to", "target", "callee", "method", "function", "operation", "handler")
        )
    if self._runtime_trace_target(payload, ("route", "path", "url", "http_path", "endpoint")):
        return "runtime_route", self._runtime_trace_target(payload, ("route", "path", "url", "http_path", "endpoint"))
    if self._runtime_trace_target(payload, ("table", "sql", "statement", "query")):
        return "runtime_sql", self._runtime_trace_sql_target(payload)
    if self._runtime_trace_target(payload, ("topic", "queue", "channel")):
        return "runtime_message", self._runtime_trace_target(payload, ("topic", "queue", "channel"))
    if self._runtime_trace_target(payload, ("key", "config", "property", "feature_flag", "flag")):
        return "runtime_config", self._runtime_trace_target(
            payload, ("key", "config", "property", "feature_flag", "flag")
        )
    return "runtime_call", self._runtime_trace_target(payload, ("to", "target", "callee", "operation", "handler"))


def _runtime_trace_sql_target(self, payload: dict[str, Any]) -> str:
    table = self._runtime_trace_target(payload, ("table", "db_table", "entity"))
    if table:
        return table
    sql = self._runtime_trace_target(payload, ("sql", "statement", "query"))
    for pattern in (SQL_READ_TABLE_PATTERN, SQL_WRITE_TABLE_PATTERN, SQL_TABLE_PATTERN):
        match = pattern.search(sql)
        if match:
            return match.group(1)
    return sql[:160]


def _runtime_trace_target(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = SourceCodeQAService._runtime_trace_string(payload, (key,))
        if value:
            return value
    return ""


def _runtime_trace_string(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            normalized = str(value).strip()
        else:
            normalized = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if normalized:
            return normalized
    return ""


def _runtime_trace_evidence(payload: dict[str, Any]) -> str:
    source = SourceCodeQAService._runtime_trace_target(payload, ("from", "source", "caller", "handler", "span"))
    target = SourceCodeQAService._runtime_trace_target(
        payload, ("to", "target", "callee", "route", "path", "url", "table", "topic", "queue", "key")
    )
    evidence = SourceCodeQAService._runtime_trace_string(payload, ("evidence", "summary", "trace_id", "span_id"))
    parts = []
    if source:
        parts.append(f"from={source}")
    if target:
        parts.append(f"to={target}")
    if evidence:
        parts.append(evidence)
    if not parts:
        parts.append(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return " | ".join(parts)[:500]


def _extract_yaml_config_assignment(line: str, stack: list[tuple[int, str]]) -> tuple[str, str] | None:
    raw = str(line or "").rstrip()
    stripped = raw.strip()
    if not stripped or stripped.startswith(("#", "---", "...", "- ")):
        return None
    match = re.match(r"^(\s*)([A-Za-z0-9_.-]{2,})\s*:\s*(.*?)\s*$", raw)
    if not match:
        return None
    indent = len(match.group(1).replace("\t", "    "))
    key = match.group(2).strip()
    value = match.group(3).strip()
    while stack and stack[-1][0] >= indent:
        stack.pop()
    full_key = ".".join([item[1] for item in stack] + [key])
    if not value or value in {"|", ">"}:
        stack.append((indent, key))
        return full_key, ""
    value = re.sub(r"\s+#.*$", "", value).strip().strip("\"'")
    if not full_key:
        return None
    return full_key, value


def _spring_annotation_arg_values(annotation_args: str, key: str) -> list[str]:
    values: list[str] = []
    pattern = re.compile(
        rf"\b{re.escape(key)}\s*=\s*(\{{[^}}]*\}}|\"[^\"]*\"|'[^']*'|[A-Za-z0-9_.-]+)"
    )
    for match in pattern.finditer(str(annotation_args or "")):
        raw_value = match.group(1).strip()
        quoted_values = re.findall(r"\"([^\"]+)\"|'([^']+)'", raw_value)
        if quoted_values:
            values.extend(next((item for item in group if item), "") for group in quoted_values)
        else:
            values.extend(item.strip() for item in raw_value.strip("{}").split(","))
    return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))


def _spring_conditional_on_property_entries(annotation_args: str) -> list[str]:
    prefix_values = SourceCodeQAService._spring_annotation_arg_values(annotation_args, "prefix")
    prefix = prefix_values[0].strip(".") if prefix_values else ""
    property_names = (
        SourceCodeQAService._spring_annotation_arg_values(annotation_args, "name")
        or SourceCodeQAService._spring_annotation_arg_values(annotation_args, "value")
    )
    having_values = SourceCodeQAService._spring_annotation_arg_values(annotation_args, "havingValue")
    having_value = having_values[0] if having_values else "<present>"
    match_if_missing = any(
        value.lower() == "true"
        for value in SourceCodeQAService._spring_annotation_arg_values(annotation_args, "matchIfMissing")
    )
    conditions: list[str] = []
    for property_name in property_names:
        normalized_name = property_name.strip(".")
        if not normalized_name:
            continue
        full_key = (
            normalized_name
            if not prefix or normalized_name.startswith(f"{prefix}.")
            else f"{prefix}.{normalized_name}"
        )
        conditions.append(f"{full_key}={having_value}")
        if match_if_missing:
            conditions.append(f"{full_key}=<missing:true>")
    return list(dict.fromkeys(conditions))


def _annotation_target_text(annotation_args: str) -> str:
    text = str(annotation_args or "").strip()
    quoted_values = re.findall(r"\"([^\"]+)\"|'([^']+)'", text)
    values = [next((item for item in group if item), "") for group in quoted_values]
    values = [value.strip() for value in values if value and value.strip()]
    if values:
        return values[0]
    cleaned = re.sub(r"^\s*(?:value|pointcut)\s*=\s*", "", text).strip()
    return cleaned[:200]


def _scheduled_target_text(annotation_args: str) -> str:
    text = str(annotation_args or "").strip()
    if not text:
        return "scheduled"
    entries: list[str] = []
    for key in ("cron", "fixedRateString", "fixedDelayString", "initialDelayString"):
        for value in SourceCodeQAService._spring_annotation_arg_values(text, key):
            entries.append(f"{key}={value}")
    for key in ("fixedRate", "fixedDelay", "initialDelay"):
        match = re.search(rf"\b{re.escape(key)}\s*=\s*([0-9]+)", text)
        if match:
            entries.append(f"{key}={match.group(1)}")
    return ";".join(entries[:4]) or SourceCodeQAService._annotation_target_text(text) or "scheduled"


def _extract_message_names(argument_text: str) -> list[str]:
    names: list[str] = []
    text = str(argument_text or "")
    for value in re.findall(r"[\"']([^\"']{3,120})[\"']", text):
        lowered = value.lower()
        if any(marker in lowered for marker in ("topic", "queue", "exchange", "event", "issue", "command", ".", "-", "_")):
            names.append(value)
    for value in re.findall(r"\$\{([^}:]+)(?::[^}]*)?\}", text):
        names.append(value)
    return list(dict.fromkeys(name.strip() for name in names if name.strip()))[:8]


def _extract_event_names(argument_text: str) -> list[str]:
    text = str(argument_text or "")
    names = []
    for value in re.findall(r"\bnew\s+([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\b", text):
        names.append(value)
    for value in re.findall(r"\b([A-Z][A-Za-z0-9_]*(?:Event|Message|Command))\.class\b", text):
        names.append(value)
    for value in re.findall(r"[\"']([^\"']*(?:event|message|command)[^\"']*)[\"']", text, re.IGNORECASE):
        names.append(value)
    return list(dict.fromkeys(name.strip() for name in names if name.strip()))[:8]


def _extract_build_file_structure(
    self,
    *,
    relative_path: str,
    lines: list[str],
    add_definition: Any,
    add_reference: Any,
    add_entity_edge: Any,
    file_entity_id: str,
) -> None:
    lowered_path = str(relative_path or "").lower()
    filename = Path(relative_path).name.lower()
    if filename == "package.json":
        try:
            payload = json.loads("\n".join(lines))
        except json.JSONDecodeError:
            payload = {}
        package_name = str(payload.get("name") or "").strip() if isinstance(payload, dict) else ""
        if package_name:
            line_no = self._first_line_number_containing(lines, package_name)
            add_definition(package_name, "npm_package", line_no, package_name)
            add_entity_edge(file_entity_id, "module_artifact", package_name, line_no, package_name)
        for section in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
            dependencies = payload.get(section) if isinstance(payload, dict) else {}
            if not isinstance(dependencies, dict):
                continue
            for dependency_name in dependencies:
                dependency = str(dependency_name or "").strip()
                if not dependency:
                    continue
                line_no = self._first_line_number_containing(lines, dependency)
                add_reference(dependency, "module_dependency", line_no, dependency)
                add_entity_edge(file_entity_id, "module_dependency", dependency, line_no, dependency)
        return
    if filename == "pom.xml":
        full_text = "\n".join(lines)
        project_header = full_text.split("<dependencies>", 1)[0]
        project_tags = dict(MAVEN_TAG_PATTERN.findall(project_header))
        project_group = str(project_tags.get("groupId") or "").strip()
        project_artifact = str(project_tags.get("artifactId") or "").strip()
        if project_artifact:
            line_no = self._first_line_number_containing(lines, project_artifact)
            add_definition(project_artifact, "maven_artifact", line_no, project_artifact)
            add_entity_edge(file_entity_id, "module_artifact", project_artifact, line_no, project_artifact)
            if project_group:
                coordinate = f"{project_group}:{project_artifact}"
                add_definition(coordinate, "maven_coordinate", line_no, coordinate)
                add_entity_edge(file_entity_id, "module_artifact", coordinate, line_no, coordinate)
        for block in MAVEN_DEPENDENCY_BLOCK_PATTERN.findall(full_text):
            tags = dict(MAVEN_TAG_PATTERN.findall(block))
            group_id = str(tags.get("groupId") or "").strip()
            artifact_id = str(tags.get("artifactId") or "").strip()
            if not artifact_id or "$" in artifact_id:
                continue
            coordinate = f"{group_id}:{artifact_id}" if group_id and "$" not in group_id else artifact_id
            line_no = self._first_line_number_containing(lines, artifact_id)
            add_reference(coordinate, "module_dependency", line_no, block)
            add_entity_edge(file_entity_id, "module_dependency", coordinate, line_no, block)
            if coordinate != artifact_id:
                add_reference(artifact_id, "module_dependency", line_no, block)
                add_entity_edge(file_entity_id, "module_dependency", artifact_id, line_no, block)
        return
    if filename in {"build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"} or lowered_path.endswith(".gradle"):
        for line_no, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith(("//", "#")):
                continue
            for group_id, artifact_id in GRADLE_COORDINATE_PATTERN.findall(stripped):
                coordinate = f"{group_id}:{artifact_id}"
                add_reference(coordinate, "module_dependency", line_no, stripped)
                add_entity_edge(file_entity_id, "module_dependency", coordinate, line_no, stripped)
                add_reference(artifact_id, "module_dependency", line_no, stripped)
                add_entity_edge(file_entity_id, "module_dependency", artifact_id, line_no, stripped)
            for module_name in GRADLE_PROJECT_DEPENDENCY_PATTERN.findall(stripped):
                raw_module = module_name.strip()
                normalized_module = self._normalize_gradle_module_name(raw_module)
                if normalized_module:
                    add_reference(normalized_module, "module_dependency", line_no, stripped)
                    add_entity_edge(file_entity_id, "module_dependency", normalized_module, line_no, stripped)
                    add_reference(normalized_module, "gradle_project_dependency", line_no, stripped)
                    add_entity_edge(file_entity_id, "gradle_project_dependency", normalized_module, line_no, stripped)
                    if raw_module and raw_module != normalized_module:
                        add_reference(raw_module, "gradle_project_dependency", line_no, stripped)
                        add_entity_edge(file_entity_id, "gradle_project_dependency", raw_module, line_no, stripped)
            include_match = GRADLE_INCLUDE_PATTERN.search(stripped)
            if include_match:
                for module_name in re.findall(r"[\"']:([^\"']+)[\"']", include_match.group(1)):
                    raw_module = f":{module_name.strip().strip(':')}"
                    normalized_module = self._normalize_gradle_module_name(raw_module)
                    if normalized_module:
                        add_definition(normalized_module, "gradle_module", line_no, stripped)
                        add_entity_edge(file_entity_id, "module_artifact", normalized_module, line_no, stripped)
                        add_entity_edge(file_entity_id, "gradle_module", normalized_module, line_no, stripped)
                        add_definition(raw_module, "gradle_module", line_no, stripped)
                        add_entity_edge(file_entity_id, "gradle_module", raw_module, line_no, stripped)


def _normalize_gradle_module_name(module_name: str) -> str:
    return str(module_name or "").strip().strip(":").replace(":", "-")


def _first_line_number_containing(lines: list[str], needle: str) -> int:
    value = str(needle or "")
    if value:
        for index, line in enumerate(lines, start=1):
            if value in line:
                return index
    return 1


def _extract_python_ast_structure(
    self,
    *,
    relative_path: str,
    lines: list[str],
    add_definition,
    add_reference,
    add_entity,
    add_entity_edge,
    file_entity_id: str,
) -> None:
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError:
        return

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.stack: list[tuple[str, str]] = []

        def _current_entity(self) -> str:
            return self.stack[-1][1] if self.stack else file_entity_id

        def visit_ClassDef(self, node: ast.ClassDef) -> Any:
            signature = lines[node.lineno - 1].strip() if 0 < node.lineno <= len(lines) else node.name
            add_definition(node.name, "python_class", node.lineno, signature)
            entity_id = SourceCodeQAService._entity_id(relative_path, "python_class", node.name, node.lineno)
            self.stack.append((node.name, entity_id))
            self.generic_visit(node)
            self.stack.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
            self._visit_function(node, "python_function")

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
            self._visit_function(node, "python_async_function")

        def _visit_function(self, node: ast.AST, kind: str) -> None:
            name = getattr(node, "name", "")
            line_no = int(getattr(node, "lineno", 1) or 1)
            signature = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else name
            parent = self.stack[-1][0] if self.stack else ""
            add_definition(name, kind, line_no, signature)
            entity_id = add_entity(name, kind, line_no, signature, parent=parent)
            self.stack.append((name, entity_id))
            self.generic_visit(node)
            self.stack.pop()

        def visit_Import(self, node: ast.Import) -> Any:
            line_no = int(getattr(node, "lineno", 1) or 1)
            evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "import"
            for alias in node.names:
                add_reference(alias.name, "import", line_no, evidence)
                add_entity_edge(self._current_entity(), "import", alias.name, line_no, evidence)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
            line_no = int(getattr(node, "lineno", 1) or 1)
            evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "import"
            module = str(node.module or "")
            for alias in node.names:
                target = f"{module}.{alias.name}" if module else alias.name
                add_reference(target, "import", line_no, evidence)
                add_entity_edge(self._current_entity(), "import", target, line_no, evidence)

        def visit_Call(self, node: ast.Call) -> Any:
            line_no = int(getattr(node, "lineno", 1) or 1)
            evidence = lines[line_no - 1].strip() if 0 < line_no <= len(lines) else "call"
            target = self._call_name(node.func)
            if target:
                add_reference(target, "call", line_no, evidence)
                add_entity_edge(self._current_entity(), "call", target, line_no, evidence)
            self.generic_visit(node)

        def _call_name(node: ast.AST) -> str:
            if isinstance(node, ast.Name):
                return node.id
            if isinstance(node, ast.Attribute):
                parts = [node.attr]
                value = node.value
                while isinstance(value, ast.Attribute):
                    parts.append(value.attr)
                    value = value.value
                if isinstance(value, ast.Name):
                    parts.append(value.id)
                return ".".join(reversed(parts))
            return ""

        _call_name = staticmethod(_call_name)

    Visitor().visit(tree)


def _extract_data_flow_targets(line: str) -> list[str]:
    stripped = str(line or "").strip()
    if not stripped or stripped.startswith(("import ", "package ", "//", "*")):
        return []
    targets: list[str] = []

    def add_tokens(value: str) -> None:
        for token in IDENTIFIER_PATTERN.findall(str(value or "")):
            lowered = token.lower()
            if lowered in STOPWORDS or lowered in LOW_VALUE_CALL_SYMBOLS:
                continue
            if len(lowered) < 4:
                continue
            targets.append(token)

    for match in SETTER_CALL_PATTERN.finditer(stripped):
        field_name = match.group(1)
        argument = match.group(2)
        targets.append(f"set{field_name}")
        add_tokens(argument)
    for match in BUILDER_FIELD_PATTERN.finditer(stripped):
        field_name = match.group(1)
        argument = match.group(2)
        if field_name.lower() not in LOW_VALUE_CALL_SYMBOLS:
            targets.append(field_name)
            add_tokens(argument)
    assignment = ASSIGNMENT_PATTERN.search(stripped)
    if assignment and "==" not in stripped:
        add_tokens(assignment.group(1))
        add_tokens(assignment.group(2))
    return list(dict.fromkeys(targets))[:12]


_CLASS_METHODS = {
    "_first_named_child_text",
    "_tree_sitter_call_target",
    "_tree_sitter_name_for_node",
    "_tree_sitter_string_values",
    "_tree_sitter_type_text_for_node",
}

_STATIC_METHODS = {
    "_annotation_target_text",
    "_dedupe_structure_rows",
    "_extract_config_assignment",
    "_extract_data_flow_targets",
    "_extract_event_names",
    "_extract_message_names",
    "_extract_yaml_config_assignment",
    "_first_line_number_containing",
    "_is_runtime_trace_file",
    "_language_for_suffix",
    "_node_line",
    "_node_start_line",
    "_node_text",
    "_normalize_gradle_module_name",
    "_runtime_trace_evidence",
    "_runtime_trace_string",
    "_runtime_trace_target",
    "_scheduled_target_text",
    "_spring_annotation_arg_values",
    "_spring_conditional_on_property_entries",
    "_tree_sitter_language_for_suffix",
}

def attach_structure_helpers(cls: type, global_context: dict[str, object]) -> None:
    helpers = {
        "_tree_sitter_parser_for_language": _tree_sitter_parser_for_language,
        "_tree_sitter_language_for_suffix": _tree_sitter_language_for_suffix,
        "_node_text": _node_text,
        "_node_line": _node_line,
        "_node_start_line": _node_start_line,
        "_first_named_child_text": _first_named_child_text,
        "_tree_sitter_name_for_node": _tree_sitter_name_for_node,
        "_tree_sitter_call_target": _tree_sitter_call_target,
        "_tree_sitter_type_text_for_node": _tree_sitter_type_text_for_node,
        "_tree_sitter_string_values": _tree_sitter_string_values,
        "_extract_tree_sitter_structure": _extract_tree_sitter_structure,
        "_extract_structure_rows": _extract_structure_rows,
        "_dedupe_structure_rows": _dedupe_structure_rows,
        "_extract_sql_http_config_structure": _extract_sql_http_config_structure,
        "_extract_message_event_structure": _extract_message_event_structure,
        "_extract_test_structure_references": _extract_test_structure_references,
        "_language_for_suffix": _language_for_suffix,
        "_extract_config_assignment": _extract_config_assignment,
        "_extract_runtime_trace_structure": _extract_runtime_trace_structure,
        "_is_runtime_trace_file": _is_runtime_trace_file,
        "_runtime_trace_edge": _runtime_trace_edge,
        "_runtime_trace_sql_target": _runtime_trace_sql_target,
        "_runtime_trace_target": _runtime_trace_target,
        "_runtime_trace_string": _runtime_trace_string,
        "_runtime_trace_evidence": _runtime_trace_evidence,
        "_extract_yaml_config_assignment": _extract_yaml_config_assignment,
        "_spring_annotation_arg_values": _spring_annotation_arg_values,
        "_spring_conditional_on_property_entries": _spring_conditional_on_property_entries,
        "_annotation_target_text": _annotation_target_text,
        "_scheduled_target_text": _scheduled_target_text,
        "_extract_message_names": _extract_message_names,
        "_extract_event_names": _extract_event_names,
        "_extract_build_file_structure": _extract_build_file_structure,
        "_normalize_gradle_module_name": _normalize_gradle_module_name,
        "_first_line_number_containing": _first_line_number_containing,
        "_extract_python_ast_structure": _extract_python_ast_structure,
        "_extract_data_flow_targets": _extract_data_flow_targets,
    }
    _bind_source_code_qa_globals(list(helpers.values()), global_context)
    for name, helper in helpers.items():
        setattr(cls, name, classmethod(helper) if name in _CLASS_METHODS else staticmethod(helper) if name in _STATIC_METHODS else helper)
