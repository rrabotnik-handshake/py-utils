from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
import re

__all__ = ["list_protobuf_messages", "schema_from_protobuf_file"]

_SCALARS = {
    "double": "float",
    "float": "float",
    "int32": "int",
    "int64": "int",
    "uint32": "int",
    "uint64": "int",
    "sint32": "int",
    "sint64": "int",
    "fixed32": "int",
    "fixed64": "int",
    "sfixed32": "int",
    "sfixed64": "int",
    "bool": "bool",
    "string": "str",
    "bytes": "str",  # JSON mapping is base64 string; keep it simple
}

_WELL_KNOWN = {
    "google.protobuf.Timestamp": "timestamp",
    "google.type.Date": "date",
    "google.type.TimeOfDay": "time",
}

_FIELD_RE = re.compile(
    r"""
    ^\s*
    (?:(required|optional|repeated)\s+)?          # label
    (?:
      map<\s*(?P<map_k>[^,\s>]+)\s*,\s*(?P<map_v>[^>]+)\s*>\s+(?P<map_name>[A-Za-z_]\w*)
      |
      (?P<type>\.?[A-Za-z_][\w\.<>]*)\s+(?P<name>[A-Za-z_]\w*)
    )
    \s*=\s*\d+\s*(?:\[.*\])?\s*;                  # tag [options]
    """,
    re.VERBOSE,
)

# single-line statements
PACKAGE_LINE_RE = re.compile(
    r'^\s*package\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*;\s*$'
)
IMPORT_LINE_RE = re.compile(
    r'^\s*import\s+(?:public\s+|weak\s+)?["\']([^"\']+)["\']\s*;\s*$'
)

# block open/close
MESSAGE_OPEN_RE = re.compile(r'^\s*message\s+([A-Za-z_]\w*)\s*\{\s*$')
ENUM_OPEN_RE = re.compile(r'^\s*enum\s+([A-Za-z_]\w*)\s*\{\s*$')
ONEOF_OPEN_RE = re.compile(r'^\s*oneof\s+([A-Za-z_]\w*)\s*\{\s*$')
BLOCK_CLOSE_RE = re.compile(r'^\s*\}\s*$')

# comments
LINE_COMMENT_RE = re.compile(r'^\s*//')
BLOCK_COMMENT_START_RE = re.compile(r'/\*')
BLOCK_COMMENT_END_RE = re.compile(r'\*/')


def _strip_inline_comments(line: str) -> str:
    # remove // comments
    if '//' in line:
        line = line.split('//', 1)[0]
    # crude block comment stripper on a single line
    line = re.sub(r'/\*.*?\*/', '', line)
    return line


def _resolve_ref(type_token: str, field_scope_fqn: str, package: Optional[str], known_msgs: Set[str]) -> Optional[str]:
    """
    Resolve a message/enum type token to a known message FQN.
    Rules:
      - leading '.' means absolute FQN (strip the dot)
      - otherwise try lexical scoping: scope, parent scope, ..., then package, then bare
    """
    t = type_token.strip()
    if t.startswith("."):
        cand = t[1:]
        return cand if cand in known_msgs else None

    # candidates from inner to outer scope
    parts = field_scope_fqn.split(".")
    for i in range(len(parts), 0, -1):
        cand = ".".join(parts[:i] + [t])
        if cand in known_msgs:
            return cand

    if package:
        cand = f"{package}.{t}"
        if cand in known_msgs:
            return cand

    # finally bare
    return t if t in known_msgs else None


def list_protobuf_messages(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        raw_lines = f.readlines()

    # strip comments
    lines: List[str] = []
    in_block = False
    for ln in raw_lines:
        if in_block:
            if BLOCK_COMMENT_END_RE.search(ln):
                in_block = False
            continue
        if BLOCK_COMMENT_START_RE.search(ln) and not BLOCK_COMMENT_END_RE.search(ln):
            in_block = True
            continue
        if LINE_COMMENT_RE.match(ln):
            continue
        # remove inline // and /* ... */ on a single line
        if '//' in ln:
            ln = ln.split('//', 1)[0]
        ln = re.sub(r'/\*.*?\*/', '', ln)
        lines.append(ln.rstrip('\n'))

    package: Optional[str] = None
    stack: List[str] = []
    out: List[str] = []

    def fq_from_stack() -> str:
        fq = ".".join(stack)
        return f"{package}.{fq}" if package else fq

    for ln in lines:
        if not ln.strip():
            continue

        m = PACKAGE_LINE_RE.match(ln)
        if m:
            package = m.group(1)
            continue

        if IMPORT_LINE_RE.match(ln):
            continue

        m = MESSAGE_OPEN_RE.match(ln)
        if m:
            stack.append(m.group(1))
            out.append(fq_from_stack())
            continue

        if ENUM_OPEN_RE.match(ln) or ONEOF_OPEN_RE.match(ln):
            # we don't list enums/oneofs as messages; but keep nesting correct
            stack.append("(enum)" if ENUM_OPEN_RE.match(
                ln) else f"oneof:{ONEOF_OPEN_RE.match(ln).group(1)}")
            continue

        if BLOCK_CLOSE_RE.match(ln):
            if stack:
                stack.pop()
            continue

    # filter out the pseudo "(enum)" / "oneof:..." frames if any leaked (shouldn't)
    return [m for m in out if "(enum)" not in m and not m.endswith(")")]



def _strip_comments(text: str) -> str:
    # Remove // line comments and /* ... */ block comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"//.*?$", "", text, flags=re.M)
    return text


def _map_type(t: str) -> Any:
    """Map protobuf type token to internal base type."""
    t = t.strip()
    # well-known fully qualified
    if t in _WELL_KNOWN:
        return _WELL_KNOWN[t]
    # generics in angle brackets (e.g., nested message types aren't generic; ignore <> if ever present)
    t = re.sub(r"<.*>", "", t).strip()
    # simple scalar?
    if t in _SCALARS:
        return _SCALARS[t]
    # If it's a qualified message/enum type, treat as object (expand when we inline message schema)
    return "object"


def _merge_object(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in src.items():
        if k not in dst:
            dst[k] = v
        elif isinstance(dst[k], dict) and isinstance(v, dict):
            _merge_object(dst[k], v)
        else:
            # last one wins (rare: duplicate field names across redefinitions)
            dst[k] = v
    return dst


def _build_message_tree(
    msg_fqn: str,
    msgs: Dict[str, Dict[str, Any]],
    enums: Set[str],
    package: Optional[str],
    children: Dict[str, List[str]],
) -> Dict[str, Any]:
    fields = msgs.get(msg_fqn, {})
    out: Dict[str, Any] = {}

    known_msgs = set(msgs.keys())

    for fname, finfo in fields.items():
        fkind = finfo["kind"]
        repeated = finfo.get("repeated", False)

        if fkind == "map":
            t = "object"
        elif fkind == "enum":
            t = "str"
        elif fkind == "message":
            raw = finfo["type"]
            scope = finfo.get("scope", msg_fqn)
            ref_fqn = _resolve_ref(raw, scope, package, known_msgs | enums)
            if ref_fqn:
                if ref_fqn in enums:
                    t = "str"          # enums → strings
                elif ref_fqn in msgs:
                    t = _build_message_tree(
                        ref_fqn, msgs, enums, package, children)
                else:
                    t = "object"       # external/unknown
            else:
                t = "object"
        else:
            t = _map_type(finfo["type"])

        if repeated:
            t = [t]
        out[fname] = t

    # also expose nested message *definitions* as properties
    for child_fqn in children.get(msg_fqn, []):
        child_name = child_fqn.rsplit(".", 1)[-1]
        out[child_name] = _build_message_tree(
            child_fqn, msgs, enums, package, children)

    return out


def _parse_proto_structure(text: str) -> Tuple[
    Dict[str, Dict[str, Any]],  # msgs: FQN -> { field_name -> info }
    Set[str],                   # enums: set of FQN enum names
    List[str],                  # top_order: list of top-level message FQNs
    Optional[str],              # package
    # children: parent message FQN -> [child message FQNs]
    Dict[str, List[str]],
]:
    text = _strip_comments(text)
    lines = [ln for ln in text.splitlines() if ln.strip()]

    package: Optional[str] = None
    msgs: Dict[str, Dict[str, Any]] = {}
    enums: Set[str] = set()
    top_order: List[str] = []
    children: Dict[str, List[str]] = {}

    # stack of message FQNs; for enums/oneofs we still track that we're inside the msg
    # ('message'|'enum'|'oneof', fqn_or_name)
    stack: List[Tuple[str, str]] = []
    cur_msg_fqn: Optional[str] = None


    def join_nested(parent_fqn: Optional[str], name: str) -> str:
        # parent_fqn is already fully-qualified; only add package at top-level
        if parent_fqn:
            return f"{parent_fqn}.{name}"
        return f"{package}.{name}" if package else name

    for ln in lines:
        # package / import
        m_pkg = PACKAGE_LINE_RE.match(ln)
        if m_pkg:
            package = m_pkg.group(1)
            continue
        if IMPORT_LINE_RE.match(ln):
            continue

        # opens
        m = MESSAGE_OPEN_RE.match(ln)
        if m:
            name = m.group(1)
            fqn = join_nested(cur_msg_fqn, name)
            stack.append(("message", fqn))
            cur_msg_fqn = fqn
            msgs.setdefault(fqn, {})
            if len([k for k, _ in stack if k == "message"]) == 1:
                top_order.append(fqn)
            # register nesting
            if "." in fqn:
                parent = fqn.rsplit(".", 1)[0]
                children.setdefault(parent, []).append(fqn)
            continue

        m = ENUM_OPEN_RE.match(ln)
        if m:
            name = m.group(1)
            fqn = join_nested(cur_msg_fqn, name)
            stack.append(("enum", fqn))
            enums.add(fqn)
            continue

        if ONEOF_OPEN_RE.match(ln):
            stack.append(("oneof", ""))  # name unused
            continue

        # closes
        if BLOCK_CLOSE_RE.match(ln):
            if stack:
                stack.pop()
            # update current enclosing message fqn
            cur_msg_fqn = None
            for k, v in reversed(stack):
                if k == "message":
                    cur_msg_fqn = v
                    break
            continue

        # fields (only when inside a message or its oneof)
        if cur_msg_fqn and stack and stack[-1][0] in ("message", "oneof"):
            fm = _FIELD_RE.match(ln)
            if not fm:
                continue

            label = fm.group(1)  # required/optional/repeated or None

            if fm.group("map_name"):
                # map<k,v> name = N;
                name = fm.group("map_name")
                msgs[cur_msg_fqn][name] = {
                    "kind": "map",
                    "key_type": fm.group("map_k"),
                    "value_type": fm.group("map_v"),
                    "repeated": False,
                    "required": (label == "required"),
                    "scope": cur_msg_fqn,
                }
                continue

            t = fm.group("type")
            name = fm.group("name")
            # classify roughly now; final resolution happens later in _build_message_tree
            kind = (
                "scalar" if (t in _SCALARS or t in _WELL_KNOWN)
                else "enum" 
                if any(t == e.split(".")[-1] or t == e or t.endswith("." + e.split(".")[-1]) for e in enums)
                else "message"
            )
            msgs[cur_msg_fqn][name] = {
                "kind": kind,
                "type": t,                  # keep raw token for resolution
                "repeated": (label == "repeated"),
                "required": (label == "required"),
                "scope": cur_msg_fqn,       # where this field was declared
            }

    return msgs, enums, top_order, package, children


def _choose_message_name(requested: Optional[str],
                         msgs: Dict[str, Dict[str, Any]],
                         top: List[str],
                         package: Optional[str]) -> str:
    if not requested:
        return top[0]

    # exact FQN
    if requested in msgs:
        return requested

    # dot-absolute ".pkg.Message"
    if requested.startswith('.') and requested[1:] in msgs:
        return requested[1:]

    # try package-qualified
    if package:
        cand = f"{package}.{requested}"
        if cand in msgs:
            return cand

    # unique suffix match (e.g. "Outer.Inner")
    matches = [m for m in msgs if m.endswith(
        f".{requested}") or m == requested]
    if len(matches) == 1:
        return matches[0]

    raise ValueError(
        f"Message '{requested}' not found. Available top-level: {', '.join(top)}"
    )
    

def _collect_required_paths_proto(
    start_fqn: str,
    msgs: Dict[str, Dict[str, Any]],
    package: Optional[str],
    children: Dict[str, List[str]],
) -> Set[str]:
    required: Set[str] = set()
    known_msgs = set(msgs.keys())

    def resolves_to(child_fqn: str, field_info: Dict[str, Any], scope_fqn: str) -> bool:
        if field_info.get("kind") != "message":
            return False
        ref = _resolve_ref(field_info["type"], field_info.get(
            "scope", scope_fqn), package, known_msgs)
        return ref == child_fqn

    def walk(msg_fqn: str, prefix: str = ""):
        fields = msgs.get(msg_fqn, {})
        # 1) field-based traversal (always)
        for fname, finfo in fields.items():
            path = f"{prefix}.{fname}" if prefix else fname
            if finfo.get("required"):
                required.add(path)
            if finfo["kind"] == "message":
                ref = _resolve_ref(
                    finfo["type"],
                    finfo.get("scope", msg_fqn),
                    package,
                    known_msgs,
                )
                if ref:
                    walk(ref, path)

        # 2) nested definition traversal — only for children not referenced by any field here
        for child_fqn in children.get(msg_fqn, []):
            # skip if any field resolves to this nested child type
            if any(resolves_to(child_fqn, finfo, msg_fqn) for finfo in fields.values()):
                continue
            child_name = child_fqn.rsplit(".", 1)[-1]
            child_prefix = f"{prefix}.{child_name}" if prefix else child_name
            walk(child_fqn, child_prefix)

    walk(start_fqn)
    return required


def schema_from_protobuf_file(path: str, message: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str], str]:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    msgs, enums, top, package, children = _parse_proto_structure(text)
    if not top:
        raise ValueError(f"No messages found in {path}")

    # Resolve desired message → FQN (support absolute and unique suffix)
    def pick_fqn(name_or_suffix: Optional[str]) -> str:
        known = list(msgs.keys())
        if name_or_suffix is None:
            return top[0]
        q = name_or_suffix.lstrip(".")
        # exact match
        for k in known:
            if k == q:
                return k
        # unique suffix match
        cand = [k for k in known if k.endswith("." + q) or k == q]
        if not cand:
            raise ValueError(
                f"Message '{name_or_suffix}' not found in {path}. Available: {', '.join(top)}")
        if len(cand) > 1:
            raise ValueError(
                f"Ambiguous message suffix '{name_or_suffix}'. Options: {', '.join(cand)}")
        return cand[0]

    chosen_fqn = pick_fqn(message)

    tree = _build_message_tree(chosen_fqn, msgs, enums, package, children)
    required = _collect_required_paths_proto(chosen_fqn, msgs, package, children)

    return tree, required, chosen_fqn
