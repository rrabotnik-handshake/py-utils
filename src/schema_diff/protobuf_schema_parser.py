"""
Protobuf schema parser → internal “type tree”.

Entry points
-----------
- list_protobuf_messages(path) -> list[str]
    Return fully-qualified names (FQNs) of all messages defined in a .proto file.

- schema_from_protobuf_file(path, message=None)
    Parse a .proto file and build a JSON-like *type tree* for a chosen message.
    Returns (tree, required_paths, chosen_fqn):
      - tree: Dict[str, Any] where leaves are 'int'|'float'|'bool'|'str'|'date'|'time'|'timestamp'|'object'
              and repeated fields are encoded as [elem_type].
      - required_paths: Set[str] of dotted field paths that are presence-required (proto2 'required' or explicit labels).
      - chosen_fqn: Fully-qualified name of the resolved message.

Notes
-----
- Enums are represented as 'str' (their JSON mapping).
- Maps are represented as 'object'.
- Nested messages are inlined structurally (fields expanded).
- Message references are resolved using lexical scope, package, and absolute FQNs.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
import re

__all__ = ["list_protobuf_messages", "schema_from_protobuf_file"]

# Scalar + well-known type mappings to internal labels
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
    "bytes": "str",  # JSON mapping is base64 string; treat as plain string
}

_WELL_KNOWN = {
    "google.protobuf.Timestamp": "timestamp",
    "google.type.Date": "date",
    "google.type.TimeOfDay": "time",
}

# Field matcher: labels + scalar/qualified types + maps
_FIELD_RE = re.compile(
    r"""
    ^\s*
    (?:(required|optional|repeated)\s+)?          # label (proto2/oneof context tolerated)
    (?:
      map<\s*(?P<map_k>[^,\s>]+)\s*,\s*(?P<map_v>[^>]+)\s*>\s+(?P<map_name>[A-Za-z_]\w*)
      |
      (?P<type>\.?[A-Za-z_][\w\.<>]*)\s+(?P<name>[A-Za-z_]\w*)
    )
    \s*=\s*\d+\s*(?:\[.*\])?\s*;                  # tag [options]
    """,
    re.VERBOSE,
)

# Single-line statements
PACKAGE_LINE_RE = re.compile(
    r'^\s*package\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*;\s*$'
)
IMPORT_LINE_RE = re.compile(
    r'^\s*import\s+(?:public\s+|weak\s+)?["\']([^"\']+)["\']\s*;\s*$'
)

# Block open/close
MESSAGE_OPEN_RE = re.compile(r'^\s*message\s+([A-Za-z_]\w*)\s*\{\s*$')
ENUM_OPEN_RE = re.compile(r'^\s*enum\s+([A-Za-z_]\w*)\s*\{\s*$')
ONEOF_OPEN_RE = re.compile(r'^\s*oneof\s+([A-Za-z_]\w*)\s*\{\s*$')
BLOCK_CLOSE_RE = re.compile(r'^\s*\}\s*$')

# Comments


def _strip_comments(text: str) -> str:
    """Remove /* ... */ block comments and // line comments."""
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"//.*?$", "", text, flags=re.M)
    return text


def _map_type(t: str) -> str:
    """
    Map a protobuf type token to our internal scalar/wkt label.
    Unknown/complex types are treated as 'object'.
    """
    t = t.strip()
    if t in _WELL_KNOWN:
        return _WELL_KNOWN[t]
    # Drop any generic angle brackets (defensive; normally not used on message tokens)
    t = re.sub(r"<.*>", "", t).strip()
    if t in _SCALARS:
        return _SCALARS[t]
    return "object"


def _resolve_ref(
    type_token: str,
    field_scope_fqn: str,
    package: Optional[str],
    known_msgs: Set[str],
) -> Optional[str]:
    """
    Resolve a message/enum type token to a known message/enum FQN.

    Resolution order:
      1) Absolute (leading '.') → exact match
      2) Lexical scope: scope, parent scope, ...
      3) Package-qualified
      4) Bare type (already FQN)
    """
    t = type_token.strip()
    if t.startswith("."):
        cand = t[1:]
        return cand if cand in known_msgs else None

    # Scope walk: current -> parent -> ...
    parts = field_scope_fqn.split(".")
    for i in range(len(parts), 0, -1):
        cand = ".".join(parts[:i] + [t])
        if cand in known_msgs:
            return cand

    if package:
        cand = f"{package}.{t}"
        if cand in known_msgs:
            return cand

    return t if t in known_msgs else None


def list_protobuf_messages(path: str) -> List[str]:
    """
    Return fully-qualified names of all message definitions in a .proto file.
    Enums and oneofs are ignored; only messages are returned.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    text = _strip_comments(raw)
    lines: List[str] = [ln.rstrip("\n") for ln in text.splitlines()]

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

        mm = MESSAGE_OPEN_RE.match(ln)
        if mm:
            stack.append(mm.group(1))
            out.append(fq_from_stack())
            continue

        me = ENUM_OPEN_RE.match(ln)
        mo = ONEOF_OPEN_RE.match(ln)
        if me or mo:
            # track nesting so subsequent closes pop correctly
            stack.append("(enum)" if me else f"oneof:{mo.group(1)}")
            continue

        if BLOCK_CLOSE_RE.match(ln):
            if stack:
                stack.pop()
            continue

    # (enum/oneof) frames never went to `out`, this is mostly belt-and-suspenders
    return [m for m in out if "(enum)" not in m]


def _build_message_tree(
    msg_fqn: str,
    msgs: Dict[str, Dict[str, Any]],
    enums: Set[str],
    package: Optional[str],
    children: Dict[str, List[str]],
) -> Dict[str, Any]:
    """
    Recursively inline a message definition into a JSON-like type tree.
    - message fields → nested dict
    - enum fields   → 'str'
    - map fields    → 'object'
    - repeated(*)   → [elem_type]
    """
    fields = msgs.get(msg_fqn, {})
    out: Dict[str, Any] = {}
    known_msgs = set(msgs.keys())

    for fname, finfo in fields.items():
        fkind = finfo["kind"]
        repeated = finfo.get("repeated", False)

        if fkind == "map":
            t: Any = "object"
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
                    t = "object"       # external/unknown type
            else:
                t = "object"
        else:
            t = _map_type(finfo["type"])

        if repeated:
            t = [t]
        out[fname] = t

    # Expose nested message *definitions* as properties (when not referenced by name)
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
    """
    Lightweight parser for .proto structure.
    Produces:
      - msgs: fields per message FQN (with coarse kind: scalar|enum|message|map)
      - enums: enum FQNs
      - top_order: top-level message FQNs in appearance order
      - package: declared package (or None)
      - children: nesting tree for messages (to expose nested defs)
    """
    text = _strip_comments(text)
    lines = [ln for ln in text.splitlines() if ln.strip()]

    package: Optional[str] = None
    msgs: Dict[str, Dict[str, Any]] = {}
    enums: Set[str] = set()
    top_order: List[str] = []
    children: Dict[str, List[str]] = {}

    # Stack of ('message'|'enum'|'oneof', fqn_or_name)
    stack: List[Tuple[str, str]] = []
    cur_msg_fqn: Optional[str] = None

    def join_nested(parent_fqn: Optional[str], name: str) -> str:
        # parent_fqn is already FQN; only add package at top-level
        if parent_fqn:
            return f"{parent_fqn}.{name}"
        return f"{package}.{name}" if package else name

    # maintain a live set of enum short names for quick checks
    enum_short_names: Set[str] = set()

    def _is_enum_token(t: str) -> bool:
        """Return True if type token refers to a known enum (FQN, absolute, or suffix)."""
        if t in enums or (t.startswith(".") and t[1:] in enums):
            return True
        last = t.rsplit(".", 1)[-1]
        return last in enum_short_names

    for ln in lines:
        # package / import
        m_pkg = PACKAGE_LINE_RE.match(ln)
        if m_pkg:
            package = m_pkg.group(1)
            continue
        if IMPORT_LINE_RE.match(ln):
            continue

        # opens
        mm = MESSAGE_OPEN_RE.match(ln)
        if mm:
            name = mm.group(1)
            fqn = join_nested(cur_msg_fqn, name)
            stack.append(("message", fqn))
            cur_msg_fqn = fqn
            msgs.setdefault(fqn, {})
            if len([k for k, _ in stack if k == "message"]) == 1:
                top_order.append(fqn)
            if "." in fqn:  # register nesting
                parent = fqn.rsplit(".", 1)[0]
                children.setdefault(parent, []).append(fqn)
            continue

        me = ENUM_OPEN_RE.match(ln)
        if me:
            name = me.group(1)
            fqn = join_nested(cur_msg_fqn, name)
            stack.append(("enum", fqn))
            enums.add(fqn)
            enum_short_names.add(name)  # keep short-name index live
            continue

        mo = ONEOF_OPEN_RE.match(ln)
        if mo:
            stack.append(("oneof", mo.group(1)))  # name unused downstream
            continue

        # closes
        if BLOCK_CLOSE_RE.match(ln):
            if stack:
                stack.pop()
            # update current enclosing message FQN
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

            label = fm.group(1)  # required|optional|repeated or None

            # map<k,v> name = N;
            map_name = fm.group("map_name")
            if map_name:
                msgs[cur_msg_fqn][map_name] = {
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

            if t in _SCALARS or t in _WELL_KNOWN:
                kind = "scalar"
            elif _is_enum_token(t):
                kind = "enum"
            else:
                kind = "message"

            msgs[cur_msg_fqn][name] = {
                "kind": kind,
                "type": t,                  # raw token; resolution happens later
                "repeated": (label == "repeated"),
                "required": (label == "required"),
                "scope": cur_msg_fqn,       # where this field was declared
            }

    return msgs, enums, top_order, package, children


def _collect_required_paths_proto(
    start_fqn: str,
    msgs: Dict[str, Dict[str, Any]],
    package: Optional[str],
    children: Dict[str, List[str]],
) -> Set[str]:
    """
    Collect dotted required paths by walking required fields recursively through
    referenced messages and nested definitions.
    """
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
        # (1) follow declared fields
        for fname, finfo in fields.items():
            path = f"{prefix}.{fname}" if prefix else fname
            if finfo.get("required"):
                required.add(path)
            if finfo["kind"] == "message":
                ref = _resolve_ref(finfo["type"], finfo.get(
                    "scope", msg_fqn), package, known_msgs)
                if ref:
                    walk(ref, path)
        # (2) traverse nested message definitions not referenced by a field
        for child_fqn in children.get(msg_fqn, []):
            if any(resolves_to(child_fqn, finfo, msg_fqn) for finfo in fields.values()):
                continue
            child_name = child_fqn.rsplit(".", 1)[-1]
            child_prefix = f"{prefix}.{child_name}" if prefix else child_name
            walk(child_fqn, child_prefix)

    walk(start_fqn)
    return required


def schema_from_protobuf_file(path: str, message: Optional[str] = None) -> Tuple[Dict[str, Any], Set[str], str]:
    """
    Parse a .proto file and produce:
      - tree: inlined type tree for the chosen message
      - required paths: dotted paths for required fields
      - chosen_fqn: fully-qualified name of the resolved message
    `message` may be:
      - None (first top-level message)
      - absolute FQN (e.g., "pkg.Outer.Inner")
      - unique suffix (e.g., "Outer.Inner")
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    msgs, enums, top, package, children = _parse_proto_structure(text)
    if not top:
        raise ValueError(f"No messages found in {path}")

    # Resolve desired message → FQN (absolute or unique suffix)
    def pick_fqn(name_or_suffix: Optional[str]) -> str:
        if name_or_suffix is None:
            return top[0]
        q = name_or_suffix.lstrip(".")
        # exact match
        if q in msgs:
            return q
        # unique suffix match
        cand = [k for k in msgs if k.endswith("." + q) or k == q]
        if not cand:
            raise ValueError(
                f"Message '{name_or_suffix}' not found in {path}. Available: {', '.join(top)}")
        if len(cand) > 1:
            raise ValueError(
                f"Ambiguous message suffix '{name_or_suffix}'. Options: {', '.join(cand)}")
        return cand[0]

    chosen_fqn = pick_fqn(message)

    tree = _build_message_tree(chosen_fqn, msgs, enums, package, children)
    required = _collect_required_paths_proto(
        chosen_fqn, msgs, package, children)

    return tree, required, chosen_fqn
