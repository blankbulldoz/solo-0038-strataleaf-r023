import contextlib
from .codec import digest, field, loads, name as checked_name, scalar_token
from .cursors import decode_cursor, encode_cursor
from .errors import InvalidCursor, InvalidDocument, NotFound, SnapshotTooOld
from .mvcc import visible_rows


def _store_of(peer):
    """Accept a Database handle or a Storage handle; reject closed or foreign objects."""
    store = getattr(peer, "_store", None)
    if store is None and hasattr(peer, "connection") and hasattr(peer, "read") and hasattr(peer, "get"):
        store = peer
    if store is None:
        raise InvalidDocument("peer must be an open Database or Storage handle")
    if getattr(store, "closed", False):
        raise InvalidDocument("peer database handle is closed")
    return store


def _facts(store):
    return {"database_id": store.get("database_id"), "mode": store.get("mode"),
            "upstream": store.get("upstream"), "head": int(store.get("head")),
            "floor": int(store.get("floor")), "catalog_epoch": int(store.get("catalog_epoch"))}


def _relation(source, peer, assume_shared_history):
    """Classify how the two facts relate.

    A replica restored from a backup deliberately keeps its source's ``database_id``,
    so identity alone cannot distinguish a follower from a second handle on the same
    file. Role (``mode``/``upstream``) is the deciding fact; ``same_identity`` means the
    same role on the same lineage, including two replicas of one primary.
    """
    source_follows = source["mode"] == "replica" and source["upstream"] == peer["database_id"]
    peer_follows = peer["mode"] == "replica" and peer["upstream"] == source["database_id"]
    if peer_follows and not source_follows:
        return "peer_follows_source"
    if source_follows and not peer_follows:
        return "source_follows_peer"
    if source["database_id"] == peer["database_id"] and source["upstream"] == peer["upstream"]:
        return "same_identity"
    if assume_shared_history:
        return "assumed_shared"
    raise InvalidDocument("peer does not share this database's replication lineage; pass "
                          "assume_shared_history=True only when the two revisions are known to be comparable")


def _requested_collections(collections):
    if collections is None:
        return None
    if isinstance(collections, str):
        collections = [collections]
    if not isinstance(collections, (list, tuple)) or not collections:
        raise InvalidDocument("collections must be a name or a nonempty list of names")
    return sorted({checked_name(item) for item in collections})


def _validate_scope(start, end, limit, assume_shared_history, include_documents):
    for label, bound in (("start", start), ("end", end)):
        if bound is not None and (not isinstance(bound, str) or not bound
                                  or len(bound.encode()) > 512):
            raise InvalidDocument(f"{label} must be a nonempty document key or None")
    if start is not None and end is not None and start >= end:
        raise InvalidDocument("key interval requires start below end")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
        raise InvalidDocument("verification limit must be between 1 and 10000")
    if not isinstance(assume_shared_history, bool) or not isinstance(include_documents, bool):
        raise InvalidDocument("assume_shared_history and include_documents must be booleans")


def _key_page(connection, collection, after, start, end, count):
    statement = "SELECT DISTINCT key FROM versions WHERE collection=?"
    parameters = [collection]
    if after is not None:
        statement += " AND key>?"
        parameters.append(after)
    if start is not None:
        statement += " AND key>=?"
        parameters.append(start)
    if end is not None:
        statement += " AND key<?"
        parameters.append(end)
    statement += " ORDER BY key LIMIT ?"
    parameters.append(count)
    return [row[0] for row in connection.execute(statement, parameters)]


def _key_stream(connection, collection, after, start, end, chunk=256):
    cursor = after
    while True:
        rows = _key_page(connection, collection, cursor, start, end, chunk)
        if not rows:
            return
        for row in rows:
            yield row
        cursor = rows[-1]


def _merged_keys(source_connection, peer_connection, collection, after, start, end):
    """Merge both key streams in ascending order without materializing either side."""
    source_keys = _key_stream(source_connection, collection, after, start, end)
    peer_keys = _key_stream(peer_connection, collection, after, start, end)
    left, right = next(source_keys, None), next(peer_keys, None)
    while left is not None or right is not None:
        if right is None or (left is not None and left < right):
            yield left
            left = next(source_keys, None)
        elif left is None or right < left:
            yield right
            right = next(peer_keys, None)
        else:
            yield left
            left, right = next(source_keys, None), next(peer_keys, None)


def _scope_keys(source_connection, peer_connection, scope, after_collection, after_key, start, end):
    """Yield (collection, key) pairs, optionally resuming mid-collection.

    When the resume point's collection is no longer in scope, continue with the next
    collection in order rather than restarting from the beginning.
    """
    resuming = after_collection is not None
    for collection in scope:
        after = None
        if resuming:
            if collection != after_collection:
                if collection < after_collection:
                    continue
                resuming = False
            else:
                resuming, after = False, after_key
        for key in _merged_keys(source_connection, peer_connection, collection, after, start, end):
            yield collection, key


def _state(connection, collection, key, revision):
    """Exact visible state of a key at a revision already validated against the floor.

    ``verify_peer`` only compares at a revision at or above both retained floors, so a
    missing row always means the key had no version yet, never that history was lost.
    """
    row = connection.execute(
        "SELECT * FROM versions WHERE collection=? AND key=? AND rev<=? ORDER BY rev DESC LIMIT 1",
        (collection, key, revision)).fetchone()
    if row is None:
        return {"state": "absent", "revision": None}
    document = loads(row["body"]) if row["body"] is not None else None
    expires = None if row["expires"] is None else float(row["expires"])
    return {"state": "live" if document is not None else "deleted", "revision": row["rev"],
            "document": document, "expires_at": expires}


def _content_token(state):
    if state["state"] != "live":
        return None
    return digest({"document": state["document"], "expires_at": state["expires_at"]})


def _summary(state):
    item = {"state": state["state"], "revision": state["revision"]}
    if state["state"] == "live":
        item["content_sha256"] = _content_token(state)
        item["expires_at"] = state["expires_at"]
    return item


def _compare(collection, key, source_state, peer_state, include_documents):
    """Classify one key's outcome; return None when the two sides agree."""
    source_live = source_state["state"] == "live"
    peer_live = peer_state["state"] == "live"
    if source_live and peer_live:
        if _content_token(source_state) != _content_token(peer_state):
            item = {"collection": collection, "key": key, "kind": "content_mismatch",
                    "source": _summary(source_state), "peer": _summary(peer_state)}
            if include_documents:
                item["source_document"] = source_state["document"]
                item["peer_document"] = peer_state["document"]
            return item
        return None
    if source_live and not peer_live:
        return {"collection": collection, "key": key, "kind": "missing_on_peer",
                "source": _summary(source_state), "peer": _summary(peer_state)}
    if peer_live and not source_live:
        return {"collection": collection, "key": key, "kind": "extra_on_peer",
                "source": _summary(source_state), "peer": _summary(peer_state)}
    if source_state["state"] != peer_state["state"]:
        # Both sides agree nothing is live, but one keeps a tombstone where the other
        # has nothing. That diverges the precondition for a later conditional write.
        return {"collection": collection, "key": key, "kind": "deletion_state_mismatch",
                "source": _summary(source_state), "peer": _summary(peer_state)}
    return None


def _catalog(connection, names):
    wanted = set(names)
    collections = {row["name"]: {"schema": row["schema"], "generation": row["generation"]}
                   for row in connection.execute("SELECT name,schema,generation FROM collections ORDER BY name")
                   if row["name"] in wanted}
    indexes = [list(row) for row in connection.execute(
        "SELECT collection,name,field,unique_flag FROM index_defs ORDER BY collection,name")
        if row[0] in wanted]
    return {"collections": collections, "indexes": indexes}


def _catalog_differences(source, peer):
    source_names, peer_names = set(source["collections"]), set(peer["collections"])
    source_indexes = {tuple(item) for item in source["indexes"]}
    peer_indexes = {tuple(item) for item in peer["indexes"]}
    return {
        "missing_on_peer": sorted(source_names - peer_names),
        "extra_on_peer": sorted(peer_names - source_names),
        "changed": sorted(name for name in source_names & peer_names
                          if source["collections"][name] != peer["collections"][name]),
        "indexes_missing_on_peer": [list(item) for item in sorted(source_indexes - peer_indexes)],
        "indexes_extra_on_peer": [list(item) for item in sorted(peer_indexes - source_indexes)],
    }


def _outstanding_coverage(connection, from_revision, to_revision, keys, commit_limit=10000):
    """Report which divergent keys an outstanding replication range would overwrite.

    Catch-up replays commits in order, so a divergent key is repaired only when at
    least one outstanding commit writes it; the last such write decides the result.
    """
    if not keys or to_revision <= from_revision:
        return {"scanned": 0, "truncated": False, "keys": {}}
    rows = connection.execute("SELECT rev,payload FROM commits WHERE rev>? AND rev<=? ORDER BY rev",
                              (from_revision, to_revision)).fetchall()
    truncated = len(rows) > commit_limit
    coverage = {}
    for row in rows[:commit_limit]:
        for operation in loads(row["payload"])["operations"]:
            identity = (operation["collection"], operation["key"])
            if identity in keys:
                action = "delete" if operation["document"] is None else "put"
                coverage[identity] = {"revision": row["rev"], "action": action}
    return {"scanned": min(len(rows), commit_limit), "truncated": truncated, "keys": coverage}


def _repair_effect(identities, coverage):
    """Split differences into ones catch-up would overwrite and ones it would leave."""
    repaired, remaining = [], []
    for collection, key, kind in identities:
        identity = (collection, key)
        write = coverage["keys"].get(identity)
        if coverage["truncated"] or write is None:
            remaining.append({"collection": collection, "key": key, "kind": kind,
                              "why": "no outstanding commit rewrites this key" if write is None
                                     else "outstanding range too large to prove coverage"})
        else:
            repaired.append({"collection": collection, "key": key, "kind": kind,
                             "overwritten_by_revision": write["revision"],
                             "action": write["action"]})
    return repaired, remaining


def _catch_up_effect(upstream_connection, source, peer, relation, run):
    """Show what finishing catch-up would do to every difference the run has seen.

    The outstanding commits are read from whichever side is the upstream, because
    that is the side a follower replays from.
    """
    if relation == "peer_follows_source":
        upstream, downstream = source, peer
    elif relation == "source_follows_peer":
        upstream, downstream = peer, source
    else:
        return None
    identities = run["difference_identities"]
    keys = {(collection, key) for collection, key, _ in identities}
    if upstream["head"] <= downstream["head"]:
        return {"outstanding_revisions": None, "catch_up_revision": None,
                "repaired_by_catch_up": [],
                "kept_after_catch_up": [{"collection": collection, "key": key, "kind": kind}
                                        for collection, key, kind in identities],
                "resolves_differences": not keys and not run["difference_identities_truncated"]}
    coverage = _outstanding_coverage(upstream_connection, downstream["head"],
                                     upstream["head"], keys)
    repaired, remaining = _repair_effect(identities, coverage)
    # With the identity list capped, earlier differences are unknown; never claim the
    # outstanding range resolves a run whose divergence list was truncated.
    resolves = (not remaining and not coverage["truncated"]
                and not run["difference_identities_truncated"])
    return {"outstanding_revisions": {"from": downstream["head"] + 1, "to": upstream["head"]},
            "catch_up_revision": upstream["head"],
            "repaired_by_catch_up": repaired, "kept_after_catch_up": remaining,
            "resolves_differences": resolves,
            "coverage_complete": not run["difference_identities_truncated"]}


RUN_IDENTITY_LIMIT = 64


def _extend_run(carried, examined, differences):
    """Accumulate facts across pages so a verdict describes the whole run, not one page."""
    previous = carried or {}
    identities = list(previous.get("difference_identities", []))
    truncated = bool(previous.get("difference_identities_truncated"))
    for item in differences:
        identity = (item["collection"], item["key"], item["kind"])
        if identity in identities:
            continue
        if len(identities) >= RUN_IDENTITY_LIMIT:
            truncated = True
            continue
        identities.append(identity)
    return {"pages": int(previous.get("pages", 0)) + 1,
            "examined": int(previous.get("examined", 0)) + examined,
            "differences": int(previous.get("differences", 0)) + len(differences),
            "difference_identities": identities,
            "difference_identities_truncated": truncated}


def _carry_run(run):
    carried = {key: run[key] for key in ("pages", "examined", "differences",
                                         "difference_identities_truncated")}
    carried["difference_identities"] = [list(item) for item in run["difference_identities"]]
    return carried


def _carried_run(token):
    raw = token.get("run") or {}
    identities = [tuple(item) for item in raw.get("difference_identities", [])
                  if isinstance(item, (list, tuple)) and len(item) == 3]
    return {"pages": int(raw.get("pages", 0)), "examined": int(raw.get("examined", 0)),
            "differences": int(raw.get("differences", 0)),
            "difference_identities": identities,
            "difference_identities_truncated": bool(raw.get("difference_identities_truncated"))}


def _catch_up(source, peer, relation):
    """Relate the two sides to the replication range that would close any gap."""
    if relation == "peer_follows_source":
        follower, upstream, downstream = "peer", source, peer
    elif relation == "source_follows_peer":
        follower, upstream, downstream = "source", peer, source
    else:
        follower = None
    ahead = follower is not None and upstream["head"] > downstream["head"]
    behind = follower is not None and upstream["head"] < downstream["head"]
    servable = not ahead or upstream["floor"] <= downstream["head"]
    outstanding = ({"from": downstream["head"] + 1, "to": upstream["head"]}
                   if ahead else None)
    unservable = ({"from": downstream["head"] + 1, "to": upstream["floor"],
                   "reason": "upstream no longer retains the commits the follower is missing"}
                  if ahead and not servable else None)
    catalog_behind = follower is not None and upstream["catalog_epoch"] > downstream["catalog_epoch"]
    # A catalog-only advance still travels in a replication envelope (possibly with no
    # document commits), so it is catch-up-able without moving the follower head.
    can_catch_up = follower is not None and not behind and (
        (ahead and servable) or (not ahead and catalog_behind))
    return {
        "relation": relation,
        "follower": follower,
        "source": {"database_id": source["database_id"], "mode": source["mode"],
                   "head": source["head"], "floor": source["floor"],
                   "catalog_epoch": source["catalog_epoch"]},
        "peer": {"database_id": peer["database_id"], "mode": peer["mode"],
                 "upstream": peer["upstream"], "head": peer["head"],
                 "floor": peer["floor"], "catalog_epoch": peer["catalog_epoch"]},
        "peer_follows_source": follower == "peer",
        "follower_ahead_of_upstream": behind,
        "outstanding_revisions": outstanding,
        "unservable_gap": unservable,
        "catalog_behind": catalog_behind,
        "indexes_ready": follower is not None and not ahead and not catalog_behind,
        "can_catch_up": can_catch_up,
    }


def _decision(relation, has_differences, catalog_differs, catch_up, complete, comparable,
              catch_up_effect):
    if not complete:
        return {"action": "resume", "reason": "the requested key interval was not fully examined"}
    if not comparable:
        return {"action": "rebuild",
                "reason": "no revision is retained by both sides, so replication cannot bridge the gap"}
    if catch_up["follower_ahead_of_upstream"]:
        return {"action": "rebuild", "reason": "a follower head is ahead of its configured upstream"}
    if has_differences:
        if catch_up["can_catch_up"] and catch_up_effect and catch_up_effect["resolves_differences"]:
            return {"action": "catch_up",
                    "reason": "every divergent key is rewritten by the outstanding replication "
                              "range; finish catch-up and verify again"}
        if catch_up_effect and catch_up_effect["repaired_by_catch_up"]:
            reason = ("catch-up would repair only part of the divergence; keys outside the "
                      "outstanding range would stay wrong")
            if not catch_up_effect["coverage_complete"]:
                reason = ("too many divergent keys were observed to prove the outstanding range "
                          "overwrites all of them; verify in smaller key intervals")
            return {"action": "rebuild", "reason": reason}
        if catch_up["follower"] is not None:
            return {"action": "rebuild",
                    "reason": "the follower already differs at its own head; replaying later commits "
                              "cannot be shown to overwrite the divergent keys"}
        return {"action": "rebuild",
                "reason": "the peer does not follow this database, so replication cannot repair content"}
    if catalog_differs:
        if catch_up["can_catch_up"]:
            return {"action": "catch_up",
                    "reason": "documents agree; schema, index and remaining data differences travel "
                              "with the outstanding replication range"}
        return {"action": "rebuild",
                "reason": "schema or index definitions differ with no replication range available"}
    if catch_up["can_catch_up"]:
        return {"action": "catch_up",
                "reason": "comparable content agrees; the follower still has outstanding commits"}
    return {"action": "none", "reason": "comparable content and catalog definitions agree"}


def _remediation(catch_up, catch_up_effect):
    """Describe what each caller-side recovery route would actually achieve."""
    repaired = catch_up_effect["repaired_by_catch_up"] if catch_up_effect else []
    kept = catch_up_effect["kept_after_catch_up"] if catch_up_effect else []
    return {
        "catch_up": {
            "available": catch_up["can_catch_up"],
            "follower": catch_up["follower"],
            "requires": "a follower bound to its upstream with continuous retained history",
            "range": catch_up["outstanding_revisions"],
            "preserves_follower_identity": True,
            "repaired_keys": [{"collection": item["collection"], "key": item["key"]}
                              for item in repaired],
            "remaining_keys": [{"collection": item["collection"], "key": item["key"]}
                               for item in kept],
            "fully_resolves_observed_differences":
                bool(catch_up_effect) and catch_up_effect["resolves_differences"],
        },
        "rebuild": {
            "available": True,
            "requires": "a fresh backup plus Archives.restore(..., follower=True) "
                        "or a fresh logical import",
            "preserves_follower_identity": False,
            "guarantees_content_match_at_target_head": True,
        },
        "note": "matching revision heads never proves matching content; only a completed "
                "scoped comparison does",
    }


@contextlib.contextmanager
def _paired_read(source, peer):
    """Open both read views at once; a single handle is opened once.

    Reads do not exclude each other under WAL, so ordering below only keeps results
    deterministic. Both snapshots are established before either side is examined,
    which is what makes the negotiated revision comparable on each side.
    """
    if source is peer:
        with source.read() as connection:
            yield connection, connection
        return
    first, second = sorted((source, peer),
                           key=lambda store: (store.get("database_id"), store.get("mode"),
                                              id(store)))
    with first.read(), second.read():
        yield source.connection, peer.connection


class Diagnostics:
    def __init__(self, store):
        self.store = store

    def inspect(self, deep=False):
        with self.store.read() as connection:
            head, floor = int(self.store.get("head")), int(self.store.get("floor"))
            integrity = [row[0] for row in connection.execute("PRAGMA integrity_check" if deep else "PRAGMA quick_check")]
            references = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
            counts = {table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                      for table in ("collections", "versions", "commits", "index_defs", "index_values", "pins", "consumers")}
            revisions = [row[0] for row in connection.execute("SELECT rev FROM commits WHERE rev>? ORDER BY rev", (floor,))]
            issues = []
            if revisions != list(range(floor + 1, head + 1)):
                issues.append("retained commit range is not contiguous")
            if references:
                issues.append("broken foreign keys")
            if integrity != ["ok"]:
                issues.append("SQLite physical integrity failure")
            if not 0 <= floor <= head:
                issues.append("invalid revision watermarks")
            for pin in connection.execute("SELECT name,rev FROM pins"):
                if not floor <= pin["rev"] <= head:
                    issues.append("read pin outside retained history: " + pin["name"])
            if deep:
                expected = set()
                for definition in connection.execute("SELECT * FROM index_defs"):
                    tokens = set()
                    for row in visible_rows(connection, definition["collection"], head):
                        token = scalar_token(field(loads(row["body"]), definition["field"]))
                        if token is None:
                            continue
                        if definition["unique_flag"] and token in tokens:
                            issues.append("unique index has duplicate source values: " + definition["name"])
                        tokens.add(token)
                        expected.add((definition["collection"], definition["name"], token, row["key"]))
                actual = {tuple(row) for row in connection.execute("SELECT collection,name,token,key FROM index_values")}
                if expected != actual:
                    issues.append("index entries differ from current visible documents")
            return {"ok": not issues, "issues": issues, "head": head, "floor": floor,
                    "database_id": self.store.get("database_id"), "mode": self.store.get("mode"),
                    "counts": counts, "sqlite": integrity, "foreign_keys": references}

    def fingerprint(self, revision=None):
        with self.store.read() as connection:
            revision = self._checked_revision(revision)
            content = []
            for collection in connection.execute("SELECT name FROM collections ORDER BY name"):
                content.extend({"collection": collection[0], "key": row["key"], "revision": row["rev"],
                                "document": loads(row["body"]), "expires_at": row["expires"]}
                               for row in visible_rows(connection, collection[0], revision))
            return {"revision": revision, "documents": len(content), "sha256": digest(content)}

    def _checked_revision(self, revision):
        from .mvcc import checked_revision
        return checked_revision(self.store, revision)

    def verify_peer(self, peer, collections=None, start=None, end=None, revision=None,
                    assume_shared_history=False, limit=1000, position=None,
                    include_documents=True):
        """Compare document content with a peer over a collection and key interval.

        Both sides are read at one negotiated revision so either may keep committing.
        ``limit`` bounds the keys examined per call; resume by passing the returned
        ``continue_token`` as ``position`` to keep the same read revision and scope.
        """
        _validate_scope(start, end, limit, assume_shared_history, include_documents)
        peer_store = _store_of(peer)
        requested = _requested_collections(collections)
        with _paired_read(self.store, peer_store) as (source_connection, peer_connection):
            source_facts, peer_facts = _facts(self.store), _facts(peer_store)
            token = None
            if position is not None:
                token = decode_cursor(self.store, position)
                if token.get("kind") != "verify_peer" or token.get("peer") != peer_facts["database_id"]:
                    raise InvalidCursor("continuation token does not belong to this peer")
            relation = _relation(source_facts, peer_facts,
                                 assume_shared_history
                                 or (token is not None and token.get("relation") == "assumed_shared"))
            negotiated = revision is None
            after_collection = after_key = resumed_scope = token_peers = None
            if token is not None:
                if token.get("relation") != relation:
                    raise InvalidCursor("peer relationship changed since the verification started")
                if revision is not None and revision != token["revision"]:
                    raise InvalidCursor("continuation token has a different read revision")
                if requested is not None and requested != token["requested"]:
                    raise InvalidCursor("continuation token has a different collection scope")
                if start is not None and start != token["start"]:
                    raise InvalidCursor("continuation token has a different key interval")
                if end is not None and end != token["end"]:
                    raise InvalidCursor("continuation token has a different key interval")
                revision, requested = token["revision"], token["requested"]
                start, end, negotiated = token["start"], token["end"], token["negotiated"]
                after_collection, after_key = token["after_collection"], token["after_key"]
                resumed_scope = token["scope"]
                token_peers = tuple(token["peers"])
                if token_peers != (source_facts["database_id"], peer_facts["database_id"],
                                   source_facts["mode"], peer_facts["mode"]):
                    raise InvalidCursor("database lineage changed since the verification started")

            low = max(source_facts["floor"], peer_facts["floor"])
            high = min(source_facts["head"], peer_facts["head"])
            unverifiable = []
            comparable = low <= high
            if not comparable:
                if revision is not None:
                    raise SnapshotTooOld(
                        "requested revision is not retained on both sides; comparable range is empty "
                        f"(floors {source_facts['floor']}/{peer_facts['floor']}, "
                        f"heads {source_facts['head']}/{peer_facts['head']})")
                unverifiable.append({"database": None, "from": 0, "to": high,
                                     "reason": "no revision is retained by both sides",
                                     "source_floor": source_facts["floor"],
                                     "peer_floor": peer_facts["floor"]})
                after_collection = after_key = None
            else:
                if revision is None:
                    revision = high
                if revision > high:
                    raise SnapshotTooOld(
                        f"revision {revision} is beyond the comparable ceiling {high}")
                if revision < low:
                    raise SnapshotTooOld(
                        f"revision {revision} is older than the comparable floor {low}")
                for facts in (source_facts, peer_facts):
                    if facts["floor"] > 0:
                        unverifiable.append({
                            "database": facts["database_id"], "from": 0, "to": facts["floor"],
                            "reason": "history reclaimed before the comparable revision; "
                                      "earlier content on this side cannot be examined"})

            source_names = {row[0] for row in source_connection.execute("SELECT name FROM collections")}
            peer_names = {row[0] for row in peer_connection.execute("SELECT name FROM collections")}
            if resumed_scope is not None:
                # Pin the collection list for the life of the paginated comparison so a
                # collection created mid-verification cannot silently extend the scope.
                pinned = resumed_scope
                scope = [name for name in pinned if name in source_names or name in peer_names]
            elif requested is None:
                pinned = sorted(source_names | peer_names)
                scope = pinned
            else:
                unknown = [name for name in requested if name not in source_names and name not in peer_names]
                if unknown:
                    raise NotFound("unknown collection: " + unknown[0])
                pinned = scope = requested
            catalog = {"source": _catalog(source_connection, scope),
                       "peer": _catalog(peer_connection, scope)}
            catalog_differences = _catalog_differences(catalog["source"], catalog["peer"])
            catalog_differs = any(catalog_differences.values())

            differences, examined, last_key, has_more = [], 0, None, False
            if comparable:
                for collection, key in _scope_keys(source_connection, peer_connection, scope,
                                                   after_collection, after_key, start, end):
                    if examined >= limit:
                        has_more = True
                        break
                    examined += 1
                    last_key = (collection, key)
                    source_state = _state(source_connection, collection, key, revision)
                    peer_state = _state(peer_connection, collection, key, revision)
                    difference = _compare(collection, key, source_state, peer_state, include_documents)
                    if difference is not None:
                        differences.append(difference)

            carried = _carried_run(token) if token is not None else None
            run = _extend_run(carried, examined, differences)
            complete = not has_more
            catch_up = _catch_up(source_facts, peer_facts, relation)
            upstream_connection = source_connection if catch_up["follower"] == "peer" \
                else peer_connection if catch_up["follower"] == "source" else None
            # Catch-up coverage must consider every difference the run has seen, not just
            # the current page, or a later page could look fully repairable on its own.
            catch_up_effect = None
            if comparable and catch_up["follower"] is not None:
                catch_up_effect = _catch_up_effect(upstream_connection, source_facts, peer_facts,
                                                   relation, run)
            counts = {kind: sum(1 for item in differences if item["kind"] == kind)
                      for kind in ("missing_on_peer", "extra_on_peer", "content_mismatch",
                                   "deletion_state_mismatch")}
            verified = comparable and complete and run["differences"] == 0
            result = {
                "relation": relation,
                "revision": revision if comparable else None,
                "negotiated_revision": negotiated,
                "source_head": source_facts["head"],
                "peer_head": peer_facts["head"],
                "revision_heads_equal": source_facts["head"] == peer_facts["head"],
                "heads_equal_but_content_differs":
                    source_facts["head"] == peer_facts["head"] and run["differences"] > 0,
                "revision_identity_used_as_content_proof": False,
                "evidence": ("scoped_content_comparison" if comparable and complete
                             else "partial_scoped_content_comparison" if comparable
                             else "none"),
                "comparable": comparable,
                "verified_equal": verified,
                "content_equal": None if not comparable or not complete else run["differences"] == 0,
                "catalog_equal": not catalog_differs,
                "consistent": verified and not catalog_differs,
                "scope": {"collections": scope, "start": start, "end": end},
                "examined": examined,
                "run": {key: value for key, value in run.items()
                        if key != "difference_identities"},
                "complete": complete,
                "has_more": has_more,
                "differences": differences,
                "difference_counts": counts,
                "unverifiable_ranges": unverifiable,
                "retention": {
                    "source": {"database_id": source_facts["database_id"],
                               "floor": source_facts["floor"], "head": source_facts["head"]},
                    "peer": {"database_id": peer_facts["database_id"],
                             "floor": peer_facts["floor"], "head": peer_facts["head"]},
                    "comparable_revisions": {"from": low, "to": high} if comparable else None,
                },
                "catalog": catalog,
                "catalog_differences": catalog_differences,
                "catch_up": catch_up,
                "catch_up_effect": catch_up_effect,
                "remediation": _remediation(catch_up, catch_up_effect),
                "decision": _decision(relation, run["differences"] > 0, catalog_differs, catch_up,
                                      complete, comparable, catch_up_effect),
            }
            if has_more and last_key is not None:
                result["continue_token"] = encode_cursor(self.store, {
                    "kind": "verify_peer", "database": self.store.get("database_id"),
                    "peer": peer_facts["database_id"], "relation": relation,
                    "peers": list(token_peers or (source_facts["database_id"], peer_facts["database_id"],
                                                  source_facts["mode"], peer_facts["mode"])),
                    "revision": revision, "negotiated": negotiated, "scope": pinned,
                    "requested": requested, "start": start, "end": end,
                    "run": _carry_run(run),
                    "after_collection": last_key[0], "after_key": last_key[1]})
            else:
                result["continue_token"] = None
            return result

