#!/usr/bin/env python3
"""Converts one writer's traced S3 requests into a TLA+ trace of S3Fence events.

Input: a request log written with SEDIMENT_S3_METER_DIR and SEDIMENT_S3_TRACE=1 (T and A lines),
and the database prefix. Output: TraceLog.tla (the Trace sequence) and, on stderr, how
every request was classified. A request that fits no rule is an error: nothing is dropped
silently. Requests outside the model (the probe, lease renewals, reads that only feed a
decision the next event shows) are counted under "not in the model".

  s3trace.py <requests.log> <prefix> <out-dir>
"""
import re
import sys
import urllib.parse
from collections import Counter

LOG_KEY = re.compile(r"^log/(\d{20})-(\d{10})/(\d{20})$")
SNAP_KEY = re.compile(r"^snapshots/(\d{20})-(\d{10})\.(db|delta)$")


GENERATIONS = {}
OPENS = [0]


def epoch(seq, gen):
    """Epoch (seq, generation) of a key the current incarnation writes. Generations are
    renumbered by incarnation: the k-th successful open is the model's generation k. The
    real counter can skip (an acquisition that landed without an answer), and an open
    whose takeover fails writes no key at all."""
    g = GENERATIONS.setdefault(int(gen), OPENS[0])
    return (int(seq), g)


def lines_of(path):
    """One log's lines, or, for a directory of per-process logs (writers that followed one
    another, e.g. after a kill -9), all their lines ordered by time, each process's own
    order kept. Ids are made unique per process."""
    import glob
    import os
    if not os.path.isdir(path):
        return [line.split() for line in open(path)]
    merged = []
    for n, log in enumerate(sorted(glob.glob(os.path.join(path, "requests-*.log")))):
        for k, line in enumerate(open(log)):
            parts = line.split()
            if len(parts) > 2 and parts[0] in ("T", "A"):
                parts[2] = f"{n}.{parts[2]}"
                merged.append((int(parts[1]), n, k, parts))
    merged.sort(key=lambda m: (m[0], m[1], m[2]))
    return [m[3] for m in merged]


def parse(path, prefix):
    """(id, op, key, cond, status, etag) in request order; key relative to the prefix."""
    reqs, answers = {}, {}
    order = []
    for parts in lines_of(path):
        if not parts:
            continue
        if parts[0] == "T":
            _, _ms, rid, op, target, cond = parts[:6]
            tag = parts[6] if len(parts) > 6 else "-"
            reqs[rid] = (op, target, cond, tag)
            order.append(rid)
        elif parts[0] == "A":
            _, _ms, rid, status, etag = parts
            answers[rid] = (status, etag)
    out = []
    for rid in order:
        op, target, cond, tag = reqs[rid]
        if tag == "seal":
            op = "PutSeal"  # a create-only PUT of a log key, tagged by seal_epoch
        status, etag = answers.get(rid, ("none", "-"))
        path_part, _, query = target.partition("?")
        q = urllib.parse.parse_qs(query)
        if op == "ListObjectsV2":
            full = q.get("prefix", [""])[0]
        else:
            full = urllib.parse.unquote(path_part).split("/", 2)[-1]  # drop /bucket/
        if op == "DeleteObjects":
            key = "(batch)"
        elif full.startswith(prefix + "/"):
            key = full[len(prefix) + 1:]
        elif full == prefix:
            key = ""
        else:
            continue  # another database's request (e.g. a restore check in the same process)
        out.append((rid, op, key, cond, status, etag))
    return merge_retries(out)


RETRIES = Counter()


def merge_retries(reqs):
    """object_store retries a request whose attempt failed at the transport level (no
    HTTP status) with an identical one: one logical request, with the last attempt's
    answer. The first attempt may have landed with its answer lost; a create-only or
    If-Match retry then gets a 412, which is how the code sees it too."""
    out = []
    for n, req in enumerate(reqs):
        rid, op, key, cond, status, etag = req
        if not status[:1].isdigit() and status != "none":
            # The next request on this key: object_store's retry is identical; the
            # code's own reaction (a resolving read, the next commit) is not.
            later = next((r for r in reqs[n + 1:] if r[2] == key), None)
            if later is not None and later[1:4] == (op, key, cond):
                RETRIES[f"{op} attempt failed ({status}), retried by object_store"] += 1
                continue
        out.append(req)
    return out


class Converter:
    def __init__(self):
        self.events = []
        self.skipped = Counter()
        self.ordinals = {}  # epoch -> {byte offset: ordinal}
        self.open = False
        self.our_etag = None
        self.pending_frame = None

    def ordinal(self, e, off):
        seen = self.ordinals.setdefault(e, {})
        if off not in seen:
            seen[off] = len(seen) + 1
        return seen[off]

    def emit(self, k, **f):
        ev = {"k": k, "ok": True, "e": (0, 0), "o": 0, "st": "-", "same": False, "gen": 0}
        ev.update(f)
        self.events.append(ev)

    def skip(self, why):
        self.skipped[why] += 1

    def run(self, reqs):
        i = 0
        n = len(reqs)
        while i < n:
            rid, op, key, cond, st, etag = reqs[i]
            nxt = reqs[i + 1] if i + 1 < n else None
            ok = st.startswith("2")
            if key.startswith("probe/"):
                self.skip("probe (open's provider check)")
            elif op == "DeleteObjects":
                if self.phase in ("run", "restore", "compact"):
                    self.emit("gc")
                else:
                    self.skip("probe cleanup (open's provider check)")
            elif key == "lease.json":
                if op == "GetObject" and nxt and nxt[2] == "lease.json" and nxt[1] == "PutObject":
                    if not nxt[4].startswith("2"):
                        # The model's Open is a successful acquisition; this open failed
                        # before it (a landed attempt still consumes a generation).
                        self.skip("lease acquisition failed (no Open)")
                        i += 2
                        continue
                    if self.open and self.phase != "off":
                        self.emit("close")  # the previous incarnation closed or died
                    elif self.open:
                        self.skip("reopen after a failed open (already off)")
                    self.open = True
                    OPENS[0] += 1
                    self.emit("open")
                    self.phase = "open"
                    i += 1
                else:
                    self.skip("lease renewal or release")
            elif key == "manifest.json" and op == "GetObject":
                if st == "404":
                    i = self.bootstrap(reqs, i + 1)
                    continue
                if self.phase == "open":
                    self.phase = "take"
                    self.skip("manifest read before the takeover (part of takeover)")
                else:
                    # adopt_manifest_if_ours: from now on this is the version we hold
                    self.our_etag = etag
                    self.skip("manifest read (adopting a manifest of ours)")
            elif key == "manifest.json" and op == "PutObject":
                if cond == "if-none-match":
                    raise SystemExit(f"unexpected manifest create outside a bootstrap: {rid}")
                kind = "takeover" if self.phase == "take" else ("compact" if self.phase == "compact" else "man")
                self.emit(kind, ok=ok, st=st, e=self.compact_epoch if kind == "compact" else (0, 0))
                if ok:
                    self.our_etag = etag
                # A failed takeover or compaction fails the open; a failed publication
                # leaves the writer running (it retries at the next commit).
                self.phase = {"takeover": "restore", "compact": "run", "man": "run"}[kind] if ok or kind == "man" else "off"
            elif key == "manifest.json" and op == "HeadObject":
                if st.startswith("2"):
                    self.emit("confirm", same=(etag == self.our_etag))
                else:
                    self.emit("confirm", ok=False, st=st)
            elif op == "ListObjectsV2" or (op == "GetObject" and (key.startswith("log/") or key.startswith("snapshots/"))):
                if self.phase == "restore":
                    # the restore's reads: one event for the whole restore
                    j = i
                    while j < n and reqs[j][1] in ("ListObjectsV2", "GetObject") and reqs[j][2] != "manifest.json" and reqs[j][2] != "lease.json":
                        j += 1
                    self.emit("restore")
                    self.phase = "compact"
                    self.skipped["restore reads (one restore event)"] += j - i
                    i = j
                    continue
                self.skip("listing or read (garbage collection, conflict check)")
            elif op == "HeadObject" and key.startswith("snapshots/"):
                self.skip("snapshot upload check (part of the snapshot upload)")
            elif op in ("PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload") and key.startswith("snapshots/"):
                m = SNAP_KEY.match(key)
                e = epoch(m.group(1), m.group(2))
                if op in ("CreateMultipartUpload", "UploadPart"):
                    self.skip("multipart part of a snapshot upload")
                elif self.phase == "compact":
                    self.compact_epoch = e
                    self.skip("snapshot upload of the open's compaction (part of compact)")
                else:
                    self.emit("snap", ok=ok, st=st, e=e)
            elif op in ("PutObject", "PutSeal") and key.startswith("log/"):
                m = LOG_KEY.match(key)
                e, off = epoch(m.group(1), m.group(2)), int(m.group(3))
                o = self.ordinal(e, off)
                if not st[:1].isdigit():
                    # No answer: the code reads the key back (resolve_failed_put).
                    back = next((r for r in reqs[i + 1:i + 6] if r[1] == "GetObject" and r[2] == key), None)
                    if back is not None and back[4].startswith("2"):
                        ok, st = True, "landed"
                        self.skipped["a landed PUT's resolving read (part of the PUT)"] += 1
                    else:
                        st = "absent"
                elif st == "412" and op == "PutSeal":
                    # our own seal already there (seal_epoch compares the bytes)
                    ok = True
                self.emit("seal" if op == "PutSeal" else "frame", ok=ok, st=st, e=e, o=o)
            else:
                raise SystemExit(f"unclassified request {rid}: {op} {key} {cond} {st}")
            i += 1

    phase = "off"
    compact_epoch = (0, 0)

    def bootstrap(self, reqs, i):
        """After a manifest GET 404: the leftover check, then snapshot + create, or a refusal."""
        n = len(reqs)
        while i < n and reqs[i][1] == "ListObjectsV2":
            self.skipped["bootstrap's leftover check (part of bootstrap)"] += 1
            i += 1
        created = None
        while i < n and reqs[i][2].startswith("snapshots/"):
            self.skipped["bootstrap snapshot upload (part of bootstrap)"] += 1
            i += 1
        if i < n and reqs[i][2] == "manifest.json" and reqs[i][1] == "PutObject":
            created = reqs[i]
            i += 1
        if created is None:
            self.emit("refused")
            self.phase = "off"
            return i
        ok = created[4].startswith("2")
        self.emit("bootstrap", ok=ok, st=created[4])
        if ok:
            self.our_etag = created[5]
        self.phase = "run" if ok else "off"
        return i


def tla(ev):
    e = f"[s |-> {ev['e'][0]}, g |-> {ev['e'][1]}]"
    ok = "TRUE" if ev["ok"] else "FALSE"
    same = "TRUE" if ev["same"] else "FALSE"
    return f'[k |-> "{ev["k"]}", ok |-> {ok}, e |-> {e}, o |-> {ev["o"]}, st |-> "{ev["st"]}", same |-> {same}]'


def main():
    log, prefix, out = sys.argv[1], sys.argv[2].strip("/"), sys.argv[3]
    c = Converter()
    c.run(parse(log, prefix))
    # Objects the workload found in S3 at the end (e.g. a killed writer's late PUT):
    # observations the model must explain with objects of its own.
    import os
    present = os.path.join(log, "present.txt") if os.path.isdir(log) else None
    if present and os.path.exists(present):
        for path in open(present).read().split():
            key = urllib.parse.unquote(path).split("/", 2)[-1][len(prefix) + 1:]
            m = LOG_KEY.match(key)
            e = epoch(m.group(1), m.group(2))
            c.emit("present", e=e, o=c.ordinal(e, int(m.group(3))))
    body = ",\n    ".join(tla(ev) for ev in c.events)
    with open(f"{out}/TraceLog.tla", "w") as f:
        f.write(f"---- MODULE TraceLog ----\nTrace == <<\n    {body}\n>>\n====\n")
    kinds = Counter(ev["k"] for ev in c.events)
    print(f"events: {len(c.events)} {dict(kinds)}", file=sys.stderr)
    for why, count in sorted(RETRIES.items()):
        print(f"merged:           {count:5} {why}", file=sys.stderr)
    for why, count in sorted(c.skipped.items()):
        print(f"not in the model: {count:5} {why}", file=sys.stderr)


if __name__ == "__main__":
    main()
