"""Recursive container unpacking.

Uses an explicit worklist (not recursion) so arbitrarily deep nesting such as
ZIP -> ZIP -> MBOX -> emails unrolls naturally, bounded by depth and total
expansion guards (zip-bomb protection).
"""

from __future__ import annotations

import io
import itertools
import mailbox
import os
import tempfile
import zipfile
from collections import deque
from typing import Iterator

from .classify import CONTAINER_KINDS, LEAF_KINDS, classify
from .models import Blob, LeafEmail

MAX_DEPTH = 16
MAX_EXPANSIONS = 10_000


class ContainerError(Exception):
    """Base for problems opening a container."""


class EncryptedContainer(ContainerError):
    pass


class CorruptContainer(ContainerError):
    pass


class UnsupportedContainer(ContainerError):
    pass


try:  # PST support is optional; see docs/DESIGN.md.
    import pypff  # type: ignore

    _PST_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the environment
    _PST_AVAILABLE = False


def _open_zip(blob: Blob) -> Iterator[Blob]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(blob.data))
    except zipfile.BadZipFile:
        raise CorruptContainer("bad zip")

    for info in zf.infolist():
        if info.is_dir():
            continue
        if info.flag_bits & 0x1:
            raise EncryptedContainer("zip member is encrypted")
        try:
            data = zf.read(info)
        except RuntimeError:  # encrypted without password
            raise EncryptedContainer("zip member is encrypted")
        except zipfile.BadZipFile:
            raise CorruptContainer("bad zip member")
        yield Blob(
            name=info.filename,
            data=data,
            lineage=blob.lineage.extend("!" + info.filename),
            partition=blob.partition,
            source_path=blob.source_path,
        )


def _open_mbox(blob: Blob) -> Iterator[Blob]:
    with tempfile.NamedTemporaryFile(suffix=".mbox", delete=False) as tmp:
        tmp.write(blob.data)
        tmp_path = tmp.name
    try:
        box = mailbox.mbox(tmp_path)
        try:
            for i, msg in enumerate(box):
                yield Blob(
                    name="%s#%d" % (blob.name, i),
                    data=msg.as_bytes(),
                    lineage=blob.lineage.extend("#%d" % i),
                    partition=blob.partition,
                    source_path=blob.source_path,
                    kind="eml",
                )
        finally:
            box.close()
    finally:
        os.unlink(tmp_path)


def _pst_message_bytes(message) -> bytes:
    headers = ""
    try:
        headers = message.transport_headers or ""
    except Exception:
        headers = ""

    body = message.plain_text_body if hasattr(message, "plain_text_body") else b""
    if isinstance(body, bytes):
        body = body.decode("utf-8", "replace")
    body = body or ""

    if headers:
        text = headers if headers.endswith("\n") else headers + "\n"
        text += "\n" + body
    else:
        subject = ""
        try:
            subject = message.subject or ""
        except Exception:
            subject = ""
        text = "Subject: %s\n\n%s" % (subject, body)
    return text.encode("utf-8", "replace")


def _walk_pst_folder(folder, blob: Blob, counter) -> Iterator[Blob]:
    for i in range(folder.number_of_sub_messages):
        message = folder.get_sub_message(i)
        idx = next(counter)
        yield Blob(
            name="%s#%d" % (blob.name, idx),
            data=_pst_message_bytes(message),
            lineage=blob.lineage.extend("#%d" % idx),
            partition=blob.partition,
            source_path=blob.source_path,
            kind="eml",
        )
    for j in range(folder.number_of_sub_folders):
        yield from _walk_pst_folder(folder.get_sub_folder(j), blob, counter)


def _open_pst(blob: Blob) -> Iterator[Blob]:
    if not _PST_AVAILABLE:
        raise UnsupportedContainer("pst parser unavailable")

    with tempfile.NamedTemporaryFile(suffix=".pst", delete=False) as tmp:
        tmp.write(blob.data)
        tmp_path = tmp.name
    try:
        pff = pypff.file()
        try:
            pff.open(tmp_path)
        except Exception:
            raise CorruptContainer("bad pst")
        try:
            root = pff.get_root_folder()
            children = list(_walk_pst_folder(root, blob, itertools.count()))
        finally:
            pff.close()
        for child in children:
            yield child
    finally:
        os.unlink(tmp_path)


def open_container(blob: Blob, kind: str) -> Iterator[Blob]:
    if kind == "zip":
        return _open_zip(blob)
    if kind == "mbox":
        return _open_mbox(blob)
    if kind == "pst":
        return _open_pst(blob)
    raise ValueError("not a container: %s" % kind)


def expand(root: Blob, store, max_depth: int = MAX_DEPTH, max_expansions: int = MAX_EXPANSIONS) -> Iterator[LeafEmail]:
    """Yield leaf emails reachable from ``root``, recording skips along the way."""
    work = deque([(root, 0)])
    expansions = 0

    while work:
        current, depth = work.popleft()
        kind = classify(current)

        if kind in LEAF_KINDS:
            yield LeafEmail(
                data=current.data,
                lineage=current.lineage,
                partition=current.partition,
                kind=kind,
                source_path=current.source_path,
            )
            continue

        if kind in CONTAINER_KINDS:
            if depth >= max_depth:
                store.record_skip(current.source_path, str(current.lineage), "depth_exceeded")
                continue
            try:
                children = list(open_container(current, kind))
            except EncryptedContainer:
                store.record_skip(current.source_path, str(current.lineage), "encrypted_%s" % kind)
                continue
            except CorruptContainer:
                store.record_skip(current.source_path, str(current.lineage), "corrupt_%s" % kind)
                continue
            except UnsupportedContainer:
                store.record_skip(current.source_path, str(current.lineage), "%s_unsupported" % kind)
                continue
            except Exception:
                store.record_skip(current.source_path, str(current.lineage), "corrupt_%s" % kind)
                continue

            if not children:
                store.record_skip(current.source_path, str(current.lineage), "empty_container")
                continue

            for child in children:
                expansions += 1
                if expansions > max_expansions:
                    store.record_skip(current.source_path, str(current.lineage), "expansion_exceeded")
                    return
                work.append((child, depth + 1))
            continue

        store.record_skip(current.source_path, str(current.lineage), "non_email")
