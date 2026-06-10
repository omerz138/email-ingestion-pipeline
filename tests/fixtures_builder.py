"""Build a sample bucket on disk, covering the provided fixtures plus the
edge cases from the assignment. Used by the tests and by
``scripts/generate_test_data.py``.
"""

from __future__ import annotations

import io
import os
import zipfile

NAMESPACE = "namespace_a"
P1 = "2024-07-15"
P2 = "2024-07-16"
P3 = "2024-07-17"  # edge-case partition

SAME_BODY = "Shared body, identical bytes everywhere.\n"


def make_eml(subject: str, body: str, frm: str = "a@example.com", to: str = "b@example.com") -> bytes:
    return (
        "From: %s\n"
        "To: %s\n"
        "Subject: %s\n"
        "Date: Mon, 15 Jul 2024 10:00:00 +0000\n"
        "\n"
        "%s\n" % (frm, to, subject, body)
    ).encode("utf-8")


def make_html(subject: str, body: str) -> bytes:
    return (
        "From: a@example.com\nTo: b@example.com\nSubject: %s\n"
        "Content-Type: text/html\n\n<html><body>%s</body></html>\n" % (subject, body)
    ).encode("utf-8")


def make_mbox(messages) -> bytes:
    out = b""
    for msg in messages:
        out += b"From sender@example.com Mon Jul 15 10:00:00 2024\n"
        out += msg
        if not msg.endswith(b"\n"):
            out += b"\n"
        out += b"\n"
    return out


def make_zip(entries) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def mark_zip_encrypted(data: bytes) -> bytes:
    """Flip the general-purpose 'encrypted' bit in a zip's headers.

    Lets us build a fixture our unpacker treats as password-protected without
    needing a third-party zip library.
    """
    b = bytearray(data)
    for signature, flag_offset in ((b"PK\x03\x04", 6), (b"PK\x01\x02", 8)):
        i = 0
        while True:
            i = b.find(signature, i)
            if i < 0:
                break
            b[i + flag_offset] |= 0x01
            i += 4
    return bytes(b)


def _write(root: str, namespace: str, partition: str, name: str, data: bytes) -> None:
    directory = os.path.join(root, namespace, "timestamp=" + partition)
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "wb") as fh:
        fh.write(data)


def build_bucket(root: str, namespace: str = NAMESPACE) -> str:
    """Populate ``root`` with the fixture bucket and return ``root``."""

    # --- Provided fixtures -------------------------------------------------
    _write(root, namespace, P1, "simple_email.eml", make_eml("Simple", "Hello world"))
    _write(root, namespace, P1, "another_email.html", make_html("HTML export", "Hi there"))
    _write(root, namespace, P1, "batch.zip", make_zip({
        "invoice.eml": make_eml("Invoice", "Amount due: 100"),
        "receipt.eml": make_eml("Receipt", "Thanks for paying"),
    }))
    _write(root, namespace, P1, "conversations.mbox", make_mbox([
        make_eml("Conversation 1", "First message"),
        make_eml("Conversation 2", "Second message"),
        make_eml("Conversation 3", "Third message"),
    ]))
    _write(root, namespace, P2, "new_email.eml", make_eml("New", "A later email"))

    # --- Edge case 1: same filename in different partitions, different bytes
    _write(root, namespace, P1, "dup_name.eml", make_eml("Dup name", "content in 07-15"))
    _write(root, namespace, P2, "dup_name.eml", make_eml("Dup name", "content in 07-16"))

    # --- Edge case 2: ZIP containing a ZIP containing emails ---------------
    inner = make_zip({
        "deep1.eml": make_eml("Deep 1", "nested email one"),
        "deep2.eml": make_eml("Deep 2", "nested email two"),
    })
    _write(root, namespace, P3, "nested.zip", make_zip({"inner.zip": inner}))

    # --- Edge case 10: ZIP -> ZIP -> MBOX -> emails ------------------------
    chain_mbox = make_mbox([
        make_eml("Chain A", "deep chain message a"),
        make_eml("Chain B", "deep chain message b"),
    ])
    mid = make_zip({"chain.mbox": chain_mbox})
    _write(root, namespace, P3, "deep_chain.zip", make_zip({"mid.zip": mid}))

    # --- Edge case 3: identical inner names, different content -> distinct --
    _write(root, namespace, P3, "collide_a.zip", make_zip({"001.eml": make_eml("Collide A", "alpha")}))
    _write(root, namespace, P3, "collide_b.zip", make_zip({"001.eml": make_eml("Collide B", "beta")}))

    # --- Identical content from two sources -> collapse to one email -------
    _write(root, namespace, P3, "dup_a.zip", make_zip({"x.eml": make_eml("Same", SAME_BODY)}))
    _write(root, namespace, P3, "dup_b.zip", make_zip({"y.eml": make_eml("Same", SAME_BODY)}))

    # --- Edge case 4: password-protected ZIP -------------------------------
    _write(root, namespace, P3, "protected.zip",
           mark_zip_encrypted(make_zip({"secret.eml": make_eml("Secret", "hidden")})))

    # --- Edge case 5: corrupted ZIP ----------------------------------------
    _write(root, namespace, P3, "corrupt.zip", b"PK\x03\x04 this is not really a zip file")

    # --- Edge case 6: non-email files --------------------------------------
    _write(root, namespace, P3, "image.png", b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
    _write(root, namespace, P3, "sheet.xlsx", b"PK\x03\x04" + b"\x00" * 8 + b"fake xlsx")

    # --- Edge case 7: empty containers -------------------------------------
    _write(root, namespace, P3, "empty.zip", make_zip({}))
    _write(root, namespace, P3, "empty.mbox", b"")

    return root


# Count of emails a clean full run should stage from build_bucket():
#   provided: simple(1) + html(1) + batch(2) + mbox(3) + new(1)          = 8
#   dup_name in two partitions (different bytes)                          = 2
#   nested.zip -> deep1, deep2                                            = 2
#   deep_chain.zip -> chain.mbox -> 2 messages                           = 2
#   collide_a/collide_b 001.eml (different bytes)                         = 2
#   dup_a/dup_b identical bytes -> collapse                              = 1
EXPECTED_UNIQUE_EMAILS = 17
