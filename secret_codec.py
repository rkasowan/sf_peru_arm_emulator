#!/usr/bin/env python3
"""Lightweight reversible obfuscation for env-file secrets.

This is intentionally only casual obfuscation, not real encryption.
It exists so credentials are not stored in plain text in env files.
"""

from __future__ import annotations

import argparse
import hashlib
from typing import Iterable

SALT = "jabberwocky"
PREFIX = "OBFMD5:"


def _keystream(length: int) -> bytes:
    seed = hashlib.md5(SALT.encode("utf-8")).digest()
    chunks = bytearray()
    counter = 0
    while len(chunks) < length:
        block = hashlib.md5(seed + counter.to_bytes(4, "big")).digest()
        chunks.extend(block)
        counter += 1
    return bytes(chunks[:length])


def encode_if_needed(value: str) -> str:
    if not value or value.startswith(PREFIX):
        return value
    plaintext = value.encode("utf-8")
    secret = bytes(a ^ b for a, b in zip(plaintext, _keystream(len(plaintext))))
    return PREFIX + secret.hex()


def decode_if_needed(value: str) -> str:
    if not value or not value.startswith(PREFIX):
        return value
    payload = value[len(PREFIX):]
    if not payload:
        return ""
    try:
        secret = bytes.fromhex(payload)
    except ValueError:
        return value
    plaintext = bytes(a ^ b for a, b in zip(secret, _keystream(len(secret))))
    try:
        return plaintext.decode("utf-8")
    except UnicodeDecodeError:
        return value


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Encode or decode obfuscated env secrets.")
    parser.add_argument("mode", choices=("encode", "decode"))
    parser.add_argument("value")
    args = parser.parse_args(argv)

    if args.mode == "encode":
        print(encode_if_needed(args.value))
    else:
        print(decode_if_needed(args.value))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
