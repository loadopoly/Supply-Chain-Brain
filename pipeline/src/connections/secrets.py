"""
Per-user encrypted credential vault for Astec pipeline connectors.

Storage: %LOCALAPPDATA%\\AstecPipeline\\creds.dat
Encryption: Windows DPAPI via CryptProtectData (CRYPTPROTECT_LOCAL_MACHINE = 0).
Bytes are encrypted under the current Windows user account — no other user
(and no other machine) can decrypt the file. The vault directory is created
with default ACLs (user-only).

CLI usage:
    python -m src.connections.secrets set azure_sql --user agard@... --password ...
    python -m src.connections.secrets get azure_sql
    python -m src.connections.secrets list
    python -m src.connections.secrets delete azure_sql
"""
from __future__ import annotations

import argparse
import ctypes
import ctypes.wintypes as wt
import getpass
import json
import os
import sys
from pathlib import Path
from typing import Any

_VAULT_NAME = "AstecPipeline"
_VAULT_FILE = "creds.dat"


def _vault_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    d = Path(base) / _VAULT_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _vault_path() -> Path:
    return _vault_dir() / _VAULT_FILE


# --- DPAPI bindings ---------------------------------------------------------

class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wt.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]


def _blob_in(data: bytes) -> _DATA_BLOB:
    buf = ctypes.create_string_buffer(data, len(data))
    return _DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))


def _blob_out(blob: _DATA_BLOB) -> bytes:
    out = ctypes.string_at(blob.pbData, blob.cbData)
    ctypes.windll.kernel32.LocalFree(blob.pbData)
    return out


def _protect(plaintext: bytes) -> bytes:
    if sys.platform != "win32":
        raise RuntimeError("DPAPI vault is Windows-only.")
    in_blob = _blob_in(plaintext)
    out_blob = _DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(in_blob), "AstecPipeline", None, None, None, 0, ctypes.byref(out_blob)
    )
    if not ok:
        raise OSError(ctypes.get_last_error(), "CryptProtectData failed")
    return _blob_out(out_blob)


def _unprotect(ciphertext: bytes) -> bytes:
    if sys.platform != "win32":
        raise RuntimeError("DPAPI vault is Windows-only.")
    in_blob = _blob_in(ciphertext)
    out_blob = _DATA_BLOB()
    descr = wt.LPWSTR()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob), ctypes.byref(descr), None, None, None, 0, ctypes.byref(out_blob)
    )
    if not ok:
        raise OSError(ctypes.get_last_error(), "CryptUnprotectData failed")
    return _blob_out(out_blob)


# --- Vault read/write -------------------------------------------------------

def _read_all() -> dict[str, dict[str, Any]]:
    p = _vault_path()
    if not p.exists() or p.stat().st_size == 0:
        return {}
    blob = p.read_bytes()
    plain = _unprotect(blob)
    return json.loads(plain.decode("utf-8"))


def _write_all(store: dict[str, dict[str, Any]]) -> None:
    plain = json.dumps(store, sort_keys=True).encode("utf-8")
    cipher = _protect(plain)
    p = _vault_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_bytes(cipher)
    os.replace(tmp, p)


def set_credentials(scope: str, **fields: str) -> None:
    """Store one credential bundle under `scope` (e.g. 'azure_sql', 'oracle_fusion')."""
    store = _read_all()
    store[scope] = {k: v for k, v in fields.items() if v is not None}
    _write_all(store)


def get_credentials(scope: str) -> dict[str, str] | None:
    """Return the credential bundle for `scope`, or None if not set."""
    return _read_all().get(scope)


def delete_credentials(scope: str) -> bool:
    store = _read_all()
    if scope in store:
        del store[scope]
        _write_all(store)
        return True
    return False


def list_scopes() -> list[str]:
    return sorted(_read_all().keys())


# --- CLI --------------------------------------------------------------------

def _redact(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return value[:1] + "*" * (len(value) - 2) + value[-1:]


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Astec pipeline credential vault (DPAPI).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_set = sub.add_parser("set", help="Save credentials for a scope.")
    p_set.add_argument("scope")
    p_set.add_argument("--user", required=True)
    p_set.add_argument("--password", default=None, help="If omitted, prompts securely.")

    p_get = sub.add_parser("get", help="Show stored fields for a scope (password redacted).")
    p_get.add_argument("scope")

    p_del = sub.add_parser("delete", help="Remove credentials for a scope.")
    p_del.add_argument("scope")

    sub.add_parser("list", help="List scopes with stored credentials.")
    sub.add_parser("path", help="Print vault file path.")

    args = parser.parse_args(argv)

    if args.cmd == "set":
        pwd = args.password if args.password is not None else getpass.getpass("Password: ")
        set_credentials(args.scope, user=args.user, password=pwd)
        print(f"Saved credentials for '{args.scope}' to {_vault_path()}")
    elif args.cmd == "get":
        creds = get_credentials(args.scope)
        if not creds:
            print(f"No credentials stored for '{args.scope}'.")
            return 1
        for k, v in creds.items():
            print(f"  {k}: {_redact(v) if k.lower() in ('password','secret','client_secret') else v}")
    elif args.cmd == "delete":
        ok = delete_credentials(args.scope)
        print("Deleted." if ok else f"No credentials stored for '{args.scope}'.")
    elif args.cmd == "list":
        scopes = list_scopes()
        if not scopes:
            print("(vault empty)")
        for s in scopes:
            print(s)
    elif args.cmd == "path":
        print(_vault_path())
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
