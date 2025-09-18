import io
import os
import socket
from typing import Optional

import paramiko


def _sftp_ensure_dirs(sftp: paramiko.SFTPClient, remote_dir: str):
    if not remote_dir or remote_dir == "/":
        return
    parts = []
    for segment in remote_dir.strip("/").split("/"):
        parts.append(segment)
        path = "/" + "/".join(parts)
        try:
            sftp.stat(path)
        except FileNotFoundError:
            sftp.mkdir(path)


def sftp_upload(
    *,
    host: str,
    username: str,
    password: Optional[str] = None,
    pkey_pem: Optional[str] = None,
    passphrase: Optional[str] = None,
    remote_path: str,
    content: str,
):
    if not host or not username:
        raise RuntimeError("SFTP requiere host y username configurados")
    sock = socket.create_connection((host.rstrip("."), 22), timeout=15)
    transport = paramiko.Transport(sock)
    transport.banner_timeout = 15
    key = None
    try:
        if pkey_pem:
            buffer = io.StringIO(pkey_pem)
            try:
                key = paramiko.Ed25519Key.from_private_key(buffer, password=passphrase)
            except Exception:
                buffer.seek(0)
                key = paramiko.RSAKey.from_private_key(buffer, password=passphrase)
            transport.connect(username=username, pkey=key)
        else:
            if not password:
                raise RuntimeError("SFTP necesita password o clave privada")
            transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            directory = os.path.dirname(remote_path) or "/"
            _sftp_ensure_dirs(sftp, directory)
            with sftp.file(remote_path, "wb") as handler:
                handler.write(content.encode("utf-8"))
                handler.flush()
        finally:
            try:
                sftp.close()
            finally:
                transport.close()
    finally:
        try:
            sock.close()
        except Exception:  # pragma: no cover - logging
            pass
