from __future__ import annotations

import io
import os
import socket
from typing import Callable, Optional, TypeVar, Union

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

T = TypeVar("T")

def sftp_upload(
    *,
    host: str,
    username: str,
    remote_path: str,
    content: Optional[Union[str, bytes]] = None,
    writer: Optional[Callable[[paramiko.SFTPFile], T]] = None,
    pkey_pem: Optional[str] = None,
    password: Optional[str] = None,
    passphrase: Optional[str] = None,
) -> Optional[T]:
    """
    Sube contenido vía SFTP como archivo en 'remote_path'.
    Admite auth con password o con llave (recomendado).
    Puedes pasar 'content' (str/bytes) o un 'writer(handler) -> T'.
    """
    if not host or not username:
        raise RuntimeError("SFTP requiere host y username configurados")
    if content is None and writer is None:
        raise ValueError("sftp_upload requiere 'content' o 'writer'")
    if content is not None and writer is not None:
        raise ValueError("sftp_upload no acepta 'content' y 'writer' al mismo tiempo")

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
                result: Optional[T] = None
                if writer is not None:
                    result = writer(handler)
                else:
                    payload = content if isinstance(content, (bytes, bytearray)) else content.encode("utf-8")
                    handler.write(payload)
                handler.flush()
                return result
        finally:
            try:
                sftp.close()
            finally:
                transport.close()
    finally:
        try:
            sock.close()
        except Exception:
            pass
