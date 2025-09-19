import io
import os
import socket
from typing import Optional, Union

import paramiko


def _sftp_ensure_dirs(sftp: paramiko.SFTPClient, remote_dir: str):
    """
    Culture Amp solo permite subir a '/', no crear directorios.
    Esta función hace no-op si remote_dir es '/' o vacío.
    La dejamos genérica por si reusas este cliente con otros SFTP.
    """
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
    content: Union[str, bytes],
):
    """
    Sube 'content' vía SFTP como archivo en 'remote_path'.
    - Admite auth por password o por llave (preferida para Culture Amp).
    - 'content' puede ser str (se codifica UTF-8) o bytes.
    """
    if not host or not username:
        raise RuntimeError("SFTP requiere host y username configurados")

    # Conexión TCP al puerto 22 con timeout corto para evitar cuelgues
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

            payload = content if isinstance(content, (bytes, bytearray)) else content.encode("utf-8")
            with sftp.file(remote_path, "wb") as handler:
                handler.write(payload)
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
