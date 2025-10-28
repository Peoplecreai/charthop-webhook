"""Rate limiter and cache utilities for API calls."""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Callable, Dict, Optional, TypeVar

T = TypeVar("T")


class RateLimiter:
    """
    Rate limiter que controla el número de requests en una ventana de tiempo.

    Ejemplo:
        limiter = RateLimiter(max_requests=100, window_seconds=60)
        limiter.wait_if_needed()  # Espera si se excedió el límite
    """

    def __init__(self, max_requests: int, window_seconds: int):
        """
        Args:
            max_requests: Número máximo de requests permitidos
            window_seconds: Ventana de tiempo en segundos
        """
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests = deque()

    def wait_if_needed(self) -> float:
        """
        Espera si se excedió el límite de rate.

        Returns:
            Tiempo esperado en segundos (0 si no hubo que esperar)
        """
        now = time.time()

        # Limpiar requests fuera de la ventana
        while self.requests and self.requests[0] < now - self.window:
            self.requests.popleft()

        # Si excedimos el límite, esperar
        if len(self.requests) >= self.max_requests:
            sleep_time = self.window - (now - self.requests[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.requests.popleft()  # Remover el más viejo
                waited = sleep_time
            else:
                waited = 0.0
        else:
            waited = 0.0

        self.requests.append(time.time())
        return waited


class TimedCache:
    """
    Caché simple con expiración por tiempo.

    Ejemplo:
        cache = TimedCache(ttl_seconds=300)
        cache.set("key", {"data": "value"})
        data = cache.get("key")  # None si expiró o no existe
    """

    def __init__(self, ttl_seconds: int = 300):
        """
        Args:
            ttl_seconds: Tiempo de vida del caché en segundos (default: 5 minutos)
        """
        self.ttl = ttl_seconds
        self._cache: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        """
        Obtiene un valor del caché si no ha expirado.

        Args:
            key: Clave del caché

        Returns:
            Valor almacenado o None si no existe o expiró
        """
        if key not in self._cache:
            return None

        timestamp, value = self._cache[key]
        if time.time() - timestamp > self.ttl:
            # Expiró, eliminar
            del self._cache[key]
            return None

        return value

    def set(self, key: str, value: Any) -> None:
        """
        Almacena un valor en el caché.

        Args:
            key: Clave del caché
            value: Valor a almacenar
        """
        self._cache[key] = (time.time(), value)

    def clear(self) -> None:
        """Limpia todo el caché."""
        self._cache.clear()

    def cleanup_expired(self) -> int:
        """
        Elimina entradas expiradas del caché.

        Returns:
            Número de entradas eliminadas
        """
        now = time.time()
        expired_keys = [
            key for key, (timestamp, _) in self._cache.items()
            if now - timestamp > self.ttl
        ]

        for key in expired_keys:
            del self._cache[key]

        return len(expired_keys)


class DictCache:
    """
    Caché de diccionarios con expiración por tiempo.
    Útil para cachear listas completas transformadas en diccionarios por key.

    Ejemplo:
        cache = DictCache(ttl_seconds=300)
        cache.load(lambda: fetch_people(), key_fn=lambda p: p["email"].lower())
        person = cache.get("john@example.com")
    """

    def __init__(self, ttl_seconds: int = 300):
        """
        Args:
            ttl_seconds: Tiempo de vida del caché en segundos
        """
        self.ttl = ttl_seconds
        self._data: Dict[str, Any] = {}
        self._loaded_at: Optional[float] = None

    def is_expired(self) -> bool:
        """Verifica si el caché ha expirado."""
        if self._loaded_at is None:
            return True
        return time.time() - self._loaded_at > self.ttl

    def load(
        self,
        loader_fn: Callable[[], list[Any]],
        key_fn: Callable[[Any], str]
    ) -> None:
        """
        Carga datos en el caché.

        Args:
            loader_fn: Función que retorna lista de items
            key_fn: Función que extrae la key de cada item
        """
        items = loader_fn()
        self._data = {key_fn(item): item for item in items if key_fn(item)}
        self._loaded_at = time.time()

    def get(self, key: str) -> Optional[Any]:
        """
        Obtiene un valor del caché.

        Args:
            key: Clave del item

        Returns:
            Item almacenado o None
        """
        if self.is_expired():
            return None
        return self._data.get(key)

    def get_all(self) -> Optional[Dict[str, Any]]:
        """
        Obtiene todo el diccionario si no ha expirado.

        Returns:
            Diccionario completo o None si expiró
        """
        if self.is_expired():
            return None
        return self._data.copy()

    def clear(self) -> None:
        """Limpia el caché."""
        self._data.clear()
        self._loaded_at = None
