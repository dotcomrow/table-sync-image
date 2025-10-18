import json
from typing import Any, Optional
import redis
from classes.logging import Logging
from classes.config_reader import ConfigReader, ConfigKeys, RedisKeys

class RedisService:
    table_count_key_format = "table_sync:{db}.{table_info.schema}.{table_info.table}:yugabyte_count"
    
    
    def __init__(self, config: ConfigReader, logging: Logging):
        self._r = redis.Redis.from_url(config.get(ConfigKeys.REDIS.value, {}).get(RedisKeys.URL.value))
        self._prefix = config.get(ConfigKeys.REDIS.value, {}).get(RedisKeys.CACHE_KEY.value)
        
        self._default_ttl = int(
            config.get(ConfigKeys.REDIS.value, {}).get(RedisKeys.DEFAULT_TTL.value, "600")
        )
        self.logging = logging
        self.logging.logMessage(Logging.LogLevel.INFO, "Initialized RedisService", prefix=self._prefix, default_ttl=self._default_ttl)
        
    def _k(self, ns: str, key: str) -> str:
        return f"{self._prefix}:{ns}:{key}"

    def get(self, ns: str, key: str) -> Optional[Any]:
        self.logging.logMessage(Logging.LogLevel.DEBUG, "Getting value from Redis", namespace=ns, key=key)
        val = self._r.get(self._k(ns, key))
        if val is None:
            return None
        try:
            return json.loads(val)
        except Exception:
            return val
        finally:
            self.logging.logMessage(Logging.LogLevel.INFO, "Retrieved value from Redis", namespace=ns, key=key, value=val)

    def set(self, ns: str, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        self.logging.logMessage(Logging.LogLevel.INFO, "Setting value in Redis", namespace=ns, key=key, value=value, ttl_seconds=ttl_seconds)
        s = json.dumps(value)
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        if ttl and ttl > 0:
            self._r.setex(self._k(ns, key), int(ttl), s)
        else:
            self._r.set(self._k(ns, key), s)

    def delete(self, ns: str, key: str) -> None:
        self.logging.logMessage(Logging.LogLevel.INFO, "Deleting value from Redis", namespace=ns, key=key)
        self._r.delete(self._k(ns, key))

    def namespace(self) -> str:
        return self._prefix