# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis
import uuid

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        key = self.key_schema.sliding_window_rate_limiter_key(name, int(self.window_size_ms), self.max_hits)
        now = datetime.datetime.now().timestamp()
        mapping = {str(uuid.uuid4()): now}
        pipe = self.redis.pipeline()
        pipe.zadd(key, mapping)
        pipe.zremrangebyscore(key, 0, now - self.window_size_ms / 1000)
        pipe.zcard(key)
        _, _, hits = pipe.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()

        # END Challenge #7
