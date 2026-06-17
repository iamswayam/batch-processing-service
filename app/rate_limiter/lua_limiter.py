import logging
import time

from redis.asyncio import Redis

from app.config import settings

logger = logging.getLogger(__name__)

# Atomic token bucket Lua script.
# Refills tokens based on elapsed time, then grants or denies the request.
RATE_LIMIT_SCRIPT = """
local data    = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens  = tonumber(data[1]) or tonumber(ARGV[1])
local ts      = tonumber(data[2]) or tonumber(ARGV[3])
local elapsed = math.max(0, (tonumber(ARGV[3]) - ts) / 1000)
tokens = math.min(tonumber(ARGV[1]), tokens + elapsed * tonumber(ARGV[2]))
local allowed = 0
if tokens >= tonumber(ARGV[4]) then
    tokens  = tokens - tonumber(ARGV[4])
    allowed = 1
end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', ARGV[3])
redis.call('EXPIRE', KEYS[1], 3600)
return allowed
"""


def get_rate_limit_config(tenant_id: str) -> tuple[int, float]:
    tenant_config = settings.tenant_rate_limits.get(tenant_id, {})
    capacity = int(tenant_config.get("capacity", settings.default_rate_limit_capacity))
    refill_rate = float(tenant_config.get("refill_rate", settings.default_rate_limit_refill_rate))

    if capacity <= 0 or refill_rate <= 0:
        raise ValueError("Rate limit capacity and refill_rate must be positive")

    return capacity, refill_rate


async def acquire_token(redis: Redis, tenant_id: str) -> bool:
    """
    Attempt to acquire one token from the tenant's bucket.
    Returns True if granted, False if rate limited.
    """
    key = f"rate_limit:{tenant_id}"
    now_ms = int(time.time() * 1000)
    capacity, refill_rate = get_rate_limit_config(tenant_id)

    result = await redis.eval(
        RATE_LIMIT_SCRIPT,
        1,           # number of keys
        key,         # KEYS[1]
        capacity,     # ARGV[1] capacity
        refill_rate,  # ARGV[2] refill per second
        now_ms,      # ARGV[3] current time in ms
        1,           # ARGV[4] tokens requested
    )

    granted = bool(result)
    if not granted:
        logger.info("Rate limited: tenant=%s", tenant_id)
    return granted
