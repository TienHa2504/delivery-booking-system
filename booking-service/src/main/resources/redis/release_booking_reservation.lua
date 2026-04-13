-- KEYS[1] remaining capacity key
-- KEYS[2] reservation state key
--
-- DB slot_released is the primary idempotency guard. This script keeps the Redis side atomic
-- for the one allowed release: capacity is restored and reservation state is marked FAILED.
local remaining_key = KEYS[1]
local reservation_key = KEYS[2]

redis.call('INCR', remaining_key)
redis.call('SET', reservation_key, 'FAILED')

return 'RELEASED'
