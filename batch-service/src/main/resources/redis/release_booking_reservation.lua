-- KEYS[1] remaining capacity key
-- KEYS[2] reservation state key
--
-- The DB slot_released flag guards idempotency. This script keeps the Redis capacity
-- increment and reservation state transition atomic when the Retry Job fails finally.
local remaining_key = KEYS[1]
local reservation_key = KEYS[2]

redis.call('INCR', remaining_key)
redis.call('SET', reservation_key, 'FAILED')

return 'RELEASED'
