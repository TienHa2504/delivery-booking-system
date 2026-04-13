-- KEYS[1] remaining capacity key
-- KEYS[2] reservation state key
--
-- Rollback is also scripted so the reservation key removal and capacity restoration
-- remain atomic after a DB insert failure.
local remaining_key = KEYS[1]
local reservation_key = KEYS[2]

local existing_state = redis.call('GET', reservation_key)
if existing_state == 'PENDING' then
    redis.call('DEL', reservation_key)
    redis.call('INCR', remaining_key)
    return 'ROLLED_BACK'
end

return 'NOOP'
