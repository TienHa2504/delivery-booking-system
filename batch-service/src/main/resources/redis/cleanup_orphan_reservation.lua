-- KEYS[1] remaining capacity key
-- KEYS[2] reservation state key
--
-- DB is the source of truth. If a Redis reservation has no DB booking row,
-- remove the reservation and restore one unit of remaining capacity.
local remaining_key = KEYS[1]
local reservation_key = KEYS[2]

if redis.call('EXISTS', reservation_key) == 1 then
    redis.call('DEL', reservation_key)
    redis.call('INCR', remaining_key)
    return 'ORPHAN_CLEANED'
end

return 'NOOP'
