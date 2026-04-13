-- KEYS[1] remaining capacity key, e.g. booking:opportunity:{opportunityId}:remaining
-- KEYS[2] reservation state key, e.g. booking:reservation:{opportunityId}:{driverId}
-- ARGV[1] reservation TTL seconds; 0 keeps the key without expiry
--
-- Atomicity matters here: duplicate detection and remaining-capacity decrement must happen
-- in the same Redis operation, otherwise concurrent requests can oversell.
local remaining_key = KEYS[1]
local reservation_key = KEYS[2]
local ttl_seconds = tonumber(ARGV[1])

local existing_state = redis.call('GET', reservation_key)
if existing_state then
    if existing_state == 'CONFIRMED' then
        return 'ALREADY_BOOKED'
    end
    return 'ALREADY_PENDING'
end

local remaining = tonumber(redis.call('GET', remaining_key) or '0')
if remaining <= 0 then
    return 'SOLD_OUT'
end

redis.call('DECR', remaining_key)
if ttl_seconds and ttl_seconds > 0 then
    redis.call('SET', reservation_key, 'PENDING', 'EX', ttl_seconds)
else
    redis.call('SET', reservation_key, 'PENDING')
end

return 'RESERVED'
