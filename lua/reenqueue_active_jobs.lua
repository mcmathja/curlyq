-- KEYS[1]: this consumer's inflight set
-- KEYS[2]: the active jobs list

-- ARGV[]: the list of job IDs

-- Returns: nil

for _,job_id in ipairs(ARGV) do
  -- Remove the jobs from this consumer's inflight set
  local removed = redis.call("srem", KEYS[1], job_id)

  if removed == 1 then
    -- Push the job back into the active jobs list
    redis.call("rpush", KEYS[2], job_id)
  end
end

return true
