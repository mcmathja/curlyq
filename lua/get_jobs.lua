-- KEYS[1]: the active job list
-- KEYS[2]: this consumer's inflight set
-- KEYS[3]: the job data hash

-- ARGV[1]: the max number of jobs to get

-- Returns: the jobs

-- Get the jobs out of the active job list
local job_ids = redis.call("lrange", KEYS[1], 0, ARGV[1] - 1)
local count = table.getn(job_ids)

-- Add the jobs to the active set
if count > 0 then
  redis.call("sadd", KEYS[2], unpack(job_ids))

  -- Remove the jobs from the active job list
  redis.call("ltrim", KEYS[1], count, -1)

  -- Return the job data
  return redis.call("hmget", KEYS[3], unpack(job_ids))
end
return {}
