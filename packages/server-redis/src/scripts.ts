/**
 * Lua scripts that perform every multi-key mutation atomically inside Redis,
 * so several server instances can share one Redis without coordinating.
 *
 * Key layout (all under a configurable prefix):
 *   {prefix}meta:{path}       HASH   stream metadata
 *   {prefix}log:{path}        STREAM entries: d=<bytes> t=<timestamp>; entry id minted per offset scheme
 *   {prefix}producers:{path}  HASH   producerId -> JSON {epoch,lastSeq,lastUpdated}
 *   {prefix}notify            pub/sub channel; payload is the stream path that changed
 */

const PRELUDE = `
local function arm_expiry(meta, log, producers)
  local m = redis.call('HMGET', meta, 'ttlSeconds', 'expiresAtMs', 'refCount')
  if tonumber(m[3] or '0') > 0 then
    redis.call('PERSIST', meta)
    redis.call('PERSIST', log)
    redis.call('PERSIST', producers)
  elseif m[1] then
    local ms = math.floor(tonumber(m[1]) * 1000)
    redis.call('PEXPIRE', meta, ms)
    redis.call('PEXPIRE', log, ms)
    redis.call('PEXPIRE', producers, ms)
  elseif m[2] then
    local at = math.floor(tonumber(m[2]))
    redis.call('PEXPIREAT', meta, at)
    redis.call('PEXPIREAT', log, at)
    redis.call('PEXPIREAT', producers, at)
  end
end

local function validate_producer(producers, pid, epoch, pseq)
  local raw = redis.call('HGET', producers, pid)
  if not raw then
    if pseq ~= 0 then return {'sequence_gap', 0, pseq} end
    return nil, {epoch = epoch, lastSeq = 0, isNew = 1}
  end
  local st = cjson.decode(raw)
  if epoch < st.epoch then return {'stale_epoch', st.epoch} end
  if epoch > st.epoch then
    if pseq ~= 0 then return {'invalid_epoch_seq'} end
    return nil, {epoch = epoch, lastSeq = 0, isNew = 1}
  end
  if pseq <= st.lastSeq then return {'duplicate', st.lastSeq} end
  if pseq == st.lastSeq + 1 then
    return nil, {epoch = epoch, lastSeq = pseq, isNew = 0}
  end
  return {'sequence_gap', st.lastSeq + 1, pseq}
end

local function closed_by_matches(raw, pid, epoch, pseq)
  if not raw then return false end
  local cb = cjson.decode(raw)
  return cb.producerId == pid and cb.epoch == epoch and cb.seq == pseq
end

local function parse_id(id)
  local first, second = string.match(id, '^(%d+)%-(%d+)$')
  return tonumber(first), tonumber(second)
end

local function id_lt(a, b)
  local af, as = parse_id(a)
  local bf, bs = parse_id(b)
  if af ~= bf then return af < bf end
  return as < bs
end

-- Next entry id after currentId for a message of len bytes.
-- bytes: id is the byte offset after the message (4-byte length + data + newline).
-- time: Redis-style ms-seq, never below the current id even if the clock steps back.
local function mint_id(scheme, currentId, now, len)
  local first, second = parse_id(currentId)
  if scheme == 'time' then
    local nowMs = tonumber(now)
    if nowMs > first then return string.format('%d-0', nowMs) end
    return string.format('%d-%d', first, second + 1)
  end
  return string.format('%d-0', first + 5 + len)
end
`

/**
 * KEYS: meta, log, producers
 * ARGV: now, data, seq, producerId, producerEpoch, producerSeq, close, channel, path, scheme
 */
export const APPEND_SCRIPT: string = withPrelude(`
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local now, data, seq, pid = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local epoch, pseq = tonumber(ARGV[5]), tonumber(ARGV[6])
local close, channel, path, scheme = ARGV[7] == '1', ARGV[8], ARGV[9], ARGV[10]

if redis.call('EXISTS', meta) == 0 then return {'not_found'} end
local m = redis.call('HMGET', meta, 'currentId', 'closed', 'closedBy', 'lastSeq', 'softDeleted')
if m[5] == '1' then return {'soft_deleted'} end
local currentId = m[1]

if m[2] == '1' then
  if pid ~= '' and closed_by_matches(m[3], pid, epoch, pseq) then
    return {'closed_duplicate', currentId, pseq}
  end
  return {'closed', currentId}
end

local state = nil
if pid ~= '' then
  local err, st = validate_producer(producers, pid, epoch, pseq)
  if err then return err end
  state = st
end

if seq ~= '' and m[4] and seq <= m[4] then return {'seq_conflict', m[4]} end

local id = mint_id(scheme, currentId, now, #data)
redis.call('XADD', log, id, 'd', data, 't', now)

local fields = {'currentId', id, 'lastAccessedAt', now}
if seq ~= '' then
  fields[#fields + 1] = 'lastSeq'
  fields[#fields + 1] = seq
end
if close then
  fields[#fields + 1] = 'closed'
  fields[#fields + 1] = '1'
  if pid ~= '' then
    fields[#fields + 1] = 'closedBy'
    fields[#fields + 1] = cjson.encode({producerId = pid, epoch = epoch, seq = pseq})
  end
end
redis.call('HSET', meta, unpack(fields))
if state then
  redis.call('HSET', producers, pid, cjson.encode({epoch = state.epoch, lastSeq = state.lastSeq, lastUpdated = tonumber(now)}))
end
arm_expiry(meta, log, producers)
redis.call('PUBLISH', channel, path)
return {'ok', id, state and state.isNew or -1}
`)

/**
 * KEYS: meta, log, producers
 * ARGV: now, producerId, producerEpoch, producerSeq, channel, path
 */
export const CLOSE_SCRIPT: string = withPrelude(`
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local now, pid, channel, path = ARGV[1], ARGV[2], ARGV[5], ARGV[6]
local epoch, pseq = tonumber(ARGV[3]), tonumber(ARGV[4])

if redis.call('EXISTS', meta) == 0 then return {'not_found'} end
local m = redis.call('HMGET', meta, 'currentId', 'closed', 'closedBy', 'softDeleted')
if m[4] == '1' then return {'soft_deleted'} end
local currentId = m[1]

if m[2] == '1' then
  if pid ~= '' then
    if closed_by_matches(m[3], pid, epoch, pseq) then
      return {'closed_duplicate', currentId, pseq}
    end
    return {'stream_closed', currentId}
  end
  return {'already_closed', currentId}
end

if pid ~= '' then
  local err, st = validate_producer(producers, pid, epoch, pseq)
  if err then return err end
  redis.call('HSET', producers, pid, cjson.encode({epoch = st.epoch, lastSeq = st.lastSeq, lastUpdated = tonumber(now)}))
  redis.call('HSET', meta, 'closed', '1', 'lastAccessedAt', now,
    'closedBy', cjson.encode({producerId = pid, epoch = epoch, seq = pseq}))
else
  redis.call('HSET', meta, 'closed', '1', 'lastAccessedAt', now)
end
arm_expiry(meta, log, producers)
redis.call('PUBLISH', channel, path)
return {'ok', currentId}
`)

/**
 * KEYS: meta, log, producers, sourceMeta, sourceLog, sourceProducers
 *       (source keys are empty strings for non-fork creates)
 * ARGV: now, path, contentType, ttlSeconds, expiresAt, expiresAtMs, closed,
 *       forkedFrom, forkId, forkOffset, forkSubOffset, prefixData, initialData, channel, scheme
 */
export const CREATE_SCRIPT: string = withPrelude(`
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local srcMeta, srcLog, srcProducers = KEYS[4], KEYS[5], KEYS[6]
local now, path, contentType = ARGV[1], ARGV[2], ARGV[3]
local ttlSeconds, expiresAt, expiresAtMs, closed = ARGV[4], ARGV[5], ARGV[6], ARGV[7]
local forkedFrom, forkId, forkOffset, forkSubOffset = ARGV[8], ARGV[9], ARGV[10], ARGV[11]
local prefixData, initialData, channel, scheme = ARGV[12], ARGV[13], ARGV[14], ARGV[15]

if redis.call('EXISTS', meta) == 1 then return {'exists'} end

local startId = '0-0'
if forkedFrom ~= '' then
  if redis.call('EXISTS', srcMeta) == 0 then return {'source_not_found'} end
  local s = redis.call('HMGET', srcMeta, 'softDeleted', 'currentId')
  if s[1] == '1' then return {'source_soft_deleted'} end
  startId = forkId
  if id_lt(s[2], startId) then return {'invalid_fork_offset'} end
  redis.call('HINCRBY', srcMeta, 'refCount', 1)
  redis.call('PERSIST', srcMeta)
  redis.call('PERSIST', srcLog)
  redis.call('PERSIST', srcProducers)
end

local fields = {'path', path, 'currentId', startId,
  'createdAt', now, 'lastAccessedAt', now, 'refCount', '0'}
local function add(name, value)
  if value ~= '' then
    fields[#fields + 1] = name
    fields[#fields + 1] = value
  end
end
add('contentType', contentType)
add('ttlSeconds', ttlSeconds)
add('expiresAt', expiresAt)
add('expiresAtMs', expiresAtMs)
add('forkedFrom', forkedFrom)
add('forkOffset', forkOffset)
add('forkSubOffset', forkSubOffset)
redis.call('HSET', meta, unpack(fields))

local id = startId
if #prefixData > 0 then
  id = mint_id(scheme, id, now, #prefixData)
  redis.call('XADD', log, id, 'd', prefixData, 't', now)
end
if #initialData > 0 then
  id = mint_id(scheme, id, now, #initialData)
  redis.call('XADD', log, id, 'd', initialData, 't', now)
end
if id ~= startId then
  redis.call('HSET', meta, 'currentId', id)
end
if closed == '1' then redis.call('HSET', meta, 'closed', '1') end
arm_expiry(meta, log, producers)
if id ~= startId then redis.call('PUBLISH', channel, path) end
return {'ok', id}
`)

/**
 * Deletes a stream, soft-deleting it when forks still reference it, and
 * cascades to soft-deleted parents whose reference count drops to zero.
 * Parent keys are derived from the prefix because the cascade is dynamic.
 *
 * KEYS: (none)
 * ARGV: prefix, path, channel
 */
export const DELETE_SCRIPT: string = withPrelude(`
local prefix, path, channel = ARGV[1], ARGV[2], ARGV[3]

local function keys_for(p)
  return prefix .. 'meta:' .. p, prefix .. 'log:' .. p, prefix .. 'producers:' .. p
end

local delete_cascade
delete_cascade = function(p)
  local meta, log, producers = keys_for(p)
  if redis.call('EXISTS', meta) == 0 then return end
  local forkedFrom = redis.call('HGET', meta, 'forkedFrom')
  redis.call('DEL', meta, log, producers)
  redis.call('PUBLISH', channel, p)
  if forkedFrom then
    local pMeta, pLog, pProducers = keys_for(forkedFrom)
    if redis.call('EXISTS', pMeta) == 1 then
      local rc = redis.call('HINCRBY', pMeta, 'refCount', -1)
      if rc < 0 then
        redis.call('HSET', pMeta, 'refCount', '0')
        rc = 0
      end
      if rc == 0 then
        if redis.call('HGET', pMeta, 'softDeleted') == '1' then
          delete_cascade(forkedFrom)
        else
          arm_expiry(pMeta, pLog, pProducers)
        end
      end
    end
  end
end

local meta = keys_for(path)
if redis.call('EXISTS', meta) == 0 then return {'not_found'} end
local m = redis.call('HMGET', meta, 'softDeleted', 'refCount')
if m[1] == '1' then return {'ok'} end
if tonumber(m[2] or '0') > 0 then
  redis.call('HSET', meta, 'softDeleted', '1')
  redis.call('PUBLISH', channel, path)
  return {'ok'}
end
delete_cascade(path)
return {'ok'}
`)

/**
 * KEYS: meta, log, producers
 * ARGV: now
 */
export const TOUCH_SCRIPT: string = withPrelude(`
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
if redis.call('EXISTS', meta) == 0 then return 0 end
redis.call('HSET', meta, 'lastAccessedAt', ARGV[1])
arm_expiry(meta, log, producers)
return 1
`)

/**
 * Marks an expired stream that still has forks as soft-deleted.
 * KEYS: meta
 */
export const SOFT_DELETE_SCRIPT: string = withPrelude(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'softDeleted', '1')
return 1
`)

function withPrelude(body: string): string {
  return PRELUDE + body
}
