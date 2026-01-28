-- 比较线程标识与锁中的标识是否一致
if(redis.call("get", KEYS[1]) == ARGV[1]) then
    -- 一致则删除锁，释放锁
    return redis.call("del", KEYS[1])
else
    -- 不一致则不做任何操作，直接返回0
    return 0
end