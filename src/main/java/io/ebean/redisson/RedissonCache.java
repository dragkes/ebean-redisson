package io.ebean.redisson;

import io.avaje.applog.AppLog;
import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheConfig;
import io.ebean.cache.ServerCacheOptions;
import io.ebean.cache.ServerCacheStatistics;
import io.ebean.redisson.encode.Encode;
import io.ebean.redisson.encode.EncodePrefixKey;
import io.ebean.meta.MetricVisitor;
import io.ebean.metric.CountMetric;
import io.ebean.metric.MetricFactory;
import io.ebean.metric.TimedMetric;
import io.ebean.metric.TimedMetricStats;
import org.redisson.api.RedissonClient;

import java.time.Duration;
import java.util.*;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

public class RedissonCache implements ServerCache {

    private static final System.Logger log = AppLog.getLogger(RedissonCache.class);

    private static final String CACHE_KEY_PREFIX = "EBEAN_CACHE";

    private final RMapCachingWrapper cacheMap;
    private final String cacheKey;
    private final EncodePrefixKey keyEncode;
    private final Encode valueEncode;
    private final Duration expiration;
    private final TimedMetric metricGet;
    private final TimedMetric metricGetAll;
    private final TimedMetric metricPut;
    private final TimedMetric metricPutAll;
    private final TimedMetric metricRemove;
    private final TimedMetric metricRemoveAll;
    private final TimedMetric metricClear;
    private final CountMetric hitCount;
    private final CountMetric missCount;

    RedissonCache(RedissonClient redissonClient, ServerCacheConfig config, Encode valueEncode) {
        this.cacheKey = config.getCacheKey();
        this.keyEncode = new EncodePrefixKey(config.getCacheKey());
        this.valueEncode = valueEncode;
        this.expiration = expiration(config);
        String namePrefix = "l2r." + config.getShortName();
        MetricFactory factory = MetricFactory.get();
        hitCount = factory.createCountMetric(namePrefix + ".hit");
        missCount = factory.createCountMetric(namePrefix + ".miss");
        metricGet = factory.createTimedMetric(namePrefix + ".get");
        metricGetAll = factory.createTimedMetric(namePrefix + ".getMany");
        metricPut = factory.createTimedMetric(namePrefix + ".put");
        metricPutAll = factory.createTimedMetric(namePrefix + ".putMany");
        metricRemove = factory.createTimedMetric(namePrefix + ".remove");
        metricRemoveAll = factory.createTimedMetric(namePrefix + ".removeMany");
        metricClear = factory.createTimedMetric(namePrefix + ".clear");
        cacheMap = RMapCachingWrapper.create(redissonClient, CACHE_KEY_PREFIX + ":" + cacheKey);
    }

    private Duration expiration(ServerCacheConfig config) {
        final ServerCacheOptions cacheOptions = config.getCacheOptions();
        if (cacheOptions != null) {
            final int maxSecsToLive = cacheOptions.getMaxSecsToLive();
            if (maxSecsToLive > 0) {
                return Duration.ofSeconds(maxSecsToLive);
            }
        }
        return null;
    }

    @Override
    public void visit(MetricVisitor visitor) {
        hitCount.visit(visitor);
        missCount.visit(visitor);
        metricGet.visit(visitor);
        metricGetAll.visit(visitor);
        metricPut.visit(visitor);
        metricPutAll.visit(visitor);
        metricRemove.visit(visitor);
        metricRemoveAll.visit(visitor);
        metricClear.visit(visitor);
    }

    private byte[] key(Object id) {
        return keyEncode.encode(id);
    }

    private byte[] value(Object data) {
        if (data == null) {
            return null;
        }
        return valueEncode.encode(data);
    }

    private Object valueDecode(byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return valueEncode.decode(data);
        } catch (Exception e) {
            log.log(ERROR, "Error decoding data, treated as cache miss", e);
            return null;
        }
    }

    private List<byte[]> keysAsBytes(Collection<Object> keys) {
        return keys.stream()
            .map(this::key)
            .toList();
    }

    private void errorOnRead(Exception e) {
        log.log(WARNING, "Error when reading redis cache", e);
    }

    private void errorOnWrite(Exception e) {
        log.log(WARNING, "Error when writing redis cache", e);
    }

    @Override
    public Map<Object, Object> getAll(Set<Object> keys) {
        try {
            if (keys.isEmpty()) {
                return Collections.emptyMap();
            }
            long start = System.nanoTime();
            List<Object> keyList = new ArrayList<>(keys);
            Map<Object, Object> map = new LinkedHashMap<>();
            List<byte[]> keysBytes = keysAsBytes(keys);
            Map<byte[], byte[]> values = cacheMap.getAll(keysBytes);
            for (int i = 0; i < keysBytes.size(); i++) {
                byte[] key = keysBytes.get(i);
                if (values.containsKey(key)) {
                    map.put(keyList.get(i), valueDecode(values.get(key)));
                }
            }
            int hits = map.size();
            int miss = keys.size() - hits;

            if (hits > 0) {
                hitCount.add(hits);
            }
            if (miss > 0) {
                missCount.add(miss);
            }
            metricGetAll.addSinceNanos(start);
            return map;
        } catch (Exception e) {
            errorOnRead(e);
            return Collections.emptyMap();
        }
    }

    @Override
    public Object get(Object id) {
        long start = System.nanoTime();
        try {
            byte[] val = cacheMap.get(key(id));
            if (val != null) {
                hitCount.increment();
            } else {
                missCount.increment();
            }
            metricGet.addSinceNanos(start);
            return valueDecode(val);
        } catch (Exception e) {
            errorOnRead(e);
            return null;
        }
    }

    @Override
    public void put(Object id, Object value) {
        long start = System.nanoTime();
        try {
            if (expiration == null) {
                cacheMap.put(key(id), value(value));
            } else {
                // Set with expiration
                cacheMap.put(key(id), value(value), expiration);
            }
            metricPut.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void putAll(Map<Object, Object> keyValues) {
        long start = System.nanoTime();
        Map<byte[], byte[]> values = new HashMap<>();
        for (var entry : keyValues.entrySet()) {
            values.put(key(entry.getKey()), value(entry.getValue()));
        }
        try {
            if (expiration == null) {
                // Simple putAll without expiration
                cacheMap.putAll(values);
            } else {
                cacheMap.putAll(values, expiration);
            }
            metricPutAll.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void remove(Object id) {
        long start = System.nanoTime();
        try {
            cacheMap.remove(key(id));
            metricRemove.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void removeAll(Set<Object> keys) {
        long start = System.nanoTime();
        try {
            cacheMap.removeAll(keysAsBytes(keys));
            metricRemoveAll.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void clear() {
        long start = System.nanoTime();
        try {
            cacheMap.clear();
            metricClear.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }


    public long getHitCount() {
        return hitCount.get(false);
    }


    public long getMissCount() {
        return missCount.get(false);
    }

    @Override
    public ServerCacheStatistics statistics(boolean reset) {
        ServerCacheStatistics cacheStats = new ServerCacheStatistics();
        cacheStats.setCacheName(cacheKey);
        cacheStats.setHitCount(hitCount.get(reset));
        cacheStats.setMissCount(missCount.get(reset));
        cacheStats.setPutCount(count(metricPut.collect(reset)));
        cacheStats.setRemoveCount(count(metricRemove.collect(reset)));
        cacheStats.setClearCount(count(metricClear.collect(reset)));
        return cacheStats;
    }

    private long count(TimedMetricStats stats) {
        return stats == null ? 0 : stats.count();
    }

}
