package io.ebean.redisson;

import io.avaje.applog.AppLog;
import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheConfig;
import io.ebean.cache.ServerCacheStatistics;
import io.ebean.meta.MetricVisitor;
import io.ebean.metric.CountMetric;
import io.ebean.metric.MetricFactory;
import io.ebean.metric.TimedMetric;
import io.ebean.metric.TimedMetricStats;
import org.redisson.api.EvictionMode;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.System.Logger.Level.WARNING;

public class RedissonCache implements ServerCache {

    private static final System.Logger log = AppLog.getLogger(RedissonCache.class);

    private static final String CACHE_KEY_PREFIX = "EBEAN_CACHE";
    private final int maxIdleSecs;
    private final int maxSecsToLive;
    private final RMapCache<String, Object> cacheMap;
    private final String cacheKey;
    private final TimedMetric metricGet;
    private final TimedMetric metricGetAll;
    private final TimedMetric metricPut;
    private final TimedMetric metricPutAll;
    private final TimedMetric metricRemove;
    private final TimedMetric metricRemoveAll;
    private final TimedMetric metricClear;
    private final CountMetric hitCount;
    private final CountMetric missCount;

    RedissonCache(RedissonClient redissonClient, ServerCacheConfig config, Codec codec) {
        this.cacheKey = config.getCacheKey();

        int maxSecsToLive = config.getCacheOptions().getMaxSecsToLive();
        this.maxSecsToLive = Math.max(maxSecsToLive, 0);
        int maxIdleSecs = config.getCacheOptions().getMaxIdleSecs();
        this.maxIdleSecs = Math.max(maxIdleSecs, 0);
        int maxSize = config.getCacheOptions().getMaxSize();

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
        cacheMap = redissonClient.getMapCache(CACHE_KEY_PREFIX + ":" + cacheKey, codec);

        if (maxSize > 0) {
            cacheMap.setMaxSize(maxSize, EvictionMode.LFU);
        }
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

    private void errorOnRead(Exception e) {
        log.log(WARNING, "Error when reading redis cache", e);
        clear();
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
            List<String> keyList = keys.stream().map(Object::toString).collect(Collectors.toList());
            Map<Object, Object> map = new LinkedHashMap<>();
            Map<String, Object> values = cacheMap.getAll(new HashSet<>(keyList));
            for (int i = 0; i < keys.size(); i++) {
                String key = keyList.get(i);
                if (values.containsKey(key)) {
                    map.put(keyList.get(i), values.get(key));
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
            Object val = cacheMap.get(id.toString());
            if (val != null) {
                hitCount.increment();
            } else {
                missCount.increment();
            }
            metricGet.addSinceNanos(start);
            return val;
        } catch (Exception e) {
            errorOnRead(e);
            return null;
        }
    }

    @Override
    public void put(Object id, Object value) {
        long start = System.nanoTime();
        try {
            cacheMap.put(id.toString(), value, maxSecsToLive, TimeUnit.SECONDS, maxIdleSecs, TimeUnit.SECONDS);
            metricPut.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void putAll(Map<Object, Object> keyValues) {
        long start = System.nanoTime();
        try {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<Object, Object> entry : keyValues.entrySet()) {
                map.put(entry.getKey().toString(), entry.getValue());
            }
            if (maxIdleSecs == 0) {
                cacheMap.putAll(map, maxSecsToLive, TimeUnit.SECONDS);
            } else {
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    cacheMap.put(entry.getKey(), entry.getValue(), maxSecsToLive, TimeUnit.SECONDS, maxIdleSecs, TimeUnit.SECONDS);
                }
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
            cacheMap.remove(id.toString());
            metricRemove.addSinceNanos(start);
        } catch (Exception e) {
            errorOnWrite(e);
        }
    }

    @Override
    public void removeAll(Set<Object> keys) {
        long start = System.nanoTime();
        try {
            var keysArray = keys.stream().map(Object::toString).toArray(String[]::new);
            cacheMap.fastRemove(keysArray);
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
