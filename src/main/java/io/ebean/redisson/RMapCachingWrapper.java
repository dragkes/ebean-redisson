package io.ebean.redisson;

import org.redisson.api.RMapCache;
import org.redisson.api.RMapCacheNative;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisClusterMaster;
import org.redisson.api.redisnode.RedisMaster;
import org.redisson.api.redisnode.RedisNode;
import org.redisson.api.redisnode.RedisNodes;
import org.redisson.client.codec.Codec;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class RMapCachingWrapper<K, V> implements Map<K, V> {

    private static final String MINIMAL_CACHE_NATIVE_VERSION = "7.4.0";

    private RMapCacheNative<K, V> mapCacheNative;
    private RMapCache<K, V> mapCache;

    private RMapCachingWrapper() {}

    public static <K, V> RMapCachingWrapper<K, V> create(RedissonClient client, String name, Codec codec) {
        boolean nativeCache = isRedisVersionAtLeast(client, MINIMAL_CACHE_NATIVE_VERSION);
        RMapCachingWrapper<K, V> wrapper = new RMapCachingWrapper<>();

        if (nativeCache) {
            wrapper.mapCacheNative = client.getMapCacheNative(name, codec);
        } else {
            wrapper.mapCache = client.getMapCache(name, codec);
        }

        return wrapper;
    }

    public static boolean isRedisVersionAtLeast(RedissonClient client, String minimumVersion) {
        String currentVersion = getRedisVersion(client);
        return compareVersions(currentVersion, minimumVersion) >= 0;
    }

    /**
     * Compares two semantic versions (e.g., "7.4.0" vs. "7.2.5").
     *
     * @return negative if v1 < v2, zero if equal, positive if v1 > v2
     */
    private static int compareVersions(String v1, String v2) {
        if (v1 == null || v2 == null) return 0;

        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");

        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            int n1 = i < parts1.length ? parseIntSafe(parts1[i]) : 0;
            int n2 = i < parts2.length ? parseIntSafe(parts2[i]) : 0;
            if (n1 != n2) {
                return Integer.compare(n1, n2);
            }
        }
        return 0;
    }

    private static int parseIntSafe(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static String getRedisVersion(RedissonClient client) {
        if (client == null) {
            throw new IllegalArgumentException("RedissonClient must not be null");
        }

        // Try each mode in order of specificity
        Optional<String> version = Optional.empty();

        try {
            RedisMaster single = client.getRedisNodes(RedisNodes.SINGLE).getInstance();
            if (single != null && single.ping()) {
                version = extractVersion(single.info(RedisNode.InfoSection.SERVER));
            }
        } catch (Exception ignored) {}

        if (version.isEmpty()) {
            try {
                RedisMaster masterSlave = client.getRedisNodes(RedisNodes.MASTER_SLAVE).getMaster();
                if (masterSlave != null && masterSlave.ping()) {
                    version = extractVersion(masterSlave.info(RedisNode.InfoSection.SERVER));
                }
            } catch (Exception ignored) {}
        }

        if (version.isEmpty()) {
            try {
                RedisMaster sentinel = client.getRedisNodes(RedisNodes.SENTINEL_MASTER_SLAVE).getMaster();
                if (sentinel != null && sentinel.ping()) {
                    version = extractVersion(sentinel.info(RedisNode.InfoSection.SERVER));
                }
            } catch (Exception ignored) {}
        }

        if (version.isEmpty()) {
            try {
                List<RedisClusterMaster> clusters = new ArrayList<>(client.getRedisNodes(RedisNodes.CLUSTER).getMasters());
                if (!clusters.isEmpty()) {
                    RedisClusterMaster masterNode = clusters.get(0);
                    version = extractVersion(masterNode.info(RedisNode.InfoSection.SERVER));
                }
            } catch (Exception ignored) {}
        }

        return version.orElse("unknown");
    }

    private static Optional<String> extractVersion(Map<String, String> info) {
        if (info == null) return Optional.empty();

        return Optional.ofNullable(info.get("redis_version"));
    }

    private Map<K, V> getMap() {
        if (mapCacheNative != null) {
            return mapCacheNative;
        }
        return mapCache;
    }

    @Override
    public int size() {
        Map<K, V> map = getMap();
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        Map<K, V> map = getMap();
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        Map<K, V> map = getMap();
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        Map<K, V> map = getMap();
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        Map<K, V> map = getMap();
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        Map<K, V> map = getMap();
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        Map<K, V> map = getMap();
        return map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<K, V> map = getMap();
        map.putAll(m);
    }

    @Override
    public void clear() {
        Map<K, V> map = getMap();
        map.clear();
    }

    @Override
    public Set<K> keySet() {
        Map<K, V> map = getMap();
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        Map<K, V> map = getMap();
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Map<K, V> map = getMap();
        return map.entrySet();
    }

    public V put(K key, V value, Duration expiration) {
        if (mapCacheNative != null) {
            return mapCacheNative.put(key, value, expiration);
        } else {
            return mapCache.put(key, value, expiration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public V putIfAbsent(K key, V value, Duration expiration) {
        if (mapCacheNative != null) {
            return mapCacheNative.putIfAbsent(key, value, expiration);
        } else {
            return mapCache.putIfAbsent(key, value, expiration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public void putAll(Map<? extends K, ? extends V> m, Duration expiration) {
        if (mapCacheNative != null) {
            mapCacheNative.putAll(m, expiration);
        } else {
            mapCache.putAll(m, expiration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public Map<K, V> getAll(Collection<K> keys) {
        Set<K> keySet = new HashSet<>(keys);
        if (mapCacheNative != null) {
            return mapCacheNative.getAll(keySet);
        } else {
            return mapCache.getAll(keySet);
        }
    }

    public void removeAll(Collection<K> keys) {
        @SuppressWarnings("unchecked")
        K[] array = (K[]) keys.toArray(new Object[0]);
        if (mapCacheNative != null) {
            mapCacheNative.fastRemove(array);
        } else {
            mapCache.fastRemove(array);
        }
    }
}
