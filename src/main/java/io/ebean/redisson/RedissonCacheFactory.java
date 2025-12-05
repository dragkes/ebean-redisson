package io.ebean.redisson;


import io.avaje.applog.AppLog;
import io.ebean.BackgroundExecutor;
import io.ebean.DatabaseBuilder;
import io.ebean.cache.*;
import io.ebean.redisson.encode.EncodeBeanData;
import io.ebean.redisson.encode.EncodeManyIdsData;
import io.ebean.redisson.encode.EncodeSerializable;
import io.ebean.redisson.near.NearCacheInvalidate;
import io.ebean.redisson.near.NearCacheNotify;
import io.ebean.redisson.topic.DaemonTopicRunner;
import io.ebean.meta.MetricVisitor;
import io.ebean.metric.MetricFactory;
import io.ebean.metric.TimedMetric;
import io.ebeaninternal.server.cache.DefaultServerCache;
import io.ebeaninternal.server.cache.DefaultServerCacheConfig;
import io.ebeaninternal.server.cache.DefaultServerQueryCache;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.Logger.Level.*;
import static java.util.Arrays.asList;

public class RedissonCacheFactory implements ServerCacheFactory {

    private static final System.Logger log = AppLog.getLogger(RedissonCacheFactory.class);

    private static final int MSG_NEARCACHE_CLEAR = 1;
    private static final int MSG_NEARCACHE_KEYS = 2;
    private static final int MSG_NEARCACHE_KEY = 3;

    /**
     * Channel for standard L2 cache messages.
     */
    private static final String CHANNEL_L2 = "ebean.l2cache";

    /**
     * Channel specifically for near cache invalidation messages.
     */
    private static final String CHANNEL_NEAR = "ebean.l2near";

    private final ConcurrentHashMap<String, RQueryCache> queryCaches = new ConcurrentHashMap<>();
    private final Map<String, NearCacheInvalidate> nearCacheMap = new ConcurrentHashMap<>();
    private final EncodeManyIdsData encodeManyIdsData = new EncodeManyIdsData();
    private final EncodeBeanData encodeBeanData = new EncodeBeanData();
    private final EncodeSerializable encodeSerializable = new EncodeSerializable();
    private final BackgroundExecutor executor;
    private final RedissonClient redissonClient;
    private final NearCacheNotify nearCacheNotify;
    private final TimedMetric metricOutNearCache;
    private final TimedMetric metricOutTableMod;
    private final TimedMetric metricOutQueryCache;
    private final TimedMetric metricInNearCache;
    private final TimedMetric metricInTableMod;
    private final TimedMetric metricInQueryCache;
    private final String serverId = ModId.id();
    private final ReentrantLock lock = new ReentrantLock();
    private ServerCacheNotify listener;

    RedissonCacheFactory(DatabaseBuilder.Settings config, BackgroundExecutor executor) {
        this.executor = executor;
        this.nearCacheNotify = new DNearCacheNotify();
        MetricFactory factory = MetricFactory.get();
        this.metricOutTableMod = factory.createTimedMetric("l2a.outTableMod");
        this.metricOutQueryCache = factory.createTimedMetric("l2a.outQueryCache");
        this.metricOutNearCache = factory.createTimedMetric("l2a.outNearKeys");
        this.metricInTableMod = factory.createTimedMetric("l2a.inTableMod");
        this.metricInQueryCache = factory.createTimedMetric("l2a.inQueryCache");
        this.metricInNearCache = factory.createTimedMetric("l2a.inNearKeys");
        this.redissonClient = getRedissonClient(config);
        new DaemonTopicRunner(redissonClient, new CacheDaemonTopic()).run();
    }

    private RedissonClient getRedissonClient(DatabaseBuilder.Settings config) {
        RedissonClient client = config.getServiceObject(RedissonClient.class);
        if (client != null) {
            return client;
        }

        Config redisConfig = config.getServiceObject(Config.class);
        if (redisConfig == null) {
            redisConfig = new Config();
            redisConfig.useSingleServer().setAddress("redis://localhost:6379");
            log.log(WARNING, "using default Redisson config with localhost:6379");
        }
        return Redisson.create(redisConfig);
    }

    @Override
    public void visit(MetricVisitor visitor) {
        metricOutQueryCache.visit(visitor);
        metricOutTableMod.visit(visitor);
        metricOutNearCache.visit(visitor);
        metricInTableMod.visit(visitor);
        metricInQueryCache.visit(visitor);
        metricInNearCache.visit(visitor);
    }

    @Override
    public ServerCache createCache(ServerCacheConfig config) {
        if (config.isQueryCache()) {
            return createQueryCache(config);
        }
        return createNormalCache(config);
    }

    private ServerCache createNormalCache(ServerCacheConfig config) {
        RedissonCache redissonCache = createRedisCache(config);
        boolean nearCache = config.getCacheOptions().isNearCache();
        if (!nearCache) {
            return config.tenantAware(redissonCache);
        }

        String cacheKey = config.getCacheKey();
        DefaultServerCache near = new DefaultServerCache(new DefaultServerCacheConfig(config));
        near.periodicTrim(executor);
        DuelCache duelCache = new DuelCache(near, redissonCache, cacheKey, nearCacheNotify);
        nearCacheMap.put(cacheKey, duelCache);
        return config.tenantAware(duelCache);
    }

    private RedissonCache createRedisCache(ServerCacheConfig config) {
        return switch (config.getType()) {
            case NATURAL_KEY -> new RedissonCache(redissonClient, config, encodeSerializable);
            case BEAN -> new RedissonCache(redissonClient, config, encodeBeanData);
            case COLLECTION_IDS -> new RedissonCache(redissonClient, config, encodeManyIdsData);
            default -> throw new IllegalArgumentException("Unexpected cache type? " + config.getType());
        };
    }

    private ServerCache createQueryCache(ServerCacheConfig config) {
        lock.lock();
        try {
            RQueryCache cache = queryCaches.get(config.getCacheKey());
            if (cache == null) {
                log.log(DEBUG, config.getCacheKey());
                cache = new RQueryCache(new DefaultServerCacheConfig(config));
                cache.periodicTrim(executor);
                queryCaches.put(config.getCacheKey(), cache);
            }
            return config.tenantAware(cache);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ServerCacheNotify createCacheNotify(ServerCacheNotify listener) {
        this.listener = listener;
        return new RServerCacheNotify();
    }

    private void sendQueryCacheInvalidation(String name) {
        long nanos = System.nanoTime();
        try {
            RTopic topic = redissonClient.getTopic(CHANNEL_L2);
            topic.publish(serverId + ":queryCache:" + name);
        } finally {
            metricOutQueryCache.addSinceNanos(nanos);
        }
    }


    private void sendTableMod(String formattedMsg) {
        long nanos = System.nanoTime();
        try {
            RTopic topic = redissonClient.getTopic(CHANNEL_L2);
            topic.publish(serverId + ":tableMod:" + formattedMsg);
        } finally {
            metricOutTableMod.addSinceNanos(nanos);
        }
    }

    /**
     * Clear the query cache if we have it.
     */
    private void queryCacheInvalidate(String key) {
        long nanos = System.nanoTime();
        try {
            RQueryCache queryCache = queryCaches.get(key);
            if (queryCache != null) {
                queryCache.invalidate();
            }
        } finally {
            metricInQueryCache.addSinceNanos(nanos);
        }
    }

    /**
     * Process a remote dependent table modify event.
     */
    private void processTableNotify(String rawMessage) {
        long nanos = System.nanoTime();
        try {
            Set<String> tables = new HashSet<>(asList(rawMessage.split(",")));
            listener.notify(new ServerCacheNotification(tables));
        } finally {
            metricInTableMod.addSinceNanos(nanos);
        }
    }

    /**
     * Invalidate key for a local near cache.
     */
    private void nearCacheInvalidateKey(String cacheKey, Object key) {
        NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
        if (invalidate == null) {
            warnNearCacheNotFound(cacheKey);
        } else {
            invalidate.invalidateKey(key);
        }
    }

    /**
     * Invalidate keys for a local near cache.
     */
    private void nearCacheInvalidateKeys(String cacheKey, Set<Object> keys) {
        NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
        if (invalidate == null) {
            warnNearCacheNotFound(cacheKey);
        } else {
            invalidate.invalidateKeys(keys);
        }
    }

    /**
     * Invalidate clear for a local near cache.
     */
    private void nearCacheInvalidateClear(String cacheKey) {
        NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
        if (invalidate == null) {
            warnNearCacheNotFound(cacheKey);
        } else {
            invalidate.invalidateClear();
        }
    }

    private void warnNearCacheNotFound(String cacheKey) {
        log.log(WARNING, "No near cache found for cacheKey [" + cacheKey + "] yet - probably on startup");
    }

    /**
     * Query cache implementation using Redis channel for message notifications.
     */
    private class RQueryCache extends DefaultServerQueryCache {

        RQueryCache(DefaultServerCacheConfig config) {
            super(config);
        }

        @Override
        public void clear() {
            super.clear();
            sendQueryCacheInvalidation(name);
        }

        /**
         * Process the invalidation message coming from the cluster.
         */
        private void invalidate() {
//            log.debug("CLEAR {}(*) - cluster invalidate", name);
            super.clear();
        }
    }

    /**
     * Publish table modifications using Redis channel (to other cluster members)
     */
    private class RServerCacheNotify implements ServerCacheNotify {

        @Override
        public void notify(ServerCacheNotification tableModifications) {
            Set<String> dependentTables = tableModifications.getDependentTables();
            if (dependentTables != null && !dependentTables.isEmpty()) {
                StringBuilder msg = new StringBuilder(50);
                for (String table : dependentTables) {
                    msg.append(table).append(',');
                }
                String formattedMsg = msg.toString();
                sendTableMod(formattedMsg);
            }
        }
    }

    /**
     * Near cache notification using a specific Redis channel (CHANNEL_NEAR).
     */
    private class DNearCacheNotify implements NearCacheNotify {

        @Override
        public void invalidateKeys(String cacheKey, Set<Object> keySet) {
            try {
                sendMessage(messageInvalidateKeys(cacheKey, keySet));
            } catch (IOException e) {
                log.log(ERROR, "failed to transmit invalidateKeys() message", e);
            }
        }

        @Override
        public void invalidateKey(String cacheKey, Object id) {
            try {
                sendMessage(messageInvalidateKey(cacheKey, id));
            } catch (IOException e) {
                log.log(ERROR, "failed to transmit invalidateKeys() message", e);
            }
        }

        @Override
        public void invalidateClear(String cacheKey) {
            try {
                sendMessage(messageInvalidateClear(cacheKey));
            } catch (IOException e) {
                log.log(ERROR, "failed to transmit invalidateKeys() message", e);
            }
        }

        private void sendMessage(byte[] message) {
            long nanos = System.nanoTime();
            try {
                RTopic topic = redissonClient.getTopic(CHANNEL_NEAR);
                topic.publish(message);
            } finally {
                metricOutNearCache.addSinceNanos(nanos);
            }
        }


        private byte[] messageInvalidateKeys(String cacheKey, Set<Object> keySet) throws IOException {
            ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
            ObjectOutputStream os = new ObjectOutputStream(ba);
            os.writeUTF(serverId);
            os.writeInt(MSG_NEARCACHE_KEYS);
            os.writeUTF(cacheKey);
            os.writeInt(keySet.size());
            for (Object key : keySet) {
                os.writeObject(key);
            }
            os.flush();
            os.close();
            return ba.toByteArray();
        }

        private byte[] messageInvalidateKey(String cacheKey, Object id) throws IOException {
            ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
            ObjectOutputStream os = new ObjectOutputStream(ba);
            os.writeUTF(serverId);
            os.writeInt(MSG_NEARCACHE_KEY);
            os.writeUTF(cacheKey);
            os.writeObject(id);
            os.flush();
            os.close();
            return ba.toByteArray();
        }

        private byte[] messageInvalidateClear(String cacheKey) throws IOException {
            ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
            ObjectOutputStream os = new ObjectOutputStream(ba);
            os.writeUTF(serverId);
            os.writeInt(MSG_NEARCACHE_CLEAR);
            os.writeUTF(cacheKey);
            os.flush();
            os.close();
            return ba.toByteArray();
        }
    }

    /**
     * Redis channel listener that supports reconnection etc.
     */
    private class CacheDaemonTopic implements DaemonTopicRunner.DaemonTopic {

        @Override
        public void subscribe(RedissonClient redissonClient) {
            // Subscribe to the channels using Redisson's RTopic
            RTopic topicL2 = redissonClient.getTopic(CHANNEL_L2);
            RTopic topicNear = redissonClient.getTopic(CHANNEL_NEAR);

            ChannelSubscriber channelSubscriber = new ChannelSubscriber();

            // Adding listeners for the channels
            topicL2.addListener(String.class, (channel, message) -> {
                channelSubscriber.processL2Message(message);
            });

            topicNear.addListener(byte[].class, (channel, message) -> {
                channelSubscriber.processNearCacheMessage(message);
            });
        }

        @Override
        public void notifyConnected() {
            log.log(INFO, "Established connection to Redis");
        }

        /**
         * Handles updates to the features (via redis topic notifications).
         */
        private class ChannelSubscriber {

            public ChannelSubscriber() {
                // Subscribe to the channel
                RTopic topic = redissonClient.getTopic(CHANNEL_NEAR);
                topic.addListener(String.class, this::onMessage);
            }

            private void onMessage(CharSequence channel, String message) {
                String channelName = channel.toString();
                if (channelName.equals(CHANNEL_L2)) {
                    processL2Message(message);
                } else {
                    processNearCacheMessage(message.getBytes(StandardCharsets.UTF_8));
                }
            }

            private void processNearCacheMessage(byte[] message) {
                long nanos = System.nanoTime();
                int msgType = 0;
                String cacheKey = null;
                try (ObjectInputStream oi = new ObjectInputStream(new ByteArrayInputStream(message))) {
                    String sourceServerId = oi.readUTF();
                    if (sourceServerId.equals(serverId)) {
                        // ignore this message as we are the server that sent it
                        return;
                    }
                    msgType = oi.readInt();
                    cacheKey = oi.readUTF();

                    switch (msgType) {
                        case MSG_NEARCACHE_CLEAR:
                            nearCacheInvalidateClear(cacheKey);
                            break;

                        case MSG_NEARCACHE_KEY:
                            Object key = oi.readObject();
                            nearCacheInvalidateKey(cacheKey, key);
                            break;

                        case MSG_NEARCACHE_KEYS:
                            int count = oi.readInt();
                            Set<Object> keys = new LinkedHashSet<>();
                            for (int i = 0; i < count; i++) {
                                keys.add(oi.readObject());
                            }
                            nearCacheInvalidateKeys(cacheKey, keys);
                            break;

                        default:
                            throw new IllegalStateException("Unexpected message type ? " + msgType);
                    }

                } catch (IOException | ClassNotFoundException e) {
                    log.log(ERROR, "failed to decode near cache message [" + new String(message, StandardCharsets.UTF_8) + "] for cache:" + cacheKey, e);
                    if (cacheKey != null) {
                        nearCacheInvalidateClear(cacheKey);
                    }
                } finally {
                    if (msgType != 0) {
                        metricInNearCache.addSinceNanos(nanos);
                    }
                }
            }

            private void processL2Message(String message) {
                try {
                    String[] split = message.split(":");
                    if (serverId.equals(split[0])) {
                        // ignore this message as we are the server that sent it
                        return;
                    }
                    switch (split[1]) {
                        case "tableMod":
                            processTableNotify(split[2]);
                            break;
                        case "queryCache":
                            queryCacheInvalidate(split[2]);
                            break;
                        default:
                            log.log(ERROR, "Unknown L2 message type[{}] on redis channel - message[{}] ", split[0], message);
                    }
                } catch (Exception e) {
                    log.log(ERROR, "Error handling L2 message[" + message + "]", e);
                }
            }
        }

    }

}
