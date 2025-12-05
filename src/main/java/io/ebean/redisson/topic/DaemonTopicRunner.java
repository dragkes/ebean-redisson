package io.ebean.redisson.topic;

import io.avaje.applog.AppLog;
import org.redisson.api.RedissonClient;

import java.util.Timer;
import java.util.TimerTask;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

/**
 * Subscribe to Redis topic and listen for changes.
 * <p>
 * Automatically handles reconnection if the connection is lost
 * and notifies when reconnection occurs.
 * </p>
 */
public final class DaemonTopicRunner {

    private static final System.Logger log = AppLog.getLogger(DaemonTopicRunner.class);
    private static final long reconnectWaitMillis = 1000;

    private final RedissonClient redissonClient;
    private final DaemonTopic daemonTopic;

    public DaemonTopicRunner(RedissonClient redissonClient, DaemonTopic daemonTopic) {
        this.redissonClient = redissonClient;
        this.daemonTopic = daemonTopic;
    }

    public void run() {
        new Thread(this::attemptConnection, "redis-sub").start();
    }

    private void attemptConnection() {
        Timer reloadTimer = new Timer("redis-sub-notify");
        ReloadNotifyTask notifyTask = new ReloadNotifyTask();
        reloadTimer.schedule(notifyTask, reconnectWaitMillis + 500);

        try {
            subscribe();
        } catch (Exception e) {
            log.log(ERROR, "Connection to Redis topic failed, retrying...", e);
            try {
                // wait before retrying
                Thread.sleep(reconnectWaitMillis);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                log.log(WARNING, "Interrupted Redis reconnection wait", e1);
            }
            attemptConnection();  // Retry connection
        }
    }

    /**
     * Subscribe and listen for messages. Reconnects automatically on connection loss.
     */
    private void subscribe() {
        try {
            daemonTopic.subscribe(redissonClient);
            daemonTopic.notifyConnected();
        } catch (Exception e) {
            log.log(ERROR, "Failed to subscribe to topic; retrying...", e);
            attemptConnection();  // Start reconnection loop
        }
    }

    private class ReloadNotifyTask extends TimerTask {
        @Override
        public void run() {
            daemonTopic.notifyConnected();
        }
    }

    public interface DaemonTopic {
        void subscribe(RedissonClient redissonClient);
        void notifyConnected();
    }
}
