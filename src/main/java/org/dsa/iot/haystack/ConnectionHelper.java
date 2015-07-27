package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.util.Objects;
import org.projecthaystack.HGrid;
import org.projecthaystack.HWatch;
import org.projecthaystack.client.CallHttpException;
import org.projecthaystack.client.CallNetworkException;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class ConnectionHelper {

    private static final Logger LOGGER;

    private final Queue<Handler<HClient>> queue = new ConcurrentLinkedQueue<>();
    private final Object lock = new Object();

    private final Handler<HWatch> watchEnabled;
    private final Handler<Void> watchDisabled;

    private volatile String username;
    private volatile char[] password;
    private volatile String url;

    private ScheduledFuture<?> connectFuture;
    private HClient client;
    private HWatch watch;

    public ConnectionHelper(Node node,
                            Handler<HWatch> watchEnabled,
                            Handler<Void> watchDisabled) {
        this.watchEnabled = watchEnabled;
        this.watchDisabled = watchDisabled;

        username = node.getConfig("username").getString();
        password = node.getPassword();
        url = node.getConfig("url").getString();
    }

    public void editConnection(String url, String user, String pass) {
        close();
        this.url = url;
        this.username = user;
        if (pass != null) {
            this.password = pass.toCharArray();
        }
        getClient(null);
    }

    public void close() {
        synchronized (lock) {
            if (connectFuture != null) {
                connectFuture.cancel(false);
                connectFuture = null;
            }

            if (watch != null) {
                try {
                    watch.close();
                } catch (Exception ignored) {
                } finally {
                    watch = null;
                }
            }

            client = null;
        }
    }

    public void getClient(Handler<HClient> onClientReceived) {
        try {
            connect(onClientReceived);
        } catch (CallNetworkException cne) {
            close();
            Throwable t = cne.getCause();
            if (t instanceof CallHttpException) {
                String s = t.getMessage();
                if (s.startsWith("303")) {
                    LOGGER.debug("303 error, reconnecting to {}", url);
                    getClient(onClientReceived);
                }
            }
        }
    }

    private void connect(Handler<HClient> onConnected) {
        synchronized (lock) {
            if (connectFuture == null && client != null) {
                if (onConnected != null) {
                    onConnected.handle(client);
                }
                return;
            } else if (connectFuture != null) {
                if (onConnected != null) {
                    queue.add(onConnected);
                }
                return;
            }
            close();
            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            Connector c = new Connector(onConnected);
            TimeUnit u = TimeUnit.SECONDS;
            connectFuture = stpe.scheduleWithFixedDelay(c, 0, 5, u);
        }
    }

    private class Connector implements Runnable {

        private final Handler<HClient> onConnected;

        public Connector(Handler<HClient> onConnected) {
            this.onConnected = onConnected;
        }

        @Override
        public void run() {
            try {
                String pass = String.valueOf(password);
                synchronized (lock) {
                    client = HClient.open(url, username, pass);
                    connectFuture.cancel(false);
                    connectFuture = null;

                    HGrid grid = client.ops();
                    Set<String> ops = new HashSet<>();
                    for (int i = 0; i < grid.numRows(); ++i) {
                        ops.add(grid.row(i).get("name").toString());
                    }

                    if (watchEnabled != null && watchDisabled != null) {
                        boolean supportsWatch = ops.contains("watchSub");
                        if (supportsWatch) {
                            watch = client.watchOpen("DSLink Haystack", null);
                            watchEnabled.handle(watch);
                        } else {
                            watchDisabled.handle(null);
                            LOGGER.warn("watchSub disabled for {}", url);
                        }
                    }

                    Handler<HClient> handler;
                    while ((handler = queue.poll()) != null) {
                        handler.handle(client);
                    }
                }
                if (onConnected != null) {
                    onConnected.handle(client);
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Unable to connect to {}", url);
            }
        }
    }

    static {
        LOGGER = LoggerFactory.getLogger(ConnectionHelper.class);
    }
}
