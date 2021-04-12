package org.dsa.iot.haystack.helpers;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.handlers.ListHandler;
import org.projecthaystack.HGrid;
import org.projecthaystack.HWatch;
import org.projecthaystack.client.CallErrException;
import org.projecthaystack.client.CallHttpException;
import org.projecthaystack.client.CallNetworkException;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class ConnectionHelper {

    private static final Logger LOGGER;

    private final Queue<Handler<HClient>> queue = new ConcurrentLinkedQueue<>();
    private final Object lock = new Object();

    private final Handler<Void> watchEnabled;
    private final Handler<Void> watchDisabled;

    private final Haystack haystack;
    private volatile String username;
    private volatile char[] password;
    private volatile String url;
    private volatile int connectTimeout;
    private volatile int readTimeout;
    private final Node statusNode;

    private ScheduledFuture<?> connectFuture;
    private HClient client;
    private HWatch watch;

    public ConnectionHelper(Haystack haystack,
                            Handler<Void> watchEnabled,
                            Handler<Void> watchDisabled) {
        this.haystack = haystack;
        this.watchEnabled = watchEnabled;
        this.watchDisabled = watchDisabled;

        Node node = haystack.getNode();
        username = node.getConfig("username").getString();
        password = node.getPassword();
        url = node.getConfig("url").getString();
        connectTimeout = (int) (node.getConfig("connect timeout").getNumber().doubleValue() * 1000);
        readTimeout = (int) (node.getConfig("read timeout").getNumber().doubleValue() * 1000);
        statusNode = Utils.getStatusNode(node);
    }

    public void editConnection(String url, String user, String pass, int connTimeout,
                               int readTimeout) {
        close();
        statusNode.setValue(new Value("Not Connected"));
        this.url = url;
        this.username = user;
        if (pass != null) {
            this.password = pass.toCharArray();
        }
        this.connectTimeout = connTimeout;
        this.readTimeout = readTimeout;
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

    public void getWatch(final StateHandler<HWatch> onWatchReceived) {
        try {
            synchronized (lock) {
                if (watch != null && !watch.isOpen()) {
                    close();
                }
                if (watch == null) {
                    getClient(new StateHandler<HClient>() {
                        @Override
                        public void handle(HClient event) {
                            synchronized (lock) {
                                if (watch != null) {
                                    return;
                                }
                                String id = "DSLink Haystack";
                                watch = client.watchOpen(id, null);
                                if (watchEnabled != null) {
                                    watchEnabled.handle(null);
                                }
                            }
                        }
                    });
                } else {
                    onWatchReceived.handle(watch);
                }
            }
        } catch (Exception e) {
            if (e instanceof CallNetworkException) {
                close();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void getClient(StateHandler<HClient> onClientReceived) {
        try {
            connect(onClientReceived);
        } catch (CallErrException cee) {
            if (onClientReceived != null && onClientReceived.incrementRetryCount() > 1) {
                throw cee;
            }
            String s = cee.getMessage();
            if (s.startsWith("proj::PermissionErr")) {
                LOGGER.debug("Permission Error, reconnecting to {}", url);
                close();
                getClient(onClientReceived);
            } else {
                throw cee;
            }
        } catch (RuntimeException x) {
            close();
            boolean rethrow = true;
            Throwable t = x.getCause();
            if (t instanceof CallHttpException) {
                String s = t.getMessage();
                if (s.startsWith("303")) {
                    LOGGER.debug("303 error, reconnecting to {}", url);
                    getClient(onClientReceived);
                    rethrow = false;
                }
            }
            if (rethrow) {
                throw x;
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
                if (!haystack.isEnabled()) {
                    close();
                    return;
                }
                statusNode.setValue(new Value("Connecting"));
                String pass = "";
                if (password != null) {
                    pass = String.valueOf(password);
                }
                synchronized (lock) {
                    client = HClient.open(url, username, pass, connectTimeout, readTimeout);
                    LOGGER.info("Opened connection to {}", url);
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
                            watchEnabled.handle(null);
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
                statusNode.setValue(new Value("Connected"));
                if (onConnected != null) {
                    onConnected.handle(client);
                }
                ListHandler.get().handle(haystack.getNode());
            } catch (CallErrException cee) {
                statusNode.setValue(new Value("Connected")); //error with call, not connection
                LOGGER.warn("Call error {} : {}", url, cee.getMessage());
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                String err = String.format("Unable to connect to %s : %s : %s", url, e.getMessage(),
                                           cause != null ? cause.getMessage() : "");
                if (haystack.isEnabled()) {
                    statusNode.setValue(new Value(err));
                }
                close();
                LOGGER.warn(err);
            }
        }
    }

    static {
        LOGGER = LoggerFactory.getLogger(ConnectionHelper.class);
    }
}
