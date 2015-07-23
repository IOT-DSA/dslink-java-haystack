package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.projecthaystack.*;
import org.projecthaystack.client.CallNetworkException;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class Haystack {

    private static final Logger LOGGER;

    private final Object lock = new Object();
    private final Map<String, Node> subs;
    private final NavHelper helper;
    private final Node node;

    private final ScheduledThreadPoolExecutor stpe;
    private ScheduledFuture<?> pollFuture;
    private HClient client;

    private boolean watchEnabled;
    private HWatch watch;

    Haystack(Node node) {
        node.setMetaData(this);
        this.stpe = Objects.createDaemonThreadPool();
        this.node = node;
        this.subs = new HashMap<>();
        this.helper = new NavHelper(this);
    }


    void connect() {
        String url = node.getConfig("url").getString();
        try {
            if (client == null) {
                String username = node.getConfig("username").getString();
                char[] password = node.getPassword();
                client = HClient.open(url, username, String.valueOf(password));

                {
                    HGrid grid = client.ops();
                    Set<String> ops = new HashSet<>();
                    for (int i = 0; i < grid.numRows(); ++i) {
                        ops.add(grid.row(i).get("name").toString());
                    }

                    watchEnabled = ops.contains("watchSub");
                }

                if (watchEnabled) {
                    watch = client.watchOpen("Haystack DSLink", null);
                    if (!subs.isEmpty()) {
                        // Restore haystack subscriptions
                        for (Map.Entry<String, Node> entry : subs.entrySet()) {
                            HRef id = HRef.make(entry.getKey());
                            Node node = entry.getValue();
                            subscribe(id, node);
                        }
                    }


                    pollFuture = stpe.scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                poll();
                            } catch (Exception e) {
                                pollFuture.cancel(false);
                                client = null;
                                pollFuture = null;
                                watch = null;
                            }
                        }
                    }, 5, 5, TimeUnit.SECONDS);
                } else {
                    LOGGER.warn("watchSub is disabled for this server");
                }
                LOGGER.info("Opened Haystack connection to {}", url);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to open Haystack connection to {}", url);
        }
    }

    private void reconnect() {
        LOGGER.warn("Reconnecting");
        stop();
        connect();
    }

    HGrid call(String op, HGrid grid) {
        if (ensureConnected()) {
            try {
                return client.call(op, grid);
            } catch (CallNetworkException e) {
                reconnect();
                return client.call(op, grid);
            }
        }
        return null;
    }

    HGrid read(String filter, int limit) {
        if (ensureConnected()) {
            try {
                return client.readAll(filter, limit);
            } catch (CallNetworkException e) {
                reconnect();
                return client.readAll(filter, limit);
            }
        }
        return null;
    }

    HGrid eval(String expr) {
        if (ensureConnected()) {
            try {
                return client.eval(expr);
            } catch (CallNetworkException e) {
                reconnect();
                return client.eval(expr);
            }
        }
        return null;
    }

    NavHelper getHelper() {
        return helper;
    }

    void subscribe(HRef id, Node node) {
        subscribe(id, node, true);
    }

    private void subscribe(HRef id, Node node, boolean add) {
        if (!watchEnabled) {
            return;
        }
        if (add) {
            subs.put(id.toString(), node);
        }
        if (ensureConnected()) {
            try {
                HWatch watch = this.watch;
                if (watch != null) {
                    watch.sub(new HRef[]{id});
                }
            } catch (Exception e) {
                if (e instanceof CallNetworkException) {
                    reconnect();
                    subscribe(id, node, add);
                } else {
                    LOGGER.error("Failed to subscribe", e);
                }
            }
        }
    }

    void unsubscribe(HRef id) {
        if (!watchEnabled) {
            return;
        }
        subs.remove(id.toString());
        if (ensureConnected()) {
            try {
                HWatch watch = this.watch;
                if (watch != null) {
                    watch.unsub(new HRef[]{id});
                }
            } catch (Exception e) {
                if (e instanceof CallNetworkException) {
                    reconnect();
                    unsubscribe(id);
                } else {
                    LOGGER.error("Failed to unsubscribe", e);
                }
            }
        }
    }

    boolean ensureConnected() {
        if (!isConnected()) {
            connect();
        }
        return isConnected();
    }

    void stop() {
        if (pollFuture != null) {
            try {
                pollFuture.cancel(true);
            } catch (Exception ignored) {
            }
        }

        client = null;
        watch = null;
    }

    void destroy() {
        stop();
        stpe.shutdownNow();
        helper.destroy();
    }

    private boolean isConnected() {
        return client != null;
    }

    private void poll() {
        synchronized (lock) {
            if (!watchEnabled || subs.isEmpty()) {
                return;
            }
            ensureConnected();

            HGrid grid = null;
            try {
                HWatch watch = this.watch;
                if (watch != null) {
                    grid = watch.pollChanges();
                }
            } catch (CallNetworkException e) {
                reconnect();
            }

            if (grid == null) {
                return;
            }

            Iterator it = grid.iterator();
            while (it.hasNext()) {
                HRow row = (HRow) it.next();
                Node node = subs.get(row.id().toString());
                if (node != null) {
                    Map<String, Node> children = node.getChildren();
                    List<String> remove = new ArrayList<>(children.keySet());

                    Iterator rowIt = row.iterator();
                    while (rowIt.hasNext()) {
                        Map.Entry entry = (Map.Entry) rowIt.next();
                        String name = (String) entry.getKey();
                        HVal val = (HVal) entry.getValue();
                        Value value = Utils.hvalToVal(val);

                        String filtered = StringUtils.filterBannedChars(name);
                        Node child = children.get(filtered);
                        if (child != null) {
                            child.setValueType(value.getType());
                            child.setValue(value);
                        } else {
                            NodeBuilder b = node.createChild(filtered);
                            b.setValueType(value.getType());
                            b.setValue(value);
                            Node n = b.build();
                            n.setSerializable(false);
                        }
                        remove.remove(filtered);
                    }

                    for (String s : remove) {
                        node.removeChild(s);
                    }
                }
            }
        }
    }

    public static void init(Node superRoot) {
        NodeBuilder builder = superRoot.createChild("addServer");
        builder.setAction(Actions.getAddServerAction(superRoot)).build();

        Map<String, Node> children = superRoot.getChildren();
        if (children != null) {
            for (Node child : children.values()) {
                if (child.getAction() == null) {
                    Haystack haystack = new Haystack(child);
                    child.clearChildren();
                    Utils.initCommon(haystack, child);
                }
            }
        }
    }

    static {
        LOGGER = LoggerFactory.getLogger(Haystack.class);
    }
}
