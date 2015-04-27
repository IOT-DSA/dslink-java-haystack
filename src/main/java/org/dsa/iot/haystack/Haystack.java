package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.projecthaystack.*;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
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

    private ScheduledFuture<?> connectFuture;
    private ScheduledFuture<?> pollFuture;

    private HClient client;
    private HWatch watch;

    Haystack(Node node) {
        this.node = node;
        this.subs = new HashMap<>();
        this.helper = new NavHelper(this);
    }

    synchronized void connect() {
        if (connectFuture != null) {
            connectFuture.cancel(false);
            connectFuture = null;
        }
        String url = node.getConfig("url").getString();
        try {
            if (client == null) {
                String username = node.getConfig("username").getString();
                char[] password = node.getPassword();
                client = HClient.open(url, username, String.valueOf(password));
                watch = client.watchOpen("Haystack DSLink", null);
                if (!subs.isEmpty()) {
                    // Restore haystack subscriptions
                    for (Map.Entry<String, Node> entry : subs.entrySet()) {
                        HRef id = HRef.make(entry.getKey());
                        Node node = entry.getValue();
                        subscribe(id, node);
                    }
                }
                pollFuture = Objects.getDaemonThreadPool().scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            poll();
                        } catch (Exception e) {
                            pollFuture.cancel(false);
                            client = null;
                            pollFuture = null;
                            synchronized (Haystack.this) {
                                watch = null;
                            }
                            scheduleReconnect();
                        }
                    }
                }, 5, 5, TimeUnit.SECONDS);
                LOGGER.info("Opened Haystack connection to {}", url);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to open Haystack connection to {}", url);
            scheduleReconnect();
        }
    }

    private synchronized void scheduleReconnect() {
        LOGGER.warn("Reconnection to haystack server scheduled");
        connectFuture = Objects.getDaemonThreadPool().schedule(new Runnable() {
            @Override
            public void run() {
                connect();
            }
        }, 5, TimeUnit.SECONDS);
    }

    HGrid call(String op, HGrid grid) {
        if (ensureConnected()) {
            return client.call(op, grid);
        }
        return null;
    }

    HGrid eval(String expr) {
        if (ensureConnected()) {
            return client.eval(expr);
        }
        return null;
    }

    NavHelper getHelper() {
        return helper;
    }

    void subscribe(HRef id, Node node) {
        subscribe(id, node, true);
    }

    private synchronized void subscribe(HRef id, Node node, boolean add) {
        if (add) {
            subs.put(id.toString(), node);
        }
        if (ensureConnected()) {
            try {
                watch.sub(new HRef[]{id});
            } catch (Exception e) {
                LOGGER.error("Failed to subscribe", e);
            }
        }
    }

    synchronized void unsubscribe(HRef id) {
        subs.remove(id.toString());
        if (ensureConnected()) {
            try {
                watch.unsub(new HRef[]{id});
            } catch (Exception e) {
                LOGGER.error("Failed to unsubscribe", e);
            }
        }
    }

    boolean ensureConnected() {
        if (!isConnected()) {
            connect();
        }
        return isConnected();
    }

    private boolean isConnected() {
        return client != null;
    }

    private void poll() {
        synchronized (lock) {
            if (subs.isEmpty()) {
                return;
            }

            HGrid grid;
            synchronized (this) {
                grid = watch.pollChanges();
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
                            child.setValue(value);
                        } else {
                            NodeBuilder b = node.createChild(filtered);
                            b.setValue(value);
                            b.build();
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
