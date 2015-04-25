package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.projecthaystack.HGrid;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HWatch;
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

    private ScheduledFuture<?> future;
    private HClient client;
    private HWatch watch;

    Haystack(Node node) {
        this.node = node;
        this.subs = new HashMap<>();
        this.helper = new NavHelper(this);
    }

    synchronized void connect() {
        String url = node.getConfig("url").getString();
        try {
            if (client == null) {
                String username = node.getConfig("username").getString();
                char[] password = node.getPassword();
                client = HClient.open(url, username, String.valueOf(password));
                watch = client.watchOpen("Haystack DSLink", null);
                if (future != null) {
                    future.cancel(false);
                }
                future = Objects.getDaemonThreadPool().scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        poll();
                    }
                }, 5, 5, TimeUnit.SECONDS);
                LOGGER.info("Opened Haystack connection to {}", url);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to open Haystack connection to {}", url, e);
        }
    }

    HGrid call(String op, HGrid grid) {
        if (!isConnected()) {
            connect();
        }
        return client.call(op, grid);
    }

    NavHelper getHelper() {
        return helper;
    }

    void subscribe(HRef id, Node node) {
        if (!isConnected()) {
            connect();
        }
        try {
            watch.sub(new HRef[]{ id });
            subs.put(id.toString(), node);
        } catch (Exception e) {
            LOGGER.error("Failed to subscribe", e);
        }
    }

    void unsubscribe(HRef id) {
        if (!isConnected()) {
            connect();
        }
        try {
            watch.unsub(new HRef[]{id});
        } catch (Exception e) {
            LOGGER.error("Failed to unsubscribe", e);
        }
        subs.remove(id.toString());
    }

    boolean isConnected() {
        return client != null;
    }

    private void poll() {
        synchronized (lock) {
            if (subs.isEmpty()) {
                return;
            }

            HGrid grid = watch.pollChanges();
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
                        String val = entry.getValue().toString();

                        String filtered = Utils.filterBannedChars(name);
                        Node child = children.get(filtered);
                        if (child != null) {
                            child.setValue(new Value(val));
                        } else {
                            NodeBuilder b = node.createChild(filtered);
                            b.setValue(new Value(val));
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
