package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.projecthaystack.*;
import org.projecthaystack.client.CallErrException;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author Samuel Grenier
 */
public class Haystack {

    private static final Logger LOGGER;

    private final Object lock = new Object();
    private final Map<String, Node> subs;
    private final Node node;

    private ScheduledFuture<?> future;
    private HClient client;
    private HWatch watch;

    private Haystack(Node node) {
        this.node = node;
        this.subs = new HashMap<>();
    }

    private synchronized void connect() {
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

                                        String filtered = filterBannedChars(name);
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
                }, 5, 5, TimeUnit.SECONDS);
                LOGGER.info("Opened Haystack connection to {}", url);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to open Haystack connection to {}", url, e);
        }
    }

    private boolean isConnected() {
        return client != null;
    }

    public static void init(Node superRoot) {
        NodeBuilder builder = superRoot.createChild("addServer");
        builder.setAction(getAddServerAction(superRoot)).build();

        Map<String, Node> children = superRoot.getChildren();
        if (children != null) {
            for (Node child : children.values()) {
                if (child.getAction() == null) {
                    final Haystack haystack = new Haystack(child);
                    child.clearChildren();

                    NodeBuilder connectNode = child.createChild("connect");
                    connectNode.setAction(getConnectAction(haystack));
                    connectNode.build();

                    NodeBuilder readNode = child.createChild("read");
                    readNode.setAction(getReadAction(haystack));
                    readNode.build();

                    NodeListener listener = child.getListener();
                    listener.addOnListHandler(haystack.getNavHandler(null));
                    haystack.connect();
                }
            }
        }
    }

    private static Action getAddServerAction(final Node parent) {
        Action act = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                JsonObject params = event.getJsonIn().getObject("params");
                if (params == null) {
                    throw new IllegalArgumentException("params");
                }

                String name = params.getString("name");
                String url = params.getString("url");
                String username = params.getString("username");
                String password = params.getString("password");
                if (name == null) {
                    throw new IllegalArgumentException("name");
                } else if (url == null) {
                    throw new IllegalArgumentException("url");
                } else if (username == null) {
                    throw new IllegalArgumentException("username");
                } else if (password == null) {
                    throw new IllegalArgumentException("password");
                }

                NodeBuilder builder = parent.createChild(name);
                builder.setConfig("url", new Value(url));
                builder.setConfig("username", new Value(username));
                builder.setPassword(password.toCharArray());
                Node node = builder.build();
                Haystack haystack = new Haystack(node);
                node.getListener().addOnListHandler(haystack.getNavHandler(null));

                builder = node.createChild("connect");
                builder.setAction(getConnectAction(haystack));
                builder.build();

                haystack.connect();
            }
        });
        act.addParameter(new Parameter("name", ValueType.STRING));
        act.addParameter(new Parameter("url", ValueType.STRING));
        act.addParameter(new Parameter("username", ValueType.STRING));
        act.addParameter(new Parameter("password", ValueType.STRING));
        return act;
    }

    private Handler<Node> getNavHandler(final String navId) {
        return new Handler<Node>() {
            @Override
            public void handle(final Node event) {
                if (!isConnected()) {
                    connect();
                }

                HGrid grid = HGrid.EMPTY;
                if (navId != null) {
                    HGridBuilder builder = new HGridBuilder();
                    builder.addCol("navId");
                    builder.addRow(new HVal[]{
                            HUri.make(navId)
                    });
                    grid = builder.toGrid();
                    LOGGER.info("Navigating: {} ({})", navId, event.getPath());
                } else {
                    LOGGER.info("Navigating root");
                }

                try {
                    HGrid nav = client.call("nav", grid);
                    iterateNavChildren(nav, event);
                } catch (CallErrException ignored) {
                }
            }
        };
    }

    private void iterateNavChildren(HGrid nav, Node node) {
        Iterator navIt = nav.iterator();
        while (navIt != null && navIt.hasNext()) {
            final HRow row = (HRow) navIt.next();

            String name = getName(row);
            if (name == null) {
                continue;
            }

            final NodeBuilder builder = node.createChild(name);
            final Node child = builder.build();

            HVal navId = row.get("navId", false);
            if (navId != null) {
                String id = navId.toString();
                Handler<Node> handler = getNavHandler(id);
                child.getListener().addOnListHandler(handler);

                HGridBuilder hGridBuilder = new HGridBuilder();
                hGridBuilder.addCol("navId");
                hGridBuilder.addRow(new HVal[] { navId });
                HGrid children = client.call("nav", hGridBuilder.toGrid());
                Iterator childrenIt = children.iterator();
                while (childrenIt.hasNext()) {
                    final HRow childRow = (HRow) childrenIt.next();
                    final String childName = getName(childRow);
                    if (childName != null) {
                        Node n = child.createChild(childName).build();
                        navId = childRow.get("navId", false);
                        if (navId != null) {
                            id = navId.toString();
                            handler = getNavHandler(id);
                            n.getListener().addOnListHandler(handler);
                        }
                        iterateRow(n, childRow);
                    }
                }
            }

            iterateRow(child, row);
        }
    }

    private void iterateRow(Node node, HRow row) {
        handleRowValSubs(node, row);
        Iterator it = row.iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String name = (String) entry.getKey();
            if ("id".equals(name)) {
                continue;
            }
            String val = entry.getValue().toString();

            Node child = node.createChild(name).build();
            child.setValue(new Value(val));
        }
    }

    private void handleRowValSubs(final Node node, HRow row) {
        final HVal id = row.get("id", false);
        if (id != null) {
            Node child = node.createChild("id").build();
            NodeListener listener = child.getListener();
            listener.addOnSubscribeHandler(new Handler<Node>() {
                @Override
                public void handle(Node event) {
                    try {
                        watch.sub(new HRef[] {
                                (HRef) id
                        });
                        subs.put(id.toString(), node);
                    } catch (Exception e) {
                        LOGGER.error("Failed to subscribe", e);
                    }
                }
            });

            listener.addOnUnsubscribeHandler(new Handler<Node>() {
                @Override
                public void handle(Node event) {
                    try {
                        watch.unsub(new HRef[]{
                                (HRef) id
                        });
                    } catch (Exception e) {
                        LOGGER.error("Failed to unsubscribe", e);
                    }
                    subs.remove(id.toString());
                }
            });
        }
    }

    private String getName(HRow row) {
        String name = filterBannedChars(row.dis());
        if (name.isEmpty() || "????".equals(name)) {
            return null;
        }
        return name;
    }


    private static Action getConnectAction(final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                if (!haystack.isConnected()) {
                    haystack.connect();
                }
            }
        });
    }

    private static Action getReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                if (!haystack.isConnected()) {
                    haystack.connect();
                }

                JsonObject params = event.getJsonIn().getObject("params");
                if (params == null) {
                    throw new RuntimeException("Missing params");
                }

                String filter = params.getString("filter");
                Integer limit = params.getInteger("limit");

                if (filter == null) {
                    throw new RuntimeException("Missing filter parameter");
                }

                HGridBuilder builder = new HGridBuilder();
                builder.addCol("filter");
                {
                    HVal[] row;
                    if (limit != null) {
                        row = new HVal[]{
                                HStr.make(filter),
                                HNum.make(limit)
                        };
                        builder.addCol("limit");
                    } else {
                        row = new HVal[]{
                                HStr.make(filter)
                        };
                    }
                    builder.addRow(row);
                }
                HGrid grid = haystack.client.call("read", builder.toGrid());

                {
                    JsonArray columns = new JsonArray();
                    for (int i = 0; i < grid.numCols(); i++) {
                        JsonObject col = new JsonObject();
                        col.putString("name", grid.col(i).name());
                        col.putString("type", ValueType.STRING.toJsonString());
                        columns.addObject(col);
                    }
                    event.setColumns(columns);
                }

                {
                    JsonArray results = new JsonArray();
                    Iterator it = grid.iterator();
                    while (it.hasNext()) {
                        HRow row = (HRow) it.next();
                        JsonArray res = new JsonArray();

                        for (int x = 0; x < grid.numCols(); x++) {
                            HVal val = row.get(grid.col(x), false);
                            if (val != null) {
                                res.addString(val.toString());
                            } else {
                                res.addString(null);
                            }
                        }

                        results.addArray(res);
                    }

                    event.setUpdates(results);
                }
            }
        }, Action.InvokeMode.ASYNC);
        a.addParameter(new Parameter("filter", ValueType.STRING));
        a.addParameter(new Parameter("limit", ValueType.NUMBER));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    private static String filterBannedChars(String name) {
        for (String banned : Node.getBannedCharacters()) {
            if (name.contains(banned)) {
                name = name.replaceAll(Pattern.quote(banned), "");
            }
        }
        return name;
    }

    static {
        LOGGER = LoggerFactory.getLogger(Haystack.class);
    }
}
