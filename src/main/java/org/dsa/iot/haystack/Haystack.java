package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
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

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Samuel Grenier
 */
public class Haystack {

    private static final Logger LOGGER;

    private final Node node;
    private HClient client;

    private Haystack(Node node) {
        this.node = node;
    }

    private synchronized void connect() {
        String url = node.getConfig("url").getString();
        try {
            if (client == null) {
                String username = node.getConfig("username").getString();
                char[] password = node.getPassword();
                client = HClient.open(url, username, String.valueOf(password));
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
                    listener.addOnListHandler(getRootListHandler(haystack));
                    haystack.connect();
                }
            }
        }
    }

    private static Handler<Node> getNavHandler(final Haystack haystack,
                                               final String navId) {
        return new Handler<Node>() {
            @Override
            public void handle(final Node event) {
                Objects.getDaemonThreadPool().execute(new Runnable() {
                    @Override
                    public void run() {
                        HGridBuilder builder = new HGridBuilder();
                        builder.addCol("navId");
                        builder.addRow(new HVal[]{
                                HUri.make(navId)
                        });
                        LOGGER.info("Navigating: {}", navId);

                        try {
                            HGrid nav = haystack.client.call("nav", builder.toGrid());
                            iterateNavChildren(haystack, nav, event);
                        } catch (CallErrException ignored) {
                        }
                    }
                });
            }
        };
    }

    private static Handler<Node> getRootListHandler(final Haystack haystack) {
        return new Handler<Node>() {
            @Override
            public void handle(Node event) {
                if (!haystack.isConnected()) {
                    haystack.connect();
                }
                HGrid nav = haystack.client.call("nav", HGrid.EMPTY);
                iterateNavChildren(haystack, nav, event);
            }
        };
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
                node.getListener().addOnListHandler(getRootListHandler(haystack));

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
                        HGrid rowData = row.grid();
                        JsonArray res = new JsonArray();
                        for (int i = 0; i < rowData.numRows(); i++) {
                            res.addString(rowData.row(i).toString());
                        }
                        results.addArray(res);
                    }

                    event.setUpdates(results);
                }
            }
        }, Action.InvokeMode.ASYNC);
        a.addParameter(new Parameter("filter", ValueType.STRING));
        a.addParameter(new Parameter("limit", ValueType.NUMBER));
        return a;
    }

    private static void iterateNavChildren(Haystack haystack, HGrid nav, Node node) {
        Iterator it = nav.iterator();
        while (it != null && it.hasNext()) {
            HRow row = (HRow) it.next();

            String name = filterBannedChars(row.dis());
            if (name.isEmpty() || "????".equals(name)) {
                continue;
            }

            NodeBuilder b = node.createChild(name);
            Node child = b.build();

            Iterator data = row.iterator();
            while (data.hasNext()) {
                Map.Entry rowData = (Map.Entry) data.next();

                String col = (String) rowData.getKey();
                String val = rowData.getValue().toString();

                b = child.createChild(filterBannedChars(col));
                Node n = b.build();
                n.setValue(new Value(val));
            }

            HVal navId = row.get("navId", false);
            if (navId != null) {
                String n = navId.toString();
                if (!n.isEmpty()) {
                    Handler<Node> h = getNavHandler(haystack, n);
                    child.getListener().addOnListHandler(h);
                }
            }
        }
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
