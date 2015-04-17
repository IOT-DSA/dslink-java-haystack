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
import org.projecthaystack.*;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
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
            LOGGER.error("Failed to open Haystack connection to {} {}", url, e);
        }
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

                    NodeListener listener = child.getListener();
                    listener.addOnListHandler(new Handler<Node>() {
                        @Override
                        public void handle(Node event) {
                            if (haystack.client == null) {
                                haystack.connect();
                            }
                            HGrid nav = haystack.client.call("nav", HGrid.EMPTY);
                            iterateNavChildren(haystack, nav, event);
                        }
                    });
                    haystack.connect();
                }
            }
        }
    }

    private static Handler<Node> getNavHandler(final Haystack haystack,
                                               final String navId) {
        return new Handler<Node>() {
            @Override
            public void handle(Node event) {
                HGridBuilder builder = new HGridBuilder();
                builder.addCol("navId");
                builder.addRow(new HVal[]{
                        HUri.make(navId)
                });
                LOGGER.info("Navigating: {}", navId);

                try {
                    HGrid nav = haystack.client.call("nav", builder.toGrid());
                    iterateNavChildren(haystack, nav, event);
                } catch (UnknownRecException ignored) {
                }
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
                haystack.connect();
            }
        });
    }

    private static void iterateNavChildren(Haystack haystack, HGrid nav, Node node) {
        Iterator it = nav.iterator();
        while (it != null && it.hasNext()) {
            HRow row = (HRow) it.next();

            HVal nameVal = row.get("treeName", false);
            if (nameVal == null) {
                nameVal = row.get("id", false);
                if (nameVal == null) {
                    nameVal = row.get("dis", false);
                    if (nameVal == null) {
                        nameVal = row.get("navId");
                        if (nameVal == null) {
                            continue; // Give up
                        }
                    }
                }
            }

            String name = filterBannedChars(nameVal.toString());
            if (name.isEmpty()) {
                continue;
            }

            HVal dis = row.get("dis", false);

            NodeBuilder b = node.createChild(name);
            if (dis != null) {
                b.setDisplayName(dis.toString());
            }
            b.build();

            Node child = b.build();
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
