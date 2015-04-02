package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;

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
        if (client == null) {
            String url = node.getConfig("url").getString();
            String username = node.getConfig("username").getString();
            char[] password = node.getPassword();
            client = HClient.open(url, username, String.valueOf(password));
            LOGGER.info("Opened connection to {}", url);
        }
    }

    public static void init(Node superRoot) {
        NodeBuilder builder = superRoot.createChild("haystack");
        Node node = builder.build();

        builder = node.createChild("addServer");
        builder.setAction(getAddServer(node));
        builder.build();

        Map<String, Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children.values()) {
                if (child.getAction() == null) {
                    Haystack haystack = new Haystack(child);
                    child.getChild("connect").setAction(getConnect(haystack));
                }
            }
        }
    }

    private static Action getAddServer(final Node parent) {
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
                builder.setAction(getConnect(haystack));
                builder.build();
            }
        });
        act.addParameter(new Parameter("name", ValueType.STRING));
        act.addParameter(new Parameter("url", ValueType.STRING));
        act.addParameter(new Parameter("username", ValueType.STRING));
        act.addParameter(new Parameter("password", ValueType.STRING));
        return act;
    }

    private static Action getConnect(final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                haystack.connect();
            }
        });
    }

    static {
        LOGGER = LoggerFactory.getLogger(Haystack.class);
    }
}
