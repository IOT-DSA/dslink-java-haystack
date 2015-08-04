package org.dsa.iot.haystack.handlers;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.SubscriptionManager;
import org.dsa.iot.dslink.node.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.util.Map;

/**
 * @author Samuel Grenier
 */
public class ClosedHandler implements Handler<Node> {

    private static final ClosedHandler HANDLER = new ClosedHandler();
    private static final Logger LOGGER;

    private ClosedHandler() {
    }

    @Override
    public void handle(Node event) {
        event.setRoConfig("lu", new Value(0));
        LOGGER.debug("Wants to remove: {}", event.getPath());
        Map<String, Node> children = event.getChildren();
        if (children == null) {
            return;
        }
        for (Node n : children.values()) {
            if (n == null
                    || (n.getValue() == null
                    && n.getAction() == null)) {
                continue;
            }
            removeNodes(n);
        }
    }

    private void removeNodes(Node node) {
        if (node == null) {
            return;
        }
        SubscriptionManager man = node.getLink().getSubscriptionManager();
        Map<String, Node> children = node.getChildren();
        if (children != null) {
            for (Node n : children.values()) {
                if (n != null) {
                    removeNodes(node);
                }
            }
        }
        if (!man.hasValueSub(node)) {
            LOGGER.debug("Removed: {}", node.getPath());
            node.getParent().removeChild(node);
        }
    }

    public static ClosedHandler get() {
        return HANDLER;
    }

    static {
        LOGGER = LoggerFactory.getLogger(ClosedHandler.class);
    }
}
