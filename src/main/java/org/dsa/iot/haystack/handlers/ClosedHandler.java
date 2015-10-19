package org.dsa.iot.haystack.handlers;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.SubscriptionManager;
import org.dsa.iot.dslink.node.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dsa.iot.dslink.util.handler.Handler;

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
        {
            Value val = new Value(0);
            val.setSerializable(false);
            event.setRoConfig("lu", val);
        }
        LOGGER.debug("Wants to remove: {}", event.getPath());
        Map<String, Node> children = event.getChildren();
        if (children == null) {
            return;
        }
        for (Node child : children.values()) {
            if (child == null) {
                continue;
            }
            Map<String, Node> nChildren = child.getChildren();
            if (nChildren == null) {
                continue;
            }
            for (Node n : nChildren.values()) {
                if (n == null
                        || (n.getValue() == null
                        && n.getAction() == null)) {
                    continue;
                }
                removeNodes(n);
            }
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
                if (n != null && (n.getValue() == null)) {
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
