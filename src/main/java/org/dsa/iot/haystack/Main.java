package org.dsa.iot.haystack;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private DSLink link;

    @Override
    public boolean isResponder() {
        return true;
    }

    @Override
    public void stop() {
        super.stop();
        if (link != null) {
            Node root = link.getNodeManager().getSuperRoot();
            Map<String, Node> children = root.getChildren();
            if (children != null) {
                for (Node node : children.values()) {
                    Haystack haystack = node.getMetaData();
                    if (haystack != null) {
                        haystack.destroy();
                    }
                }
            }
        }
    }

    @Override
    public void onResponderInitialized(DSLink link) {
        this.link = link;
        LOGGER.info("Connected");

        Node superRoot = link.getNodeManager().getSuperRoot();
        Haystack.init(superRoot);
    }

    public static void main(String[] args) {
        DSLinkFactory.start(args, new Main());
    }
}
