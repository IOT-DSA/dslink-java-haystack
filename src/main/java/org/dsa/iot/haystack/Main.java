package org.dsa.iot.haystack;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    @Override
    public void onResponderInitialized(DSLink link) {
        LOGGER.info("Connected");

        Node superRoot = link.getNodeManager().getNode("/").getNode();
        Haystack.init(superRoot);
    }

    public static void main(String[] args) {
        DSLinkFactory.startResponder("haystack", args, new Main());
    }
}
