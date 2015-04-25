package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.vertx.java.core.Handler;

import java.util.regex.Pattern;

/**
 * @author Samuel Grenier
 */
public class Utils {

    static String filterBannedChars(String name) {
        for (String banned : Node.getBannedCharacters()) {
            if (name.contains(banned)) {
                name = name.replaceAll(Pattern.quote(banned), "");
            }
        }
        return name;
    }

    static void initCommon(Haystack haystack, Node node) {
        NodeBuilder connectNode = node.createChild("connect");
        connectNode.setAction(Actions.getConnectAction(haystack));
        connectNode.build();

        NodeBuilder readNode = node.createChild("read");
        readNode.setAction(Actions.getReadAction(haystack));
        readNode.build();

        NodeBuilder evalNode = node.createChild("eval");
        evalNode.setAction(Actions.getEvalAction(haystack));
        evalNode.build();

        NodeListener listener = node.getListener();
        Handler<Node> handler = haystack.getHelper().getNavHandler(null);
        listener.addOnListHandler(handler);

        haystack.connect();
    }
}
