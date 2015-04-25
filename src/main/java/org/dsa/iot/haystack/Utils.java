package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.projecthaystack.HBool;
import org.projecthaystack.HNum;
import org.projecthaystack.HVal;
import org.vertx.java.core.Handler;

import java.util.regex.Pattern;

/**
 * @author Samuel Grenier
 */
public class Utils {

    static Value hvalToVal(HVal val) {
        if (val == null) {
            return null;
        } else if (val instanceof HNum) {
            return new Value(((HNum) val).val);
        } else if (val instanceof HBool) {
            return new Value(((HBool) val).val);
        }
        return new Value(val.toString());
    }

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
