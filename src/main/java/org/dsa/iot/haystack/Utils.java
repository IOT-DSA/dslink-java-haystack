package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.projecthaystack.HBool;
import org.projecthaystack.HDateTime;
import org.projecthaystack.HNum;
import org.projecthaystack.HVal;
import org.vertx.java.core.Handler;

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
        } else if (val instanceof HDateTime) {
            HDateTime time = (HDateTime) val;
            StringBuilder s = new StringBuilder();
            s.append(time.date.toZinc());
            s.append('T');
            s.append(time.time.toZinc());
            if (time.tzOffset == 0) {
                s.append('Z');
            } else {
                int offset = time.tzOffset;
                if (offset < 0) {
                    s.append('-');
                    offset = -offset;
                } else {
                    s.append('+');
                }
                int zh = offset / 3600;
                int zm = (offset % 3600) / 60;
                if (zh < 10) {
                    s.append('0');
                }
                s.append(zh);
                if (zm < 10) {
                    s.append('0');
                }
                s.append(zm);
            }
            return new Value(s.toString());
        }
        return new Value(val.toString());
    }

    static void initCommon(Haystack haystack, Node node) {
        NodeBuilder remServer = node.createChild("removeServer");
        remServer.setAction(Actions.getRemoveServerAction(node, haystack));
        remServer.build();

        NodeBuilder readNode = node.createChild("read");
        readNode.setAction(Actions.getReadAction(haystack));
        readNode.build();

        NodeBuilder evalNode = node.createChild("eval");
        evalNode.setAction(Actions.getEvalAction(haystack));
        evalNode.build();

        NodeBuilder hisReadNode = node.createChild("hisRead");
        hisReadNode.setAction(Actions.getHisReadAction(haystack));
        hisReadNode.build();

        node.setHasChildren(true);
        NodeListener listener = node.getListener();
        Handler<Node> handler = haystack.getHelper().getNavHandler(null);
        listener.setOnListHandler(handler);

        haystack.connect();
    }
}
