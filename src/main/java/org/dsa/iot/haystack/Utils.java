package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.ServerActions;
import org.projecthaystack.HBool;
import org.projecthaystack.HDateTime;
import org.projecthaystack.HNum;
import org.projecthaystack.HVal;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class Utils {

    public static Value hvalToVal(HVal val) {
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

    public static void initCommon(Haystack haystack, Node node) {
        NodeBuilder remServer = node.createChild("removeServer");
        remServer.setDisplayName("Remove Server");
        remServer.setAction(ServerActions.getRemoveServerAction(node, haystack));
        remServer.setSerializable(false);
        remServer.build();

        NodeBuilder editServer = node.createChild("editServer");
        editServer.setDisplayName("Edit Server");
        editServer.setAction(ServerActions.getEditServerAction(node));
        editServer.setSerializable(false);
        editServer.build();

        NodeBuilder readNode = node.createChild("read");
        readNode.setDisplayName("Read");
        readNode.setAction(Actions.getReadAction(haystack));
        readNode.setSerializable(false);
        readNode.build();

        NodeBuilder evalNode = node.createChild("eval");
        evalNode.setDisplayName("Evaluate");
        evalNode.setAction(Actions.getEvalAction(haystack));
        evalNode.setSerializable(false);
        evalNode.build();

        NodeBuilder hisReadNode = node.createChild("hisRead");
        hisReadNode.setDisplayName("History Read");
        hisReadNode.setAction(Actions.getHisReadAction(haystack));
        hisReadNode.setSerializable(false);
        hisReadNode.build();

        NodeBuilder subNode = node.createChild("subscribe");
        subNode.setDisplayName("Subscribe");
        subNode.setAction(Actions.getSubscribeAction(haystack));
        subNode.setSerializable(false);
        subNode.build();

        NodeBuilder writeNode = node.createChild("pointWrite");
        writeNode.setDisplayName("Point Write");
        writeNode.setAction(Actions.getPointWriteAction(haystack));
        writeNode.setSerializable(false);
        writeNode.build();

        node.setHasChildren(true);
        NodeListener listener = node.getListener();
        Handler<Node> handler = haystack.getNavHelper().getNavHandler(null);
        listener.setOnListHandler(handler);
    }

    public static ValueType getHaystackTypes() {
        String[] enums = new String[] {
                "bool",
                "number",
                "str",
        };
        return ValueType.makeEnum(enums);
    }
}
