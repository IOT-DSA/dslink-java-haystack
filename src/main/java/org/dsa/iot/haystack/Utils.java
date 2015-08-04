package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.InvokeActions;
import org.dsa.iot.haystack.actions.ServerActions;
import org.dsa.iot.haystack.handlers.ListHandler;
import org.projecthaystack.*;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class Utils {

    public static HRef idToRef(Value value) {
        return idToRef(value.getString());
    }

    public static HRef idToRef(String id) {
        if (id.startsWith("@")) {
            id = id.substring(1);
        }
        return HRef.make(id);
    }

    public static void argToDict(HDictBuilder b, String name, Value value) {
        switch (name) {
            case "str":
                b.add(name, value.getString());
                break;
            case "bool":
                b.add(name, value.getBool());
                break;
            case "number":
                b.add(name, value.getNumber().doubleValue());
                break;
            case "date":
                b.add("date", HDate.make(value.getString()));
                break;
            default:
                throw new RuntimeException("Unknown type: " + name);
        }
    }

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
        editServer.setAction(ServerActions.getEditAction(node));
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

        NodeBuilder invokeNode = node.createChild("invoke");
        invokeNode.setDisplayName("Invoke");
        invokeNode.setAction(InvokeActions.getInvokeAction(haystack));
        invokeNode.setSerializable(false);
        invokeNode.build();

        node.setHasChildren(true);
        node.setRoConfig("navId", new Value((String) null));
        NodeListener listener = node.getListener();
        Handler<Node> handler = ListHandler.get();
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
