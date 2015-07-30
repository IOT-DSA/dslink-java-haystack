package org.dsa.iot.haystack.actions;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class ServerActions {

    public static Action getAddServerAction(final Node parent) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                ValueType vt = ValueType.STRING;
                Value vName = event.getParameter("name", vt);
                Value vUrl = event.getParameter("url", vt);
                Value vUser = event.getParameter("username");
                Value vPass = event.getParameter("password");

                String name = vName.getString();
                String url = vUrl.getString();

                NodeBuilder builder = parent.createChild(name);
                builder.setConfig("url", new Value(url));
                if (vUser != null) {
                    String user = vUser.getString();
                    builder.setConfig("username", new Value(user));
                }
                if (vPass != null) {
                    char[] pass = vPass.getString().toCharArray();
                    builder.setPassword(pass);
                }
                Node node = builder.build();

                Haystack haystack = new Haystack(node);
                Utils.initCommon(haystack, node);
            }
        });
        a.addParameter(new Parameter("name", ValueType.STRING));
        a.addParameter(new Parameter("url", ValueType.STRING));
        a.addParameter(new Parameter("username", ValueType.STRING));
        {
            Parameter p = new Parameter("password", ValueType.STRING);
            p.setEditorType(EditorType.PASSWORD);
            a.addParameter(p);
        }
        return a;
    }

    public static Action getRemoveServerAction(final Node node,
                                               final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                node.getParent().removeChild(node);
                haystack.stop();
            }
        });
    }

    public static Action getEditAction(final Node node) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Haystack haystack = node.getMetaData();

                Value vUrl = event.getParameter("url", ValueType.STRING);
                Value vUser = event.getParameter("username", ValueType.STRING);
                Value vPass = event.getParameter("password");

                String url = vUrl.getString();
                String user = vUser.getString();
                String pass = vPass == null ? null : vPass.getString();

                node.setConfig("url", vUrl);
                node.setConfig("username", vUser);
                if (vPass != null) {
                    node.setPassword(pass.toCharArray());
                }

                haystack.editConnection(url, user, pass);
            }
        });
        {
            Parameter p = new Parameter("url", ValueType.STRING);
            p.setDefaultValue(node.getConfig("url"));
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("username", ValueType.STRING);
            p.setDefaultValue(node.getConfig("username"));
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("password", ValueType.STRING);
            p.setDescription("Leave blank to leave password unchanged");
            p.setEditorType(EditorType.PASSWORD);
            a.addParameter(p);
        }
        return a;
    }

}
