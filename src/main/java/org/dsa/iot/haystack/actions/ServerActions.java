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
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;

/**
 * @author Samuel Grenier
 */
public class ServerActions {

    public static Action getAddServerAction(final Node parent) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                ValueType vt = ValueType.STRING;
                Value vName = event.getParameter("Name", vt);
                Value vUrl = event.getParameter("URL", vt);
                Value vUser = event.getParameter("Username", vt);
                Value vPass = event.getParameter("Password");
                Value vConnTimeout = event.getParameter("Connect Timeout");
                Value vReadTimeout = event.getParameter("Read Timeout");
                Value vEnabled = event.getParameter("Enabled", ValueType.BOOL);
                Value vPollRate = event.getParameter("Poll Rate");

                String name = vName.getString();
                String url = vUrl.getString();

                NodeBuilder builder = Utils.getBuilder(parent, name);
                builder.setConfig("url", new Value(url));
                if (vUser != null) {
                    String user = vUser.getString();
                    builder.setConfig("username", new Value(user));
                }
                if (vPass != null) {
                    char[] pass = vPass.getString().toCharArray();
                    builder.setPassword(pass);
                }
                builder.setConfig("enabled", vEnabled);
                builder.setConfig("pollRate", vPollRate);
                builder.setConfig("connect timeout", vConnTimeout);
                builder.setConfig("read timeout", vReadTimeout);
                Node node = builder.build();

                Haystack haystack = new Haystack(node);
                Utils.initCommon(haystack, node);
            }
        });
        a.addParameter(new Parameter("Name", ValueType.STRING));
        a.addParameter(new Parameter("URL", ValueType.STRING)
                               .setDescription("http://{domain}/api/{projectName}")
                               .setPlaceHolder("http://{domain}/api/{projectName}"));
        a.addParameter(new Parameter("Username", ValueType.STRING));
        {
            Parameter p = new Parameter("Password", ValueType.STRING);
            p.setEditorType(EditorType.PASSWORD);
            a.addParameter(p);
        }
        a.addParameter(new Parameter("Enabled", ValueType.BOOL, new Value(true)));
        a.addParameter(new Parameter("Poll Rate", ValueType.NUMBER, new Value(5)).setDescription("Poll rate in seconds"));
        a.addParameter(new Parameter("Connect Timeout", ValueType.NUMBER, new Value(60)).setDescription("Connect timeout in seconds"));
        a.addParameter(new Parameter("Read Timeout", ValueType.NUMBER, new Value(60)).setDescription("Read timeout in seconds"));
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

                Value vUrl = event.getParameter("URL", ValueType.STRING);
                Value vUser = event.getParameter("Username", ValueType.STRING);
                Value vPass = event.getParameter("Password");
                Value vEnabled = event.getParameter("Enabled", ValueType.BOOL);
                Value vPR = event.getParameter("Poll Rate", ValueType.NUMBER);
                Value vConnTimeout = event.getParameter("Connect Timeout");
                Value vReadTimeout = event.getParameter("Read Timeout");

                String url = vUrl.getString();
                String user = vUser.getString();
                String pass = vPass == null ? null : vPass.getString();

                node.setConfig("url", vUrl);
                node.setConfig("username", vUser);
                if (vPass != null) {
                    node.setPassword(pass.toCharArray());
                }

                if (vPR.getNumber().intValue() < 1) {
                    vPR.set(1);
                }
                node.setConfig("pollRate", vPR);
                node.setConfig("connect timeout", vConnTimeout);
                node.setConfig("read timeout", vReadTimeout);
                node.setConfig("enabled", vEnabled);
                int pollRate = vPR.getNumber().intValue();
                int connTimeout = (int) (vConnTimeout.getNumber().doubleValue() * 1000);
                int readTimeout = (int) (vReadTimeout.getNumber().doubleValue() * 1000);

                haystack.editConnection(
                        url, user, pass, pollRate, connTimeout, readTimeout, vEnabled.getBool());
            }
        });
        {
            Parameter p = new Parameter("URL", ValueType.STRING)
                    .setDescription("http://{domain}/api/{projectName}")
                    .setPlaceHolder("http://{domain}/api/{projectName}");
            p.setDefaultValue(node.getConfig("url"));
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Username", ValueType.STRING);
            p.setDefaultValue(node.getConfig("username"));
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Password", ValueType.STRING);
            p.setDescription("Leave blank to leave password unchanged");
            p.setEditorType(EditorType.PASSWORD);
            a.addParameter(p);
        }
        a.addParameter(new Parameter( "Enabled", ValueType.BOOL, node.getConfig("enabled")));
        {
            Parameter p = new Parameter("Poll Rate", ValueType.NUMBER);
            String desc = "How often the Haystack server should be polled ";
            desc += "changes";
            p.setDescription(desc);
            {
                Haystack haystack = node.getMetaData();
                p.setDefaultValue(haystack.getPollRate());
            }

            a.addParameter(p);
        }
        a.addParameter(new Parameter(
                "Connect Timeout", ValueType.NUMBER, node.getConfig("connect timeout"))
                               .setDescription("Connect timeout in seconds"));
        a.addParameter(new Parameter(
                "Read Timeout", ValueType.NUMBER, node.getConfig("read timeout"))
                               .setDescription("Read timeout in seconds"));
        return a;
    }

}
