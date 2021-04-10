package org.dsa.iot.haystack.actions;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.node.value.ValueUtils;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.helpers.StateHandler;
import org.projecthaystack.HDict;
import org.projecthaystack.HDictBuilder;
import org.projecthaystack.HGrid;
import org.projecthaystack.HRef;
import org.projecthaystack.client.HClient;

/**
 * @author Samuel Grenier
 */
public class InvokeActions {

    private static final Pattern PATTERN = Pattern.compile("(\\$\\w+)");

    public static Action getInvokeAction(final Haystack haystack,
                                         final HRef id,
                                         final String act,
                                         final List<Parameter> params) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                final CountDownLatch latch = new CountDownLatch(1);
                haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        HDictBuilder b = new HDictBuilder();
                        for (Parameter p : params) {
                            String name = p.getName();
                            Value v = event.getParameter(name);
                            if (v != null) {
                                Utils.argToDict(b, name, v);
                            }
                        }
                        HGrid res = client.invokeAction(id, act, b.toDict());
                        Actions.buildTable(res, event, false);
                        latch.countDown();
                    }
                });
                try {
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        String err = "Failed to retrieve data";
                        throw new RuntimeException(err);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        for (Parameter p : params) {
            a.addParameter(p);
        }
        a.setResultType(ResultType.TABLE);
        return a;
    }

    public static Action getInvokeAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                final CountDownLatch latch = new CountDownLatch(1);
                haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        Value vID = event.getParameter("ID", ValueType.STRING);
                        final HRef id = Utils.idToRef(vID);

                        Value vAct = event.getParameter("Action", ValueType.STRING);
                        final String act = vAct.getString();

                        Value vArgs = event.getParameter("Args", ValueType.MAP);
                        final JsonObject args = vArgs.getMap();

                        HDictBuilder b = new HDictBuilder();
                        for (Map.Entry<String, Object> entry : args) {
                            String name = entry.getKey();
                            Value val = ValueUtils.toValue(entry.getValue());
                            Utils.argToDict(b, name, val);
                        }
                        HGrid res = client.invokeAction(id, act, b.toDict());
                        Actions.buildTable(res, event, false);
                        latch.countDown();
                    }
                });
                try {
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        String err = "Failed to retrieve data";
                        throw new RuntimeException(err);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        {
            Parameter p = new Parameter("ID", ValueType.STRING);
            p.setDescription("Haystack ref ID to write to.");
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Action", ValueType.STRING);
            p.setDescription("Name of the action to set");
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Args", ValueType.MAP);
            JsonObject def = new JsonObject();
            def.put("str", "Hello world");
            def.put("bool", true);
            p.setPlaceHolder(def.toString());
            a.addParameter(p);
        }
        a.setResultType(ResultType.TABLE);
        return a;
    }

    public static void handleAction(Haystack haystack,
                                    HRef id,
                                    Node node,
                                    HDict row) {
        String dis = row.dis();
        String expr = row.getStr("expr");

        final List<Parameter> params = new LinkedList<>();
        Matcher matcher = PATTERN.matcher(expr);
        matcher:
        while (matcher.find()) {
            String name = matcher.group(0).substring(1);
            ValueType type;
            switch (name) {
                case "self":
                    continue matcher;
                case "bool":
                    type = ValueType.BOOL;
                    break;
                default:
                    type = ValueType.STRING;
            }
            Parameter p = new Parameter(name, type);
            params.add(p);
        }

        dis = StringUtils.encodeName(dis);
        NodeBuilder b = Utils.getBuilder(node, dis);
        b.setSerializable(false);
        b.setAction(getInvokeAction(haystack, id, dis, params));
        b.build();
    }
}
