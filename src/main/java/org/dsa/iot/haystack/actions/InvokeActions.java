package org.dsa.iot.haystack.actions;

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
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.projecthaystack.HDictBuilder;
import org.projecthaystack.HGrid;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.client.HClient;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                final CountDownLatch latch = new CountDownLatch(1);
                haystack.getConnHelper().getClient(new Handler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        HDictBuilder b = new HDictBuilder();
                        for (Parameter p : params) {
                            String name = p.getName();
                            ValueType type = p.getType();
                            Value v = event.getParameter(name, type);
                            Utils.argToDict(b, name, v);
                        }
                        HGrid res = client.invokeAction(id, act, b.toDict());
                        Actions.buildTable(res, event);
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
        return a;
    }

    public static Action getInvokeAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                final CountDownLatch latch = new CountDownLatch(1);
                haystack.getConnHelper().getClient(new Handler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        Value vID = event.getParameter("ID", ValueType.STRING);
                        final HRef id = Utils.idToRef(vID);

                        Value vAct = event.getParameter("Action", ValueType.STRING);
                        final String act = vAct.getString();

                        Value vArgs = event.getParameter("Args", ValueType.MAP);
                        final JsonObject args = vArgs.getMap();

                        HDictBuilder b = new HDictBuilder();
                        for (Map.Entry<String, Object> entry
                                    : args.toMap().entrySet()) {
                            String name = entry.getKey();
                            Value val = ValueUtils.toValue(entry.getValue());
                            Utils.argToDict(b, name, val);
                        }
                        HGrid res = client.invokeAction(id, act, b.toDict());
                        Actions.buildTable(res, event);
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
            def.putString("str", "Hello world");
            def.putBoolean("bool", true);
            p.setPlaceHolder(def.toString());
            a.addParameter(p);
        }
        a.setResultType(ResultType.TABLE);
        return a;
    }

    public static void handleAction(Haystack haystack,
                                    HRef id,
                                    Node node,
                                    HRow row) {
        String dis = row.dis();
        String expr = row.getStr("expr");

        final List<Parameter> params = new LinkedList<>();
        Matcher matcher = PATTERN.matcher(expr);
        while (matcher.find()) {
            String name = matcher.group(0).substring(1);
            ValueType type;
            switch (name) {
                case "number":
                    type = ValueType.NUMBER;
                    break;
                case "bool":
                    type = ValueType.BOOL;
                    break;
                default:
                    type = ValueType.STRING;
            }
            Parameter p = new Parameter(name, type);
            params.add(p);
        }

        dis = StringUtils.filterBannedChars(dis);
        NodeBuilder b = node.createChild(dis);
        b.setSerializable(false);
        b.setAction(getInvokeAction(haystack, id, dis, params));
        b.build();
    }
}