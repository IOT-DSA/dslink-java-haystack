package org.dsa.iot.haystack;

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
import org.projecthaystack.*;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

/**
 * @author Samuel Grenier
 */
public class Actions {

    static Action getAddServerAction(final Node parent) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                ValueType vt = ValueType.STRING;
                Value vName = event.getParameter("name", vt);
                Value vUrl = event.getParameter("url", vt);
                Value vUser = event.getParameter("username", vt);
                Value vPass = event.getParameter("password", vt);

                String name = vName.getString();
                String url = vUrl.getString();
                String username = vUser.getString();
                String password = vPass.getString();

                NodeBuilder builder = parent.createChild(name);
                builder.setConfig("url", new Value(url));
                builder.setConfig("username", new Value(username));
                builder.setPassword(password.toCharArray());
                Node node = builder.build();

                Haystack haystack = new Haystack(node);
                Utils.initCommon(haystack, node);
            }
        });
        a.addParameter(new Parameter("name", ValueType.STRING));
        a.addParameter(new Parameter("url", ValueType.STRING));
        a.addParameter(new Parameter("username", ValueType.STRING));
        a.addParameter(new Parameter("password", ValueType.STRING));
        return a;
    }

    static Action getRemoveServerAction(final Node node,
                                        final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                node.getParent().removeChild(node);
                haystack.stop();
            }
        });
    }

    static Action getConnectAction(final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                haystack.ensureConnected();
            }
        });
    }

    static Action getReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vFilter = event.getParameter("filter", ValueType.STRING);
                Value vLimit = event.getParameter("limit");

                String filter = vFilter.getString();
                /*HGridBuilder builder = new HGridBuilder();
                builder.addCol("filter");
                {
                    HVal[] row;
                    if (vLimit != null) {
                        int limit = vLimit.getNumber().intValue();
                        row = new HVal[]{
                                HStr.make(filter),
                                HNum.make(limit)
                        };
                        builder.addCol("limit");
                    } else {
                        row = new HVal[]{
                                HStr.make(filter)
                        };
                    }
                    builder.addRow(row);
                }*/

                HGrid grid;
                if (vLimit != null) {
                    int lim = vLimit.getNumber().intValue();
                    grid = haystack.read(filter, lim);
                } else {
                    grid = haystack.read(filter, 1);
                }

                if (grid != null) {
                    buildTable(grid, event);
                }
            }
        });
        a.addParameter(new Parameter("filter", ValueType.STRING));
        a.addParameter(new Parameter("limit", ValueType.NUMBER));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    static Action getEvalAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vExpr = event.getParameter("expr", ValueType.STRING);
                String expr = vExpr.getString();

                HGrid grid = haystack.eval(expr);
                buildTable(grid, event);
            }
        });
        a.addParameter(new Parameter("expr", ValueType.STRING));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    static Action getHisReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vId = event.getParameter("id", ValueType.STRING);
                Value vRange = event.getParameter("range", ValueType.STRING);
                String id = vId.getString();
                String range = vRange.getString();

                HGridBuilder builder = new HGridBuilder();
                builder.addCol("id");
                builder.addCol("range");
                builder.addRow(new HVal[]{
                        HRef.make(id),
                        HStr.make(range)
                });

                HGrid grid = haystack.call("hisRead", builder.toGrid());
                if (grid != null) {
                    buildTable(grid, event);
                }
            }
        });
        a.addParameter(new Parameter("id", ValueType.STRING));
        a.addParameter(new Parameter("range", ValueType.STRING));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    private static void buildTable(HGrid in, ActionResult out) {
        {
            JsonArray columns = new JsonArray();
            for (int i = 0; i < in.numCols(); i++) {
                JsonObject col = new JsonObject();
                col.putString("name", in.col(i).name());
                col.putString("type", ValueType.DYNAMIC.toJsonString());
                columns.addObject(col);
            }
            out.setColumns(columns);
        }

        {
            JsonArray results = new JsonArray();
            Iterator it = in.iterator();
            while (it.hasNext()) {
                HRow row = (HRow) it.next();
                JsonArray res = new JsonArray();

                for (int i = 0; i < in.numCols(); i++) {
                    HVal val = row.get(in.col(i), false);
                    if (val != null) {
                        Value value = Utils.hvalToVal(val);
                        ValueUtils.toJson(res, value);
                    } else {
                        res.add(null);
                    }
                }

                results.addArray(res);
            }

            out.setUpdates(results);
        }
    }
}
