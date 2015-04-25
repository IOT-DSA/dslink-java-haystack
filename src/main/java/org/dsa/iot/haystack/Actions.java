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
        Action act = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                JsonObject params = event.getJsonIn().getObject("params");
                if (params == null) {
                    throw new IllegalArgumentException("params");
                }

                String name = params.getString("name");
                String url = params.getString("url");
                String username = params.getString("username");
                String password = params.getString("password");
                if (name == null) {
                    throw new IllegalArgumentException("name");
                } else if (url == null) {
                    throw new IllegalArgumentException("url");
                } else if (username == null) {
                    throw new IllegalArgumentException("username");
                } else if (password == null) {
                    throw new IllegalArgumentException("password");
                }

                NodeBuilder builder = parent.createChild(name);
                builder.setConfig("url", new Value(url));
                builder.setConfig("username", new Value(username));
                builder.setPassword(password.toCharArray());
                Node node = builder.build();

                Haystack haystack = new Haystack(node);
                Utils.initCommon(haystack, node);
            }
        });
        act.addParameter(new Parameter("name", ValueType.STRING));
        act.addParameter(new Parameter("url", ValueType.STRING));
        act.addParameter(new Parameter("username", ValueType.STRING));
        act.addParameter(new Parameter("password", ValueType.STRING));
        return act;
    }

    static Action getConnectAction(final Haystack haystack) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                if (!haystack.isConnected()) {
                    haystack.connect();
                }
            }
        });
    }

    static Action getReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                JsonObject params = event.getJsonIn().getObject("params");
                if (params == null) {
                    throw new RuntimeException("Missing params");
                }

                String filter = params.getString("filter");
                Integer limit = params.getInteger("limit");

                if (filter == null) {
                    throw new RuntimeException("Missing filter parameter");
                }

                HGridBuilder builder = new HGridBuilder();
                builder.addCol("filter");
                {
                    HVal[] row;
                    if (limit != null) {
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
                }

                HGrid grid = haystack.call("read", builder.toGrid());
                {
                    JsonArray columns = new JsonArray();
                    for (int i = 0; i < grid.numCols(); i++) {
                        JsonObject col = new JsonObject();
                        col.putString("name", grid.col(i).name());
                        col.putString("type", ValueType.STRING.toJsonString());
                        columns.addObject(col);
                    }
                    event.setColumns(columns);
                }

                {
                    JsonArray results = new JsonArray();
                    Iterator it = grid.iterator();
                    while (it.hasNext()) {
                        HRow row = (HRow) it.next();
                        JsonArray res = new JsonArray();

                        for (int x = 0; x < grid.numCols(); x++) {
                            HVal val = row.get(grid.col(x), false);
                            if (val != null) {
                                res.addString(val.toString());
                            } else {
                                res.addString(null);
                            }
                        }

                        results.addArray(res);
                    }

                    event.setUpdates(results);
                }
            }
        }, Action.InvokeMode.ASYNC);
        a.addParameter(new Parameter("filter", ValueType.STRING));
        a.addParameter(new Parameter("limit", ValueType.NUMBER));
        a.setResultType(ResultType.TABLE);
        return a;
    }

}
