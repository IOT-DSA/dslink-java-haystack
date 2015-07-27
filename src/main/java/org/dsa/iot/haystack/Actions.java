package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.*;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.projecthaystack.*;
import org.vertx.java.core.Handler;

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

    static Action getEditServerAction(final Node node) {
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
                    node.setConfig("vPass", new Value(pass));
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

    static Action getReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                Value vFilter = event.getParameter("filter", ValueType.STRING);
                Value vLimit = event.getParameter("limit");

                String filter = vFilter.getString();
                int limit = 1;
                if (vLimit != null) {
                    limit = vLimit.getNumber().intValue();
                }
                haystack.read(filter, limit, new Handler<HGrid>() {
                    @Override
                    public void handle(HGrid grid) {
                        if (grid != null) {
                            buildTable(grid, event);
                        }
                    }
                });

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
            public void handle(final ActionResult event) {
                Value vExpr = event.getParameter("expr", ValueType.STRING);
                String expr = vExpr.getString();

                haystack.eval(expr, new Handler<HGrid>() {
                    @Override
                    public void handle(HGrid grid) {
                        buildTable(grid, event);
                    }
                });
            }
        });
        a.addParameter(new Parameter("expr", ValueType.STRING));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    static Action getHisReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
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

                haystack.call("hisRead", builder.toGrid(), new Handler<HGrid>() {
                    @Override
                    public void handle(HGrid grid) {
                        if (grid != null) {
                            buildTable(grid, event);
                        }
                    }
                });

            }
        });
        a.addParameter(new Parameter("id", ValueType.STRING));
        a.addParameter(new Parameter("range", ValueType.STRING));
        a.setResultType(ResultType.TABLE);
        return a;
    }

    private static void buildTable(HGrid in, ActionResult out) {
        Table t = out.getTable();
        for (int i = 0; i < in.numCols(); i++) {
            String name = in.col(i).name();
            Parameter p = new Parameter(name, ValueType.DYNAMIC);
            t.addColumn(p);
        }

        Iterator it = in.iterator();
        while (it.hasNext()) {
            HRow hRow = (HRow) it.next();
            Row row = new Row();
            for (int i = 0; i < in.numCols(); i++) {
                HVal val = hRow.get(in.col(i), false);
                if (val != null) {
                    Value value = Utils.hvalToVal(val);
                    row.addValue(value);
                } else {
                    row.addValue(null);
                }
            }
            t.addRow(row);
        }
    }
}
