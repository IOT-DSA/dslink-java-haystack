package org.dsa.iot.haystack.actions;

import org.dsa.iot.dslink.methods.StreamState;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.*;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.helpers.SubHelper;
import org.projecthaystack.*;
import org.vertx.java.core.Handler;

import java.util.Iterator;

/**
 * @author Samuel Grenier
 */
public class Actions {

    public static Action getSubscribeAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {

            @Override
            public void handle(ActionResult event) {
                Value vId = event.getParameter("ID", ValueType.STRING);
                String id = vId.getString();
                if (id.startsWith("@")) {
                    id = id.substring(1);
                }

                Value vPoll = event.getParameter("Poll Rate", ValueType.NUMBER);
                int pollRate = vPoll.getNumber().intValue();

                final SubHelper helper = new SubHelper(haystack, id);
                event.setCloseHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        helper.stop();
                    }
                });
                event.setStreamState(StreamState.OPEN);
                helper.start(event.getTable(), pollRate);
            }
        });
        {
            Parameter p = new Parameter("ID", ValueType.STRING);
            p.setDescription("Haystack ref ID to subscribe to.");
            a.addParameter(p);
        }
        {
            Value def = new Value(5000);
            Parameter p = new Parameter("Poll Rate", ValueType.NUMBER, def);
            p.setDescription("Poll Rate is in milliseconds.");
            a.addParameter(p);
        }
        a.setResultType(ResultType.STREAM);
        return a;
    }

    public static Action getReadAction(final Haystack haystack) {
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

    public static Action getEvalAction(final Haystack haystack) {
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

    public static Action getHisReadAction(final Haystack haystack) {
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
