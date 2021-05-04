package org.dsa.iot.haystack.actions;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import org.dsa.iot.dslink.methods.StreamState;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.helpers.StateHandler;
import org.dsa.iot.haystack.helpers.SubHelper;
import org.projecthaystack.HBool;
import org.projecthaystack.HCol;
import org.projecthaystack.HDict;
import org.projecthaystack.HGrid;
import org.projecthaystack.HGridBuilder;
import org.projecthaystack.HNum;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HStr;
import org.projecthaystack.HVal;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Actions {

    private static final Logger LOGGER = LoggerFactory.getLogger(Actions.class);

    public static Action getSubscribeAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {

            @Override
            public void handle(ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                Value vId = event.getParameter("ID", ValueType.STRING);
                String id = vId.getString();
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
            Value def = new Value(5);
            Parameter p = new Parameter("Poll Rate", ValueType.NUMBER, def);
            p.setDescription("Poll Rate is in seconds.");
            a.addParameter(p);
        }
        a.setResultType(ResultType.STREAM);
        return a;
    }

    public static Node getPointWriteAction(Haystack haystack, Node parent) {
        return getPointWriteAction(haystack, parent, null, null);
    }

    public static Node getPointWriteAction(final Haystack haystack,
                                           final Node parent,
                                           final HRef treeId,
                                           final String kind) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        Value vLev = event.getParameter("Level", ValueType.STRING);
                        Value vValue = event.getParameter("Value");
                        Value vVT = event.getParameter("Value Type");
                        Value vUnit = event.getParameter("Value Unit");
                        Value vWho = event.getParameter("Who");
                        Value vDur = event.getParameter("Duration");
                        Value vDurUnit = event.getParameter("Duration Unit");

                        HRef id = treeId;
                        if (id == null) {
                            Value vId = event.getParameter("ID", ValueType.STRING);
                            id = Utils.idToRef(vId);
                        }

                        String sLevel = vLev.getString();
                        int level = Integer.parseInt(sLevel);
                        if (level < 1 || level > 17) {
                            throw new RuntimeException("Invalid level");
                        }

                        HVal val = null;
                        if ((vValue != null) && !vValue.getString().isEmpty()) {
                            String type;
                            if (kind != null) {
                                type = kind.toLowerCase();
                            } else if (vVT != null) {
                                type = vVT.getString();
                            } else {
                                String err = "Missing value type";
                                throw new RuntimeException(err);
                            }
                            String stringVal = vValue.getString();
                            switch (type) {
                                case "bool":
                                    boolean b = Boolean.parseBoolean(stringVal);
                                    val = HBool.make(b);
                                    break;
                                case "number":
                                    double num = Double.parseDouble(stringVal);
                                    String unit = null;
                                    if (vUnit != null) {
                                        unit = vUnit.getString();
                                    }
                                    val = HNum.make(num, unit);
                                    break;
                                case "str":
                                    val = HStr.make(stringVal);
                                    break;
                                default:
                                    String err = "Unknown type: " + type;
                                    throw new RuntimeException(err);
                            }
                        }

                        String who = null;
                        if (vWho != null) {
                            who = vWho.getString();
                        }

                        HNum dur = null;
                        if (vDur != null) {
                            if (vDurUnit == null) {
                                String err = "Missing duration unit";
                                throw new RuntimeException(err);
                            }
                            String unit = vDurUnit.getString();
                            dur = HNum.make(vDur.getNumber().intValue(), unit);
                        }
                        client.pointWrite(id, level, who, val, dur);
                    }
                });
            }
        });
        if (treeId == null) {
            Parameter p = new Parameter("ID", ValueType.STRING);
            p.setDescription("Haystack ref ID to write to.");
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Value", ValueType.STRING);
            String msg = "Value to write or none to set the level ";
            msg += "back to auto.";
            p.setDescription(msg);
            a.addParameter(p);
        }
        if (kind == null) {

            ValueType type = Utils.getHaystackTypes();
            Parameter p = new Parameter("Value Type", type);
            p.setDescription("Haystack value type.");
            p.setDefaultValue(new Value("str"));
            a.addParameter(p);
        }
        {

            Parameter p = new Parameter("Value Unit", ValueType.STRING);
            p.setDescription("Value unit, only affects number types.");
            a.addParameter(p);
        }
        {
            String[] enums = new String[17];
            for (int i = 0; i < enums.length; ++i) {
                enums[i] = String.valueOf(i + 1);
            }
            ValueType type = ValueType.makeEnum(enums);
            Parameter p = new Parameter("Level", type);
            p.setDescription("Number from 1-17 for level to write.");
            p.setDefaultValue(new Value("17"));
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Who", ValueType.STRING);
            String msg = "optional username performing the write, ";
            msg += "otherwise user dis is used.";
            p.setDescription(msg);
            a.addParameter(p);
        }
        {
            Parameter p = new Parameter("Duration", ValueType.NUMBER);
            String msg = "Duration before level expires back to auto.";
            msg += " This takes effect when using a level of 8";
            p.setDescription(msg);
            a.addParameter(p);
        }
        {
            String[] enums = new String[]{
                    "ms",
                    "sec",
                    "min",
                    "hr",
                    "day",
                    "wk",
                    "mo",
                    "yr"
            };
            ValueType type = ValueType.makeEnum(enums);
            Parameter p = new Parameter("Duration Unit", type);
            p.setDefaultValue(new Value("hr"));
            p.setDescription("Duration unit.");
            a.addParameter(p);
        }
        NodeBuilder writeNode = Utils.getBuilder(parent, "pointWrite");
        writeNode.setDisplayName("Point Write");
        writeNode.setSerializable(false);
        writeNode.setAction(a);
        return writeNode.build();
    }

    public static Node getSetAction(final Haystack haystack,
                                    final Node parent,
                                    final HRef treeId,
                                    final String kind) {
        final String type = kind.toLowerCase(Locale.ROOT);
        final ValueType valueType;
        switch (type) {
            case "bool":
                valueType = ValueType.BOOL;
                break;
            case "number":
                valueType = ValueType.NUMBER;
                break;
            case "str":
                valueType = ValueType.STRING;
                break;
            default:
                throw new RuntimeException("Unknown type: " + kind);
        }
        Action a = new Action(Permission.WRITE, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                    @Override
                    public void handle(HClient client) {
                        Value vValue = event.getParameter("Value");

                        HRef id = treeId;
                        if (id == null) {
                            Value vId = event.getParameter("ID", ValueType.STRING);
                            id = Utils.idToRef(vId);
                        }

                        HVal val;
                        switch (type) {
                            case "bool":
                                val = HBool.make(vValue.getBool());
                                break;
                            case "number":
                                val = HNum.make(vValue.getNumber().doubleValue());
                                break;
                            default:
                                val = HStr.make(vValue.getString());
                                break;
                        }
                        client.pointWrite(id, 17, null, val, null);
                    }
                });
            }
        });
        Parameter p = new Parameter("Value", valueType);
        a.addParameter(p);
        return parent.createChild("set", false)
                     .setAction(a)
                     .setDisplayName("Set")
                     .build();
    }

    public static Action getReadAction(final Haystack haystack) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult event) {
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
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
                            buildTable(grid, event, false);
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
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
                Value vExpr = event.getParameter("expr", ValueType.STRING);
                String expr = vExpr.getString();

                haystack.eval(expr, new Handler<HGrid>() {
                    @Override
                    public void handle(HGrid grid) {
                        if (grid != null) {
                            buildTable(grid, event, false);
                        } else {
                            event.getTable().close();
                        }
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
                if (!haystack.isEnabled()) {
                    throw new IllegalStateException("Disabled");
                }
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
                            buildTable(grid, event, false);
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

    public static void buildTable(HGrid in, ActionResult out, boolean historyColNames) {
        Table t = out.getTable();

        {
            HDict meta = in.meta();
            if (meta != null && !meta.isEmpty()) {
                Iterator<?> it = meta.iterator();
                JsonObject metaObj = new JsonObject();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    String name = (String) entry.getKey();
                    if (name != null) {
                        HVal val = (HVal) entry.getValue();
                        Value value = Utils.hvalToVal(val);
                        metaObj.put(name, value);
                    }
                }
                t.setTableMeta(metaObj);
            }
        }

        for (int i = 0; i < in.numCols(); i++) {
            HCol col = in.col(i);
            String name = col.name();
            if (historyColNames) {
                if ("ts".equals(name)) {
                    name = "timestamp";
                } else if ("val".equals(name)) {
                    name = "value";
                }
            }
            Parameter p = new Parameter(name, ValueType.DYNAMIC);

            HDict meta = col.meta();
            if (meta != null && !meta.isEmpty()) {
                Iterator<?> it = meta.iterator();
                JsonObject metaObj = new JsonObject();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    name = (String) entry.getKey();
                    if (name != null) {
                        HVal val = (HVal) entry.getValue();
                        Value value = Utils.hvalToVal(val);
                        metaObj.put(name, value);
                    }
                }
                p.setMetaData(metaObj);
            }

            t.addColumn(p);
        }

        Iterator<?> it = in.iterator();
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
