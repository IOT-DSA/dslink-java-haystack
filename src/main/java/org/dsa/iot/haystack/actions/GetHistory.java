package org.dsa.iot.haystack.actions;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.dsa.iot.dslink.methods.StreamState;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.actions.table.BatchRow;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.provider.LoopProvider;
import org.dsa.iot.dslink.util.TimeUtils;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.historian.stats.interval.IntervalParser;
import org.dsa.iot.historian.stats.interval.IntervalProcessor;
import org.dsa.iot.historian.stats.rollup.Rollup;
import org.dsa.iot.historian.utils.QueryData;
import org.projecthaystack.HCol;
import org.projecthaystack.HDateTime;
import org.projecthaystack.HDict;
import org.projecthaystack.HGrid;
import org.projecthaystack.HGridBuilder;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HStr;
import org.projecthaystack.HTimeZone;
import org.projecthaystack.HVal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class GetHistory implements Handler<ActionResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetHistory.class);

    private static final Parameter INTERVAL;
    private static final Parameter ROLLUP;
    private static final Parameter TIMERANGE;
    private static final Parameter TIMESTAMP = new Parameter("timestamp", ValueType.TIME);
    private static final Parameter VALUE = new Parameter("value", ValueType.DYNAMIC);

    private Node actionNode;
    private final Haystack haystack;
    private final HRef id;
    private final HTimeZone tz;

    static {
        List<String> enums = new ArrayList<>();
        enums.add("none");
        enums.add("and");
        enums.add("or");
        enums.add("avg");
        enums.add("min");
        enums.add("max");
        enums.add("sum");
        enums.add("first");
        enums.add("last");
        enums.add("count");
        enums.add("delta");

        INTERVAL = new Parameter("Interval", ValueType.STRING);
        INTERVAL.setDefaultValue(new Value("none"));
        ROLLUP = new Parameter("Rollup", ValueType.makeEnum(enums));
        TIMERANGE = new Parameter("Timerange", ValueType.STRING);
        TIMERANGE.setEditorType(EditorType.DATE_RANGE);
    }

    public GetHistory(Node node,
                      Haystack haystack,
                      HRef id,
                      HTimeZone tz) {
        this.haystack = haystack;
        this.id = id;
        if (tz == null) {
            tz = HTimeZone.DEFAULT;
        }
        this.tz = tz;
        initAction(node);
    }

    public Node getActionNode() {
        return actionNode;
    }

    @Override
    public void handle(final ActionResult event) {
        long from;
        long to;
        Calendar cal;
        Value v = event.getParameter("Timerange");
        if (v != null) {
            String range = event.getParameter("Timerange").getString();
            String[] split = range.split("/");
            cal = TimeUtils.decode(split[0], null);
            from = cal.getTimeInMillis();
            TimeUtils.decode(split[1], cal);
            to = cal.getTimeInMillis();
        } else { // Default to today
            cal = TimeUtils.alignDay(Calendar.getInstance());
            from = cal.getTimeInMillis();
            TimeUtils.addDays(1, cal);
            to = cal.getTimeInMillis();
        }
        TimeUtils.recycleCalendar(cal);

        Value def = new Value("none");
        String sInterval = event.getParameter("Interval", def).getString();
        String sRollup = event.getParameter("Rollup", def).getString();

        Table table = event.getTable();
        event.setStreamState(StreamState.INITIALIZED);
        table.setMode(Table.Mode.APPEND);

        IntervalParser parser = IntervalParser.parse(sInterval);
        Rollup.Type type = Rollup.Type.toEnum(sRollup);
        process(event, from, to, type, parser);
    }

    protected void process(final ActionResult event,
                           final long from,
                           final long to,
                           final Rollup.Type rollup,
                           final IntervalParser parser) {
        final IntervalProcessor interval = IntervalProcessor.parse(parser, rollup, tz.java);
        final Table table = event.getTable();
        final StringBuilder buffer = new StringBuilder();
        final Calendar calendar = TimeUtils.reuseCalendar();
        calendar.setTimeZone(tz.java);
        query(table, from, to, new CompleteHandler<QueryData>() {

            private List<QueryData> updates = new LinkedList<>();

            @Override
            public void handle(QueryData data) {
                List<QueryData> updates = this.updates;
                if (updates != null) {
                    updates.add(data);
                    if (updates.size() >= 100) {
                        processQueryData(table, interval, updates, calendar, buffer);
                    }
                }
            }

            @Override
            public void complete() {
                if (!updates.isEmpty()) {
                    processQueryData(table, interval, updates, calendar, buffer);
                }
                updates = null;
                if (interval != null) {
                    Row row = interval.complete();
                    if (row != null) {
                        table.addRow(row);
                    }
                }
                table.close();
                TimeUtils.recycleCalendar(calendar);
            }
        });
    }

    protected void query(final Table table, long from, long to, final CompleteHandler<QueryData> handler) {
        StringBuilder buf = new StringBuilder();
        buf.append(HDateTime.make(from, tz));
        buf.append(',');
        buf.append(HDateTime.make(to, tz));
        HGridBuilder builder = new HGridBuilder();
        builder.addCol("id");
        builder.addCol("range");
        builder.addRow(new HVal[]{
                HRef.make(id.toString()),
                HStr.make(buf.toString())
        });

        haystack.call("hisRead", builder.toGrid(), new Handler<HGrid>() {
            @Override
            public void handle(HGrid grid) {
                LoopProvider.getProvider().schedule(new QueryProcessor(table, grid, handler));
            }
        });
    }

    protected void processQueryData(Table table,
                                    IntervalProcessor interval,
                                    Collection<QueryData> data,
                                    Calendar calendar,
                                    StringBuilder buffer) {
        if (data.isEmpty()) {
            return;
        }
        BatchRow batch = null;
        Iterator<QueryData> it = data.iterator();
        while (it.hasNext()) {
            QueryData update = it.next();
            it.remove();
            Row row;
            long time = update.getTimestamp();
            if (interval == null) {
                row = new Row();
                calendar.setTimeInMillis(time);
                buffer.setLength(0);
                String t = TimeUtils.encode(calendar, true, buffer).toString();
                row.addValue(new Value(t));
                row.addValue(update.getValue());
            } else {
                row = interval.getRowUpdate(update, time);
            }

            if (row != null) {
                if (batch == null) {
                    batch = new BatchRow();
                }
                batch.addRow(row);
            }
        }
        if (batch != null) {
            // If we can't get a stream open to the requester, then there's a chance batch rows
            // could eventually cause an out of memory situation.  So fail the invocation after a
            // minute of not getting a response.
            if (table.waitForStream(60000, true, true)) {
                table.addBatchRows(batch);
            }
        }
    }

    private void initAction(Node node) {
        Action a = new Action(Permission.READ, this);

        a.addParameter(TIMERANGE);
        a.addParameter(INTERVAL);
        a.addParameter(ROLLUP);

        a.addResult(TIMESTAMP);
        a.addResult(VALUE);
        a.setResultType(ResultType.STREAM);

        NodeBuilder b = node.createChild("getHistory", false);
        b.setProfile("getHistory_");
        b.setDisplayName("Get History");
        b.setSerializable(false);
        b.setAction(a);
        actionNode = b.build();
    }

    private static class QueryProcessor implements Runnable {

        private final HGrid grid;
        private final CompleteHandler<QueryData> handler;
        private final Table table;

        QueryProcessor(Table table, HGrid grid, CompleteHandler<QueryData> handler) {
            this.grid = grid;
            this.handler = handler;
            this.table = table;
        }

        public void run() {
            try {
                HDict meta = grid.meta();
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
                    table.setTableMeta(metaObj);
                }
                HCol ts = grid.col("ts");
                HCol val = grid.col("val");
                Iterator it = grid.iterator();
                HRow row;
                while (it.hasNext()) {
                    row = (HRow) it.next();
                    HDateTime dt = (HDateTime) row.get(ts, false);
                    handler.handle(new QueryData(
                            Utils.hvalToVal(row.get(val, false)),
                            dt.millis()));
                }
            } catch (Exception x) {
                LOGGER.error("", x);
            }
            handler.complete();
        }
    }

}
