package org.dsa.iot.haystack.helpers;

import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.projecthaystack.*;
import org.projecthaystack.client.HClient;
import org.dsa.iot.dslink.util.handler.Handler;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class SubHelper {

    private final ScheduledThreadPoolExecutor stpe;
    private final Haystack haystack;
    private final HRef id;

    private ScheduledFuture<?> future;
    private boolean running = true;

    public SubHelper(Haystack haystack, String id) {
        this.haystack = haystack;
        this.stpe = haystack.getStpe();
        this.id = Utils.idToRef(id);
    }

    public void start(final Table table, final int pollRate) {
        ConnectionHelper helper = haystack.getConnHelper();
        final Thread thread = Thread.currentThread();
        final CountDownLatch latch = new CountDownLatch(1);
        helper.getClient(new Handler<HClient>() {
            @Override
            public void handle(final HClient client) {
                if (!running) {
                    return;
                }

                final List<String> cols = new LinkedList<>();
                try {
                    HDict data = client.readById(id);
                    Iterator it = data.iterator();
                    Row row = new Row();
                    while (it.hasNext()) {
                        Map.Entry col = (Map.Entry) it.next();
                        String name = (String) col.getKey();
                        Parameter p = new Parameter(name, ValueType.DYNAMIC);
                        table.addColumn(p);
                        cols.add(name);

                        HVal val = (HVal) col.getValue();
                        row.addValue(Utils.hvalToVal(val));
                    }
                    table.addRow(row);
                    latch.countDown();
                } catch (Exception e) {
                    thread.notify();
                    throw new RuntimeException(e);
                }

                final String uid = UUID.randomUUID().toString();
                final WatchCont watchCont = new WatchCont();
                watchCont.watch = client.watchOpen(uid, null);
                watchCont.watch.sub(new HRef[]{id});
                future = stpe.scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            HWatch watch = watchCont.watch;
                            HGrid grid = watch.pollChanges();
                            handleGrid(table, grid, cols);
                        } catch (RuntimeException e) {
                            if (running) {
                                start(table, pollRate);
                            }
                        }
                    }
                }, 0, pollRate, TimeUnit.SECONDS);
            }
        });
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                String err = "Failed to retrieve columns and data";
                throw new RuntimeException(err);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        running = false;
        if (future != null) {
            future.cancel(false);
            future = null;
        }
    }

    private void handleGrid(Table table, HGrid grid, List<String> cols) {
        if (grid == null || grid.isEmpty()) {
            return;
        }

        Row row = new Row();
        for (String c : cols) {
            HRow hRow = grid.row(0);
            HVal val = hRow.get(c, false);
            if (val != null) {
                row.addValue(Utils.hvalToVal(val));
            } else {
                row.addValue(null);
            }
        }
        table.addRow(row);
    }

    private static class WatchCont {
        private HWatch watch;
    }
}
