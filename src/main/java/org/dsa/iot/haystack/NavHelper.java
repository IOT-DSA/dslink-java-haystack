package org.dsa.iot.haystack;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.projecthaystack.*;
import org.projecthaystack.client.CallErrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class NavHelper {

    private static final long REFRESH_TIME = TimeUnit.SECONDS.toMillis(60);
    private static final Logger LOGGER;

    private final ScheduledThreadPoolExecutor stpe;
    private final Haystack haystack;

    NavHelper(Haystack haystack) {
        this.stpe = Objects.createDaemonThreadPool();
        this.haystack = haystack;
    }

    Handler<Node> getNavHandler(final String navId) {
        return new Handler<Node>() {

            private long lastUpdate;

            @Override
            public void handle(final Node event) {
                long curr = System.currentTimeMillis();
                if (curr - lastUpdate < REFRESH_TIME) {
                    return;
                }
                lastUpdate = curr;

                stpe.execute(new Runnable() {
                    @Override
                    public void run() {
                        HGrid grid = HGrid.EMPTY;
                        if (navId != null) {
                            HGridBuilder builder = new HGridBuilder();
                            builder.addCol("navId");
                            builder.addRow(new HVal[]{
                                    HUri.make(navId)
                            });
                            grid = builder.toGrid();
                            String path = event.getPath();
                            LOGGER.info("Navigating: {} ({})", navId, path);
                        } else {
                            LOGGER.info("Navigating root");
                        }

                        try {
                            HGrid nav = haystack.call("nav", grid);
                            if (nav != null) {
                                if (LOGGER.isDebugEnabled()) {
                                    StringWriter writer = new StringWriter();
                                    nav.dump(new PrintWriter(writer));
                                    LOGGER.debug("Received nav: {}", writer.toString());
                                }
                                iterateNavChildren(nav, event);
                            }
                        } catch (Exception e) {
                            LOGGER.info("Error navigating children", e);
                        }
                    }
                });
            }
        };
    }

    private void iterateNavChildren(HGrid nav, Node node) {
        Iterator navIt = nav.iterator();
        while (navIt != null && navIt.hasNext()) {
            final HRow row = (HRow) navIt.next();

            String name = getName(row);
            if (name == null) {
                continue;
            }

            final NodeBuilder builder = node.createChild(name);
            final Node child = builder.build();
            child.setSerializable(false);

            HVal navId = row.get("navId", false);
            if (navId != null) {
                String id = navId.toString();
                LOGGER.debug("Received navId of {}", id);
                Handler<Node> handler = getNavHandler(id);
                child.getListener().setOnListHandler(handler);

                HGridBuilder hGridBuilder = new HGridBuilder();
                hGridBuilder.addCol("navId");
                hGridBuilder.addRow(new HVal[]{navId});
                HGrid grid = hGridBuilder.toGrid();
                try {
                    HGrid children = haystack.call("nav", grid);

                    StringWriter writer = new StringWriter();
                    nav.dump(new PrintWriter(writer));
                    LOGGER.debug("Received nav: {}", writer.toString());

                    if (children != null) {
                        Iterator childrenIt = children.iterator();
                        while (childrenIt.hasNext()) {
                            final HRow childRow = (HRow) childrenIt.next();
                            final String childName = getName(childRow);
                            if (childName != null) {
                                NodeBuilder b = child.createChild(childName);
                                b.getChild().setSerializable(false);
                                navId = childRow.get("navId", false);
                                if (navId != null) {
                                    id = navId.toString();
                                    handler = getNavHandler(id);
                                    b.getChild().setHasChildren(true);
                                    b.getListener().setOnListHandler(handler);
                                }
                                iterateRow(b.build(), childRow);
                            }
                        }
                    }
                } catch (CallErrException e) {
                    LOGGER.info("Error calling nav on ID: {}", id, e);
                }
            }

            iterateRow(child, row);
        }
    }

    private void iterateRow(Node node, HRow row) {
        handleRowValSubs(node, row);
        Iterator it = row.iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String name = (String) entry.getKey();
            if ("id".equals(name)) {
                continue;
            }
            HVal val = (HVal) entry.getValue();
            Value value = Utils.hvalToVal(val);

            Node child = node.createChild(name).build();
            child.setValueType(value.getType());
            child.setValue(value);
        }
    }

    private void handleRowValSubs(final Node node, HRow row) {
        final HVal id = row.get("id", false);
        if (id != null) {
            Node child = node.createChild("id").build();
            Value val = Utils.hvalToVal(id);
            child.setValueType(val.getType());
            child.setValue(val);
            NodeListener listener = child.getListener();
            listener.setOnSubscribeHandler(new Handler<Node>() {
                @Override
                public void handle(Node event) {
                    haystack.subscribe((HRef) id, node);
                }
            });

            listener.setOnUnsubscribeHandler(new Handler<Node>() {
                @Override
                public void handle(Node event) {
                    haystack.unsubscribe((HRef) id);
                }
            });
        }
    }

    private String getName(HRow row) {
        String name = StringUtils.filterBannedChars(row.dis());
        if (name.isEmpty() || "????".equals(name)) {
            return null;
        }
        return name;
    }

    static {
        LOGGER = LoggerFactory.getLogger(NavHelper.class);
    }
}
