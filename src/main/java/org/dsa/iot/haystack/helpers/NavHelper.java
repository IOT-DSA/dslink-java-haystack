package org.dsa.iot.haystack.helpers;

import org.dsa.iot.dslink.link.Linkable;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.SubscriptionManager;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.InvokeActions;
import org.projecthaystack.*;
import org.projecthaystack.client.CallErrException;
import org.projecthaystack.io.HZincReader;
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

    public NavHelper(Haystack haystack) {
        this.stpe = Objects.createDaemonThreadPool();
        this.haystack = haystack;
    }

    public void destroy() {
        stpe.shutdownNow();
    }

    Handler<Node> getClosedHandler() {
        return new Handler<Node>() {
            @Override
            public void handle(Node event) {
                event.setRoConfig("lu", new Value(0));
                LOGGER.info("Wants to remove: {}", event.getPath());
                Map<String, Node> children = event.getChildren();
                if (children == null) {
                    return;
                }
                for (Node n : children.values()) {
                    if (n == null
                            || (n.getValue() == null
                                && n.getAction() == null)) {
                        continue;
                    }
                    removeNodes(n);
                }
            }
        };
    }

    private void removeNodes(Node node) {
        if (node == null) {
            return;
        }
        SubscriptionManager man = node.getLink().getSubscriptionManager();
        Map<String, Node> children = node.getChildren();
        if (children != null) {
            for (Node n : children.values()) {
                if (n != null) {
                    removeNodes(node);
                }
            }
        }
        if (!man.hasValueSub(node)) {
            LOGGER.debug("Removed: {}", node.getPath());
            node.getParent().removeChild(node);
        }
    }

    public Handler<Node> getNavHandler(final String navId) {
        return new Handler<Node>() {

            @Override
            public void handle(final Node event) {
                if (event == null) {
                    return;
                }
                Value val = event.getRoConfig("lu");
                long curr = System.currentTimeMillis();
                if (val == null) {
                    val = new Value(0);
                }
                long lastUpdate = val.getNumber().longValue();
                long diff = curr - lastUpdate;
                if (diff < REFRESH_TIME) {
                    return;
                }
                val = new Value(curr);
                event.setRoConfig("lu", val);

                stpe.execute(new Runnable() {
                    @Override
                    public void run() {
                        HGrid grid = HGrid.EMPTY;
                        if (navId != null) {
                            HGridBuilder builder = new HGridBuilder();
                            builder.addCol("navId");
                            builder.addRow(new HVal[] {
                                    HUri.make(navId)
                            });
                            grid = builder.toGrid();

                            String path = event.getPath();
                            LOGGER.info("Navigating: {} ({})", navId, path);
                        } else {
                            LOGGER.info("Navigating root");
                        }

                        try {
                            haystack.call("nav",
                                    grid, new Handler<HGrid>() {
                                @Override
                                public void handle(HGrid nav) {
                                    if (nav == null) {
                                        return;
                                    }
                                    if (LOGGER.isDebugEnabled()) {
                                        StringWriter writer = new StringWriter();
                                        nav.dump(new PrintWriter(writer));
                                        String n = writer.toString();
                                        LOGGER.debug("Received nav: {}", n);
                                    }
                                    iterateNavChildren(nav, event);
                                }
                            });
                        } catch (Exception e) {
                            LOGGER.info("Error navigating children", e);
                        }
                    }
                });
            }
        };
    }

    private void iterateNavChildren(final HGrid nav, Node node) {
        Iterator navIt = nav.iterator();
        while (navIt != null && navIt.hasNext()) {
            final HRow row = (HRow) navIt.next();

            String name = getName(row);
            if (name == null) {
                continue;
            }

            // Handle child
            final NodeBuilder builder = node.createChild(name);
            HVal navId = row.get("navId", false);
            if (navId != null) {
                builder.setHasChildren(true);
            }
            builder.setDisplayName(row.dis());

            final Node child = builder.build();
            child.setSerializable(false);

            // Handle writable
            final HVal writable = row.get("writable", false);
            if (writable instanceof HMarker) {
                HRef id = row.id();
                NodeBuilder b = child.createChild("pointWrite");
                b.setDisplayName("Point Write");
                b.setSerializable(false);

                HVal hKind = row.get("kind", false);
                String kind = hKind.toString();

                b.setAction(Actions.getPointWriteAction(haystack, id, kind));
                b.build();
            }

            // Handle actions
            HVal actions = row.get("actions", false);
            if (actions instanceof HStr) {
                String zinc = ((HStr) actions).val;
                if (!zinc.endsWith("\n")) {
                    zinc += "\n";
                }
                HZincReader reader = new HZincReader(zinc);
                HGrid grid = reader.readGrid();
                Iterator it = grid.iterator();
                HRef id = row.id();
                while (it.hasNext()) {
                    HRow r = (HRow) it.next();
                    InvokeActions.handleAction(haystack, id, child, r);
                }
            }

            // Handle navId
            if (navId != null) {
                String id = navId.toString();
                LOGGER.debug("Received navId of {}", id);

                final Handler<Node> closedHandler = getClosedHandler();
                Handler<Node> navHandler = getNavHandler(id);
                NodeListener listener = child.getListener();
                listener.setOnListClosedHandler(closedHandler);
                listener.setOnListHandler(navHandler);

                HGridBuilder hGridBuilder = new HGridBuilder();
                hGridBuilder.addCol("navId");
                hGridBuilder.addRow(new HVal[]{navId});
                HGrid grid = hGridBuilder.toGrid();
                try {
                    haystack.call("nav", grid, new Handler<HGrid>() {
                        @Override
                        public void handle(HGrid children) {
                            if (LOGGER.isDebugEnabled()) {
                                StringWriter writer = new StringWriter();
                                nav.dump(new PrintWriter(writer));
                                String s = writer.toString();
                                LOGGER.debug("Received nav: {}", s);
                            }

                            if (children != null) {
                                Iterator childrenIt = children.iterator();
                                while (childrenIt.hasNext()) {
                                    final HRow childRow = (HRow) childrenIt.next();
                                    final String childName = getName(childRow);
                                    if (childName == null) {
                                        continue;
                                    }
                                    NodeBuilder b = child.createChild(childName);
                                    b.setDisplayName(childRow.dis());
                                    b.getChild().setSerializable(false);
                                    HVal navId = childRow.get("navId", false);
                                    if (navId != null) {
                                        String id = navId.toString();
                                        Handler<Node> navHandler = getNavHandler(id);
                                        b.setHasChildren(true);
                                        b.setRoConfig("navId", new Value(id));
                                        NodeListener listener = b.getListener();
                                        listener.setOnListClosedHandler(closedHandler);
                                        listener.setOnListHandler(navHandler);
                                    }
                                    iterateRow(b.build(), childRow);
                                }
                            }
                        }
                    });
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
        if (id == null) {
            return;
        }
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
                Linkable link = event.getLink();
                SubscriptionManager man = link.getSubscriptionManager();
                Map<String, Node> children = event.getParent().getChildren();
                if (children != null) {
                    for (Node n : children.values()) {
                        if (man.hasValueSub(n)) {
                            return;
                        }
                    }
                }
                haystack.unsubscribe((HRef) id);
            }
        });
    }

    private String getName(HRow row) {
        HRef id = (HRef) row.get("id", false);
        String name;
        if (id != null) {
            name = id.val;
        } else {
            name = row.dis();
        }
        return StringUtils.encodeName(name);
    }

    static {
        LOGGER = LoggerFactory.getLogger(NavHelper.class);
    }
}
