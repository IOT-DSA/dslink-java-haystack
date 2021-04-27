package org.dsa.iot.haystack.helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeListener;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.GetHistory;
import org.dsa.iot.haystack.actions.InvokeActions;
import org.dsa.iot.haystack.handlers.ClosedHandler;
import org.dsa.iot.haystack.handlers.ListHandler;
import org.projecthaystack.HGrid;
import org.projecthaystack.HMarker;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HStr;
import org.projecthaystack.HTimeZone;
import org.projecthaystack.HVal;
import org.projecthaystack.io.HZincReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class NavHelper {

    private static final Logger LOGGER;

    private final ScheduledThreadPoolExecutor stpe;
    private final Haystack haystack;
    private final Map<Node, SubscriptionController> subControllers = new HashMap<>();

    public NavHelper(Haystack haystack) {
        this.stpe = Objects.createDaemonThreadPool();
        this.haystack = haystack;
    }

    public void destroy() {
        stpe.shutdownNow();
    }

    public ScheduledThreadPoolExecutor getStpe() {
        return stpe;
    }

    public void iterateNavChildren(final HGrid nav,
                                   final Node node,
                                   boolean continueNav) {
        Iterator<?> navIt = nav.iterator();
        List<HRow> equipRefs = new ArrayList<>();
        while (navIt != null && navIt.hasNext()) {
            final HRow row = (HRow) navIt.next();

            HVal val = row.get("equipRef", false);
            if (val != null) {
                equipRefs.add(row);
                continue;
            }

            String name = getName(row);
            if (name == null) {
                continue;
            }

            // Handle child
            String encoded = StringUtils.encodeName(name);
            NodeBuilder builder = Utils.getBuilder(node, encoded);
            HVal navId = row.get("navId", false);
            if (navId != null) {
                builder.setHasChildren(true);
            }

            HVal dis = row.get("navName", false);
            if (dis == null) {
                dis = row.get("dis", false);
            }
            if (dis != null) {
                builder.setDisplayName(dis.toString());
            } else if (!encoded.equals(name)) {
                builder.setDisplayName(name);
            }

            builder.setSerializable(false);
            final Node child = builder.build();

            // Handle writable
            final HVal writable = row.get("writable", false);
            if (writable instanceof HMarker) {
                HRef id = row.id();
                HVal hKind = row.get("kind", false);
                String kind = hKind.toString();
                Actions.getPointWriteAction(haystack, child, id, kind);
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
                Iterator<?> it = grid.iterator();
                HRef id = row.id();
                while (it.hasNext()) {
                    HRow r = (HRow) it.next();
                    InvokeActions.handleAction(haystack, id, child, r);
                }
            }

            // Handle navId
            if (navId != null) {
                LOGGER.debug("Received navId of {}", navId);

                // Ensure proper data is attached to child
                child.setRoConfig("navId", new Value(navId.toZinc()));
                child.setMetaData(haystack);

                // Navigate a level deeper
                if (continueNav) {
                    haystack.nav(navId, new Handler<HGrid>() {
                        @Override
                        public void handle(HGrid event) {
                            iterateNavChildren(event, child, false);
                        }
                    });
                }

                NodeListener listener = child.getListener();
                listener.setOnListClosedHandler(ClosedHandler.get());
                listener.setOnListHandler(ListHandler.get());
            }

            iterateRow(child, row);
        }

        for (HRow row : equipRefs) {
            String name = getName(row);
            if (name == null) {
                continue;
            }

            // Handle child
            HVal val = row.get("equipRef");
            String ref = ((HRef) val).val;
            String encoded = StringUtils.encodeName(ref);
            Node n = node.getParent().getChild(encoded, false);
            if (n == null) {
                if (encoded.equals(node.getName())) {
                    n = node;
                } else {
                    n = node.createChild(encoded, false).build();
                }
            }

            encoded = StringUtils.encodeName(name);
            NodeBuilder builder = Utils.getBuilder(n, encoded);
            HVal navId = row.get("navId", false);
            if (navId != null) {
                builder.setHasChildren(true);
            }

            HVal dis = row.get("navName", false);
            if (dis == null) {
                dis = row.get("dis", false);
            }
            if (dis != null) {
                builder.setDisplayName(dis.toString());
            } else if (!encoded.equals(name)) {
                builder.setDisplayName(name);
            }

            builder.setSerializable(false);
            final Node child = builder.build();

            // Handle writable
            final HVal writable = row.get("writable", false);
            if (writable instanceof HMarker) {
                HRef id = row.id();
                HVal hKind = row.get("kind", false);
                String kind = hKind.toString();
                Actions.getPointWriteAction(haystack, child, id, kind);
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
                Iterator<?> it = grid.iterator();
                HRef id = row.id();
                while (it.hasNext()) {
                    HRow r = (HRow) it.next();
                    InvokeActions.handleAction(haystack, id, child, r);
                }
            }

            // Handle navId
            if (navId != null) {
                LOGGER.debug("Received navId of {}", navId);

                // Ensure proper data is attached to child
                child.setRoConfig("navId", new Value(navId.toZinc()));
                child.setMetaData(haystack);

                // Navigate a level deeper
                if (continueNav) {
                    haystack.nav(navId, new Handler<HGrid>() {
                        @Override
                        public void handle(HGrid event) {
                            iterateNavChildren(event, child.getParent(), false);
                        }
                    });
                }

                NodeListener listener = child.getListener();
                listener.setOnListClosedHandler(ClosedHandler.get());
                listener.setOnListHandler(ListHandler.get());
            }

            iterateRow(child, row);
        }
    }

    private void iterateRow(Node node, HRow row) {
        SubscriptionController subController = getSubController(node, row);
        Iterator<?> it = row.iterator();
        Node curVal = null;
        Node his = null;
        String id = null;
        String kind = null;
        String tz = null;
        boolean writable = false;
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String name = (String) entry.getKey();
            HVal val = (HVal) entry.getValue();
            Value value = Utils.hvalToVal(val);

            if (value == null) {
                continue;
            }

            Node child = node.getChild(name, true);
            if (child == null) {
                child = node.createChild(name, true).build();
            }
            child.setValueType(value.getType());
            child.setValue(value);
            boolean hasAction = false;
            switch (name) {
                case "curVal":
                    curVal = child;
                    hasAction = true;
                    break;
                case "his":
                    if (val instanceof HMarker) {
                        his = child;
                        hasAction = true;
                    }
                    break;
                case "id":
                    id = val.toString();
                    break;
                case "kind":
                    kind = val.toString();
                    break;
                case "tz":
                    tz = val.toString();
                    break;
                case "writable":
                    writable = val instanceof HMarker;
                    break;
                default:
                    child.setHasChildren(false);
            }
            if (!hasAction) {
                child.setHasChildren(false);
            }

            NodeListener listener = child.getListener();
            listener.setOnSubscribeHandler(subController.getSubHandler());
            listener.setOnUnsubscribeHandler(subController.getUnsubHandler());
            if (child.getLink().getSubscriptionManager().hasValueSub(child)) {
                subController.childSubscribed(child);
            }
        }
        if (id != null) { //add getHistory and set
            HRef hid = HRef.make(id);
            if (his != null) {
                HTimeZone htz = null;
                if (tz != null) {
                    htz = HTimeZone.make(tz, false);
                }
                new GetHistory(his, haystack, hid, htz);
                //add to parent, but sometimes parent shows in metrics rather than nav tree
                //new GetHistory(node, haystack, hid, htz);
            }
            if ((curVal != null) && writable && (kind != null)) {
                Actions.getSetAction(haystack, curVal, hid, kind);
            }
        }
    }

    private SubscriptionController getSubController(Node node, HRow row) {
        SubscriptionController subController = subControllers.get(node);
        if (subController == null) {
            subController = new SubscriptionController(node, haystack);
            subControllers.put(node, subController);
        }

        final HVal id = row.get("id", false);
        if (id != null) {
            subController.setId((HRef) id);
        }

        return subController;
    }

    private String getDisplayName(HRow row) {
        HVal val = row.get("navName", false);
        if (val != null) {
            return val.toString();
        }
        return getName(row);
    }

    private String getName(HRow row) {
        HRef id = (HRef) row.get("id", false);
        String name = null;
        if (id != null) {
            name = id.val;
        } else {
            name = row.dis();
        }
        return name;
    }

    static {
        LOGGER = LoggerFactory.getLogger(NavHelper.class);
    }
}
