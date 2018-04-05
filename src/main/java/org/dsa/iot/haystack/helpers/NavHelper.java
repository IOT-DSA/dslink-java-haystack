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
import org.dsa.iot.haystack.handlers.ClosedHandler;
import org.dsa.iot.haystack.handlers.ListHandler;
import org.projecthaystack.*;
import org.projecthaystack.io.HZincReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dsa.iot.dslink.util.handler.Handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author Samuel Grenier
 */
public class NavHelper {

    private static final Logger LOGGER;

    private final ScheduledThreadPoolExecutor stpe;
    private final Haystack haystack;
    private final Map<Node, SubscriptionController> subControllers = new HashMap<Node, SubscriptionController>();

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
            NodeBuilder builder = Utils.getBuilder(node, name);
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
            }

            builder.setSerializable(false);
            final Node child = builder.build();

            // Handle writable
            final HVal writable = row.get("writable", false);
            if (writable instanceof HMarker) {
                HRef id = row.id();
                NodeBuilder b = Utils.getBuilder(child, "pointWrite");
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
                Iterator<?> it = grid.iterator();
                HRef id = row.id();
                while (it.hasNext()) {
                    HRow r = (HRow) it.next();
                    InvokeActions.handleAction(haystack, id, child, r);
                }
            }

            // Handle navId
            if (navId != null) {
                LOGGER.debug("Received navId of {}", navId.toString());

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
            String ref = StringUtils.encodeName(((HRef) val).val);
            Node n = node.getParent().getChild(ref);
            if (n == null) {
                n = node.createChild(ref).build();
            }

            NodeBuilder builder = Utils.getBuilder(n, name);
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
            }

            builder.setSerializable(false);
            final Node child = builder.build();

            // Handle writable
            final HVal writable = row.get("writable", false);
            if (writable instanceof HMarker) {
                HRef id = row.id();
                NodeBuilder b = Utils.getBuilder(child, "pointWrite");
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
                Iterator<?> it = grid.iterator();
                HRef id = row.id();
                while (it.hasNext()) {
                    HRow r = (HRow) it.next();
                    InvokeActions.handleAction(haystack, id, child, r);
                }
            }

            // Handle navId
            if (navId != null) {
                LOGGER.debug("Received navId of {}", navId.toString());

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
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String name = (String) entry.getKey();
            HVal val = (HVal) entry.getValue();
            Value value = Utils.hvalToVal(val);
            
            if (value == null) {
            	continue;
            }

            Node child = node.getChild(name);
            if (child == null) {
            	child = node.createChild(name).build();
            }
            child.setValueType(value.getType());
            child.setValue(value);
            
            NodeListener listener = child.getListener();
            listener.setOnSubscribeHandler(subController.getSubHandler());
            listener.setOnUnsubscribeHandler(subController.getUnsubHandler());
            if (child.getLink().getSubscriptionManager().hasValueSub(child)) {
            	subController.childSubscribed(child);
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
