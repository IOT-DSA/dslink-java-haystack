package org.dsa.iot.haystack;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.ServerActions;
import org.dsa.iot.haystack.helpers.ConnectionHelper;
import org.dsa.iot.haystack.helpers.NavHelper;
import org.dsa.iot.haystack.helpers.StateHandler;
import org.projecthaystack.HGrid;
import org.projecthaystack.HGridBuilder;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HVal;
import org.projecthaystack.HWatch;
import org.projecthaystack.client.HClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Haystack {

    private static final Logger LOGGER = LoggerFactory.getLogger(Haystack.class);

    private final ConnectionHelper conn;
    private final NavHelper navHelper;
    private final Node node;
    private Set<HRef> pendingSubscribe;
    private Set<HRef> pendingUnsubscribe;
    private ScheduledFuture<?> pollFuture;
    private final ScheduledThreadPoolExecutor stpe;
    private final Map<String, Node> subs;
    private boolean updating;
    private boolean watchEnabled;

    public Haystack(final Node node) {
        {
            Value val = new Value(0);
            val.setSerializable(false);
            node.setRoConfig("lu", val);
        }
        node.setMetaData(this);

        Value enabled = node.getConfig("enabled");
        if (enabled == null) {
            enabled = new Value(true);
            node.setConfig("enabled", enabled);
        } else if (enabled.getType() == ValueType.NUMBER) {
            enabled = new Value(enabled.getNumber().intValue() != 0);
            node.setConfig("enabled", enabled);
        }

        Value pr;
        if ((pr = node.removeConfig("pr")) != null) {
            node.setConfig("pollRate", pr);
        }
        if (node.getConfig("pollRate") == null) {
            node.setConfig("pollRate", new Value(5));
        }

        Value cto = node.getConfig("connect timeout");
        if (cto == null) {
            node.setConfig("connect timeout", new Value(60));
        }

        Value rto = node.getConfig("read timeout");
        if (rto == null) {
            node.setConfig("read timeout", new Value(60));
        }

        if (node.getConfig("maxConnections") == null) {
            node.setConfig("maxConnections", new Value(5));
        }

        this.stpe = Objects.createDaemonThreadPool();
        this.node = node;
        this.subs = new ConcurrentHashMap<>();
        this.navHelper = new NavHelper(this);
        Utils.getStatusNode(node);
        Utils.initCommon(this, node);
        this.conn = new ConnectionHelper(this, new Handler<Void>() {
            @Override
            public void handle(Void event) {
                watchEnabled = true;
                if (!subs.isEmpty()) {
                    // Restore haystack subscriptions
                    for (Map.Entry<String, Node> entry : subs.entrySet()) {
                        HRef id = HRef.make(entry.getKey());
                        Node node = entry.getValue();
                        subscribe(id, node);
                    }
                }

                setupPoll(getPollRate().getNumber().intValue());
            }
        }, new Handler<Void>() {
            @Override
            public void handle(Void event) {
                watchEnabled = false;
                pollFuture.cancel(false);
                pollFuture = null;
                synchronized (this) {
                    pendingSubscribe = null;
                    pendingUnsubscribe = null;
                }
            }
        });
        if (enabled.getBool()) {
            // Ensure subscriptions are subscribed
            node.setRoConfig("lu", new Value(0));
            conn.getClient(null);
        } else {
            Utils.getStatusNode(node).setValue(new Value("Disabled"));
            stop();
        }
    }

    public void call(final String op,
                     final HGrid grid,
                     final Handler<HGrid> onComplete) {
        if (!isEnabled()) {
            onComplete.handle(null);
            return;
        }
        conn.getClient(new StateHandler<HClient>() {
            @Override
            public void handle(HClient event) {
                HGrid ret = event.call(op, grid);
                if (onComplete != null) {
                    onComplete.handle(ret);
                }
            }
        });
    }

    public void editConnection(String url,
                               String user,
                               String pass,
                               int pollRate,
                               int connTimeout,
                               int readTimeout,
                               int maxConnections,
                               boolean enabled) {
        LOGGER.info("Edit Server url={} user={} enabled={}", url, user, enabled);
        node.setRoConfig("lu", new Value(0));
        stop();
        List<String> list = new ArrayList<>(node.getChildren().keySet());
        for (String name : list) {
            Node tmp = node.getChild(name, false);
            if (tmp == null) {
                tmp = node.getChild(name, true);
            }
            if (tmp == null) {
                continue;
            }
            if (tmp.getAction() != null) {
                continue;
            }
            if (tmp.getRoConfig("navId") != null) {
                if (node.removeChild(name, false) == null) {
                    node.removeChild(name, true);
                }
            }
        }
        if (!enabled) {
            Utils.getStatusNode(node).setValue(new Value("Disabled"));
        } else {
            conn.editConnection(url, user, pass, connTimeout, readTimeout, maxConnections);
            setupPoll(pollRate);
        }

        Action a = ServerActions.getEditAction(node);
        node.getChild("editServer", false).setAction(a);

    }

    public void eval(final String expr, final Handler<HGrid> onComplete) {
        conn.getClient(new StateHandler<HClient>() {
            @Override
            public void handle(HClient event) {
                try {
                    HGrid ret = event.eval(expr);
                    if (onComplete != null) {
                        onComplete.handle(ret);
                    }
                } catch (RuntimeException x) {
                    LOGGER.error(expr, x);
                    throw x;
                }
            }
        });
    }

    public ConnectionHelper getConnHelper() {
        return conn;
    }

    public NavHelper getNavHelper() {
        return navHelper;
    }

    public Node getNode() {
        return node;
    }

    public Value getPollRate() {
        return node.getConfig("pollRate");
    }

    public ScheduledThreadPoolExecutor getStpe() {
        return stpe;
    }

    public int getMaxConnections() {
        Value v = node.getConfig("maxConnections");
        if (v != null) {
            Number n = v.getNumber();
            if (n != null) {
                return n.intValue();
            }
        }
        return 5;
    }

    public void setMaxConnections(int max) {
        if (max < 1) {
            throw new IllegalArgumentException("Max connections must be > 1: " + max);
        }
        node.setConfig("maxConnections", new Value(max));
    }

    public static void init(Node superRoot) {
        NodeBuilder builder = Utils.getBuilder(superRoot, "addServer");
        builder.setDisplayName("Add Server");
        builder.setSerializable(false);
        builder.setAction(ServerActions.getAddServerAction(superRoot)).build();

        Map<String, Node> children = superRoot.getChildren();
        if (children != null) {
            for (Node child : children.values()) {
                if (child.getAction() == null && !"sys".equals(child.getName())) {
                    child.clearChildren();
                    new Haystack(child);
                }
            }
        }

        builder = Utils.getBuilder(superRoot, "eval");
        builder.setDisplayName("Eval");
        builder.setSerializable(false);
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(final ActionResult result) {
                try {
                    Value vUrl = result.getParameter("URL", ValueType.STRING);
                    Value vUser = result.getParameter("Username", ValueType.STRING);
                    Value vPass = result.getParameter("Password");
                    Value vExpr = result.getParameter("Expression", ValueType.STRING);
                    Value vConnect = result.getParameter("Connect Timeout", ValueType.NUMBER);
                    Value vRead = result.getParameter("Read Timeout", ValueType.NUMBER);
                    HClient client = HClient.open(
                            vUrl.getString(),
                            vUser.getString(),
                            vPass.getString(),
                            vConnect.getNumber().intValue() * 1000,
                            vRead.getNumber().intValue() * 1000);
                    HGrid grid = client.eval(vExpr.getString());
                    if (grid != null) {
                        Actions.buildTable(grid, result, false);
                    } else {
                        result.getTable().close();
                    }
                } catch (RuntimeException x) {
                    LOGGER.error("Root Eval Action", x);
                    throw x;
                }
            }
        });
        a.addParameter(new Parameter("Expression", ValueType.STRING));
        a.addParameter(new Parameter("URL", ValueType.STRING));
        a.addParameter(new Parameter("Username", ValueType.STRING));
        Parameter p = new Parameter("Password", ValueType.STRING);
        p.setEditorType(EditorType.PASSWORD);
        a.addParameter(p);

        p = new Parameter("Connect Timeout", ValueType.NUMBER);
        p.setDefaultValue(new Value(30));
        p.setDescription("Seconds");
        a.addParameter(p);

        p = new Parameter("Read Timeout", ValueType.NUMBER);
        p.setDefaultValue(new Value(30));
        p.setDescription("Seconds");
        a.addParameter(p);

        a.setResultType(ResultType.TABLE);
        builder.setAction(a).build();
    }

    public boolean isEnabled() {
        Value enabled = node.getConfig("enabled");
        if (enabled != null) {
            return enabled.getBool();
        }
        return false;
    }

    public void nav(final HVal navId, final Handler<HGrid> onComplete) {
        if (!isEnabled()) {
            return;
        }
        HGrid grid = HGrid.EMPTY;
        if (navId != null) {
            HGridBuilder builder = new HGridBuilder();
            builder.addCol("navId");
            builder.addRow(new HVal[]{navId});
            grid = builder.toGrid();
        }
        call("nav", grid, onComplete);
    }

    public void read(final String filter,
                     final int limit,
                     final Handler<HGrid> onComplete) {
        conn.getClient(new StateHandler<HClient>() {
            @Override
            public void handle(HClient event) {
                HGrid ret = event.readAll(filter, limit);
                if (onComplete != null) {
                    onComplete.handle(ret);
                }
            }
        });
    }

    public void stop() {
        if (pollFuture != null) {
            try {
                pollFuture.cancel(true);
            } catch (Exception ignored) {
            }
        }

        conn.close();
    }

    public synchronized void subscribe(final HRef id, Node node) {
        subs.put(id.toString(), node);
        if (!isEnabled() || !watchEnabled) {
            return;
        }
        if (pendingSubscribe == null) {
            pendingSubscribe = new HashSet<>();
        }
        if (pendingUnsubscribe != null) {
            pendingUnsubscribe.remove(id);
        }
        if (pendingSubscribe.add(id)) {
            stpe.schedule(new Runnable() {
                @Override
                public void run() {
                    updateSubscriptions();
                }
            }, 1, TimeUnit.SECONDS);
        }
    }

    public synchronized void unsubscribe(final HRef id) {
        subs.remove(id.toString());
        if (!isEnabled() || !watchEnabled) {
            return;
        }
        if (pendingUnsubscribe == null) {
            pendingUnsubscribe = new HashSet<>();
        }
        if (pendingSubscribe != null) {
            pendingSubscribe.remove(id);
        }
        if (pendingUnsubscribe.add(id)) {
            stpe.schedule(new Runnable() {
                @Override
                public void run() {
                    updateSubscriptions();
                }
            }, 1, TimeUnit.SECONDS);
        }
    }

    void destroy() {
        stop();
        stpe.shutdownNow();
        navHelper.destroy();
    }

    private void poll() {
        if (!isEnabled() || !watchEnabled || subs.isEmpty()) {
            return;
        }

        conn.getWatch(new StateHandler<HWatch>() {
            @Override
            public void handle(HWatch event) {
                HGrid grid = event.pollChanges();
                if (grid == null) {
                    return;
                }

                Iterator<?> it = grid.iterator();
                while (it.hasNext()) {
                    HRow row = (HRow) it.next();
                    Node node = subs.get(row.id().toString());
                    if (node != null) {
                        Map<String, Node> children = node.getChildren();

                        Iterator<?> rowIt = row.iterator();
                        while (rowIt.hasNext()) {
                            Map.Entry entry = (Map.Entry) rowIt.next();
                            String name = (String) entry.getKey();
                            HVal val = (HVal) entry.getValue();
                            Value value = Utils.hvalToVal(val);

                            String encoded = StringUtils.encodeName(name);
                            Node child = null;
                            if (children != null) {
                                child = children.get(encoded);
                            }
                            if (child != null) {
                                child.setValueType(value.getType());
                                child.setValue(value);
                            } else {
                                NodeBuilder b = Utils.getBuilder(node, encoded);
                                b.setValueType(value.getType());
                                b.setValue(value);
                                Node n = b.build();
                                n.setSerializable(false);
                            }
                        }
                    }
                }
            }
        });
    }

    private void setupPoll(int time) {
        if (pollFuture != null) {
            pollFuture.cancel(false);
            pollFuture = null;
        }

        pollFuture = stpe.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    poll();
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
        }, time, time, TimeUnit.SECONDS);
    }

    private void updateSubscriptions() {
        Set<HRef> toSubscribe;
        Set<HRef> toUnsubscribe;
        synchronized (this) {
            if (updating) {
                return;
            }
            updating = true;
            toSubscribe = pendingSubscribe;
            toUnsubscribe = pendingUnsubscribe;
            pendingSubscribe = null;
            pendingUnsubscribe = null;
        }
        try {
            while ((toSubscribe != null) || (toUnsubscribe != null)) {
                if ((toSubscribe != null) && !toSubscribe.isEmpty()) {
                    final HRef[] ids = new HRef[toSubscribe.size()];
                    toSubscribe.toArray(ids);
                    conn.getWatch(new StateHandler<HWatch>() {
                        @Override
                        public void handle(HWatch event) {
                            event.sub(ids);
                        }
                    });
                }
                if ((toUnsubscribe != null) && !toUnsubscribe.isEmpty()) {
                    final HRef[] ids = new HRef[toUnsubscribe.size()];
                    toUnsubscribe.toArray(ids);
                    conn.getWatch(new StateHandler<HWatch>() {
                        @Override
                        public void handle(HWatch event) {
                            event.unsub(ids);
                        }
                    });
                }
                synchronized (this) {
                    toSubscribe = pendingSubscribe;
                    toUnsubscribe = pendingUnsubscribe;
                    pendingSubscribe = null;
                    pendingUnsubscribe = null;
                }
            }
        } finally {
            synchronized (this) {
                updating = false;
            }
        }
    }
}
