package org.dsa.iot.haystack;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.GetHistory;
import org.dsa.iot.haystack.actions.InvokeActions;
import org.dsa.iot.haystack.helpers.StateHandler;
import org.projecthaystack.HDict;
import org.projecthaystack.HGrid;
import org.projecthaystack.HRef;
import org.projecthaystack.HRow;
import org.projecthaystack.HStr;
import org.projecthaystack.HTimeZone;
import org.projecthaystack.HVal;
import org.projecthaystack.client.HClient;
import org.projecthaystack.io.HZincReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private DSLink link;
    private final Object subFailLock = new Object();

    @Override
    public boolean isResponder() {
        return true;
    }

    @Override
    public void stop() {
        super.stop();
        if (link != null) {
            Node root = link.getNodeManager().getSuperRoot();
            Map<String, Node> children = root.getChildren();
            if (children != null) {
                for (Node node : children.values()) {
                    Haystack haystack = node.getMetaData();
                    if (haystack != null) {
                        haystack.destroy();
                    }
                }
            }
        }
    }

    @Override
    public void onResponderInitialized(DSLink link) {
        this.link = link;
        LOGGER.info("Connected");
        Node superRoot = link.getNodeManager().getSuperRoot();
        Haystack.init(superRoot);
    }

    @Override
    public Node onSubscriptionFail(String path) {
        NodeManager manager = link.getNodeManager();
        String[] split = NodeManager.splitPath(path);
        Node superRoot = manager.getSuperRoot();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignore) {
        }
        synchronized (subFailLock) {
            Node node = manager.getNode(path, false, false).getNode();
            if (node != null) {
                return node;
            }
            Node n = superRoot;
            int i = 0;
            while (i < split.length) {
                Node next = n.getChild(split[i], false);
                int tries = 0;
                while (next == null && tries < 10) {
                    tries++;
                    try {
                        subFailLock.wait(250);
                    } catch (InterruptedException ignore) {
                    }
                    next = n.getChild(split[i], false);
                }
                if (next == null) {
                    return null;
                }
                n = next;
                n.getListener().postListUpdate();
                i++;
            }
            return n;
        }
    }

    @Override
    public Node onInvocationFail(final String path) {
        final String[] split = NodeManager.splitPath(path);

        final HRef id;
        {
            String sID = split[split.length - 2];
            sID = StringUtils.decodeName(sID);
            id = HRef.make(sID);
        }

        final NodeManager manager = link.getNodeManager();
        final Node superRoot = manager.getSuperRoot();
        final Haystack haystack = superRoot.getChild(split[0], true).getMetaData();
        final String actName = StringUtils.decodeName(split[split.length - 1]);

        final CountDownLatch latch = new CountDownLatch(1);
        final Container container = new Container();
        if ("getHistory".equals(actName)) {
            haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                @Override
                public void handle(HClient event) {
                    final HRef id;
                    {
                        String sID = split[split.length - 3];
                        sID = StringUtils.decodeName(sID);
                        id = HRef.make(sID);
                    }

                    HDict dict = event.readById(id);
                    HVal tz = dict.get("tz", false);
                    HTimeZone htz = null;
                    if (tz != null) {
                        htz = HTimeZone.make(tz.toString(), false);
                    }

                    String[] pSplit = Arrays.copyOf(split, split.length - 1);
                    String parent = StringUtils.join(pSplit, "/");
                    Node node = manager.getNode(parent, true).getNode();
                    container.node = new GetHistory(node, haystack, HRef.make(id.toString()), htz)
                            .getActionNode();
                    latch.countDown();
                }
            });
        } else if ("set".equals(actName)) {
            haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                @Override
                public void handle(HClient event) {
                    HDict dict = event.readById(id);
                    HVal hKind = dict.get("kind", false);
                    String kind = null;
                    if (hKind != null) {
                        kind = hKind.toString();
                    }

                    String[] pSplit = Arrays.copyOf(split, split.length - 1);
                    String parent = StringUtils.join(pSplit, "/");
                    Node node = manager.getNode(parent, true).getNode();
                    container.node = Actions.getSetAction(haystack, node, id, kind);
                    latch.countDown();
                }
            });
        } else if ("pointWrite".equals(actName)) {
            haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                @Override
                public void handle(HClient event) {
                    HDict dict = event.readById(id);
                    HVal hKind = dict.get("kind", false);
                    String kind = null;
                    if (hKind != null) {
                        kind = hKind.toString();
                    }

                    String[] pSplit = Arrays.copyOf(split, split.length - 1);
                    String parent = StringUtils.join(pSplit, "/");
                    Node node = manager.getNode(parent, true).getNode();
                    container.node = Actions.getPointWriteAction(haystack, node, id, kind);
                    latch.countDown();
                }
            });
        } else {
            haystack.getConnHelper().getClient(new StateHandler<HClient>() {
                @Override
                public void handle(HClient event) {
                    HDict dict = event.readById(id);
                    HVal actions = dict.get("actions");
                    String zinc = ((HStr) actions).val;
                    if (!zinc.endsWith("\n")) {
                        zinc += "\n";
                    }
                    HZincReader reader = new HZincReader(zinc);
                    HGrid grid = reader.readGrid();
                    Iterator<?> it = grid.iterator();
                    boolean doThrow = true;
                    while (it.hasNext()) {
                        HRow r = (HRow) it.next();
                        if (!actName.equals(r.dis())) {
                            continue;
                        }
                        String[] pSplit = Arrays.copyOf(split, split.length - 1);
                        String parent = StringUtils.join(pSplit, "/");
                        Node node = manager.getNode(parent, true).getNode();
                        InvokeActions.handleAction(haystack, id, node, r);
                        doThrow = false;

                        String name = split[split.length - 1];
                        name = StringUtils.encodeName(name);
                        container.node = node.getChild(name, false);
                        break;
                    }
                    if (doThrow) {
                        String err = "Action " + actName + " does not exist";
                        throw new RuntimeException(err);
                    }
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return container.node;
    }

    public static void main(String[] args) {
        DSLinkFactory.start(args, new Main());
    }

    private static class Container {

        Node node;
    }
}
