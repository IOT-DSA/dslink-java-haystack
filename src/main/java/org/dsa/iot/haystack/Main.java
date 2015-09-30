package org.dsa.iot.haystack;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.haystack.actions.Actions;
import org.dsa.iot.haystack.actions.InvokeActions;
import org.projecthaystack.*;
import org.projecthaystack.client.HClient;
import org.projecthaystack.io.HZincReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dsa.iot.dslink.util.handler.Handler;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private DSLink link;

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

        Node node = manager.getNode(path, true).getNode();
        HRef id;
        {
            String sID = split[split.length - 2];
            sID = StringUtils.decodeName(sID);
            id = HRef.make(sID);
        }

        Node superRoot = manager.getSuperRoot();
        Haystack haystack = superRoot.getChild(split[0]).getMetaData();
        haystack.subscribe(id, node.getParent());
        return node;
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
        final Haystack haystack = superRoot.getChild(split[0]).getMetaData();
        final String actName = StringUtils.decodeName(split[split.length - 1]);

        final CountDownLatch latch = new CountDownLatch(1);
        final Container container = new Container();
        switch (actName) {
            case "pointWrite": {
                haystack.getConnHelper().getClient(new Handler<HClient>() {
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
                        NodeBuilder b = node.createChild("pointWrite");
                        b.setDisplayName("Point Write");
                        b.setSerializable(false);
                        b.setAction(Actions.getPointWriteAction(haystack, id, kind));
                        container.node = b.build();
                        latch.countDown();
                    }
                });
                break;
            }
            default: {
                haystack.getConnHelper().getClient(new Handler<HClient>() {
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
                        Iterator it = grid.iterator();
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
                            container.node = node.getChild(name);
                            break;
                        }
                        if (doThrow) {
                            String err = "Action " + actName + " does not exist";
                            throw new RuntimeException(err);
                        }
                        latch.countDown();
                    }
                });
                break;
            }
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
