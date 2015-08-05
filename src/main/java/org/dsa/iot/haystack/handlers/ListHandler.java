package org.dsa.iot.haystack.handlers;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.helpers.NavHelper;
import org.projecthaystack.HGrid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class ListHandler implements Handler<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListHandler.class);
    private static final long REFRESH_TIME = TimeUnit.SECONDS.toMillis(60);
    private static final ListHandler HANDLER = new ListHandler();

    private ListHandler() {
    }

    @Override
    public void handle(final Node event) {
        if (event == null) {
            return;
        }
        final Haystack haystack = event.getMetaData();
        if (haystack == null) {
            return;
        }
        final NavHelper helper = haystack.getNavHelper();
        final Value vNav = event.getRoConfig("navId");
        final String navId;
        if (vNav != null) {
            navId = vNav.getString();
        } else {
            navId = null;
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

        ScheduledThreadPoolExecutor stpe = helper.getStpe();
        stpe.execute(new Runnable() {
            @Override
            public void run() {
                if (navId != null) {
                    String path = event.getPath();
                    LOGGER.info("Navigating: {} ({})", navId, path);
                } else {
                    LOGGER.info("Navigating root");
                }

                try {
                    haystack.nav(navId, new Handler<HGrid>() {
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
                            helper.iterateNavChildren(nav, event, true);
                        }
                    });
                } catch (Exception e) {
                    LOGGER.warn("Error navigating children", e);
                }
            }
        });
    }

    public static ListHandler get() {
        return HANDLER;
    }
}
