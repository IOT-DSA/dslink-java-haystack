package org.dsa.iot.haystack.handlers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.Haystack;
import org.dsa.iot.haystack.Utils;
import org.dsa.iot.haystack.helpers.NavHelper;
import org.projecthaystack.HGrid;
import org.projecthaystack.HUri;
import org.projecthaystack.HVal;
import org.projecthaystack.io.HZincReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class ListHandler implements Handler<Node> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListHandler.class);
    private static final ListHandler HANDLER = new ListHandler();
    public static final long REFRESH_TIME = TimeUnit.SECONDS.toMillis(60);

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
        if (!Utils.shouldUpdateList(event)) {
            return;
        }
        final NavHelper helper = haystack.getNavHelper();
        final Value vNav = event.getRoConfig("navId");
        final HVal navId;
        if (vNav != null) {
            String navIdZinc = vNav.getString();
            HVal navIdtemp;
            try {
                navIdtemp = new HZincReader(navIdZinc).readVal();
            } catch (Exception e) {
                navIdtemp = HUri.make(navIdZinc);
            }
            navId = navIdtemp;
        } else {
            navId = null;
        }

        Value val = new Value(System.currentTimeMillis());
        val.setSerializable(false);
        event.setRoConfig("lu", val);

        ScheduledThreadPoolExecutor stpe = helper.getStpe();
        stpe.execute(new Runnable() {
            @Override
            public void run() {
                if (navId != null) {
                    String path = event.getPath();
                    LOGGER.debug("Navigating: {} ({})", navId, path);
                } else {
                    LOGGER.debug("Navigating root");
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
