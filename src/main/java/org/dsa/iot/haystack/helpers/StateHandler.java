package org.dsa.iot.haystack.helpers;

import java.util.concurrent.atomic.AtomicInteger;
import org.dsa.iot.dslink.util.handler.Handler;

/**
 * @author Samuel Grenier
 */
public abstract class StateHandler<T> implements Handler<T> {

    private final AtomicInteger retryCount = new AtomicInteger();

    public int incrementRetryCount() {
        return retryCount.getAndIncrement();
    }
}
