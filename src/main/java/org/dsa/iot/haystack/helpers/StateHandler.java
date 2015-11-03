package org.dsa.iot.haystack.helpers;

import org.dsa.iot.dslink.util.handler.Handler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Samuel Grenier
 */
public abstract class StateHandler<T> implements Handler<T> {

    private AtomicInteger retryCount = new AtomicInteger();

    public int incrementRetryCount() {
        return retryCount.getAndIncrement();
    }
}
