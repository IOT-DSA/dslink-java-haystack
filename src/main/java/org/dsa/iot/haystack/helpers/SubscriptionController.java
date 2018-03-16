package org.dsa.iot.haystack.helpers;

import java.util.HashSet;
import java.util.Set;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.haystack.Haystack;
import org.projecthaystack.HRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionController {
	private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionController.class);
	
	private final Haystack haystack;
	private final Node node;
	private final Set<Node> subscribedChildren = new HashSet<Node>();
	private final SubHandler subHandler = new SubHandler();
	private final UnsubHandler unsubHandler = new UnsubHandler();
	private HRef id;
	
	public SubscriptionController(Node node, Haystack haystack) {
		this.node = node;
		this.haystack = haystack;
	}
	
	synchronized public void childSubscribed(Node child) {
		boolean wasEmpty = subscribedChildren.isEmpty();
		subscribedChildren.add(child);
		if (wasEmpty) {			
			if (id != null) {
				LOGGER.debug("Subscribing " + node.getDisplayName());
				haystack.subscribe(id, node);
			}
		}
	}
	
	synchronized public void childUnsubscribed(Node child) {
		subscribedChildren.remove(child);
		if (subscribedChildren.isEmpty()) {
			if (id != null) {
				LOGGER.debug("Unsubscribing " + node.getDisplayName());
				haystack.unsubscribe(id);
			}
		}
	}
	
	synchronized public void setId(HRef id) {
		this.id = id;
	}
	
	synchronized public HRef getId() {
		return id;
	}
	
	public SubHandler getSubHandler() {
		return subHandler;
	}
	
	public UnsubHandler getUnsubHandler() {
		return unsubHandler;
	}
	
	
	private class SubHandler implements Handler<Node> {
		@Override
		public void handle(Node event) {
			childSubscribed(event);
		}
	}
	
	private class UnsubHandler implements Handler<Node> {
		@Override
		public void handle(Node event) {
			childUnsubscribed(event);
		}
	}

}
