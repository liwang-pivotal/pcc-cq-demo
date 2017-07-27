package io.pivotal.listener;

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

public class CustomerCQListener implements CqListener {
	
	Logger logger =  LoggerFactory.getLogger(this.getClass());
	
    private SimpMessagingTemplate webSocket;
	
	public CustomerCQListener(SimpMessagingTemplate webSocket) {
		this.webSocket = webSocket;
	}

	@Override
	public void onEvent(CqEvent e) {
		try {
			
			webSocket.convertAndSend("/topic/cq_log", "<b>[CQ Event]</b> " + e.getNewValue());
	
		} catch (Exception ex) {
			logger.info("Exception is: " + ex);
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void onError(CqEvent aCqEvent) {
	}
	
}
