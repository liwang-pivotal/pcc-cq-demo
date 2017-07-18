package io.pivotal.listener;

import org.apache.geode.cache.query.CqEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.gemfire.listener.ContinuousQueryListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class CustomerCQListener implements ContinuousQueryListener {
	
	Logger logger =  LoggerFactory.getLogger(this.getClass());
	
	@Autowired
    private SimpMessagingTemplate webSocket;

	@Override
	public void onEvent(CqEvent e) {
		logger.info("hello world");
		try {
			webSocket.convertAndSend("/topic/cq_log", "####### Catched a event! " + e);
	
		} catch (Exception ex) {
			logger.info("Exception is: " + ex);
		}
	}
	
}
