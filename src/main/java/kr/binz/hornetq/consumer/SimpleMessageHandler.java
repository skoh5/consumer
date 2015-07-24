package kr.binz.hornetq.consumer;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMessageHandler implements MessageHandler {
	static final Logger LOG = LoggerFactory.getLogger(SimpleMessageHandler.class);
	public void onMessage(ClientMessage message) {
//		try {
//			message.acknowledge();			
			LOG.info("receive msg: {}", message);
			HornetQBuffer buf = message.getBodyBuffer();
			int size = message.getBodySize();
			byte[] bytes = new byte[size];
			buf.readBytes(bytes, 0, size);
			LOG.info("out: {}", new String(bytes));
			/*
		} catch (HornetQException e) {
			e.printStackTrace();
		}
		*/
	}
}
