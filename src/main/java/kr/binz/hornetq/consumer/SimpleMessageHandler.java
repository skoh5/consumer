package kr.binz.hornetq.consumer;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;

public class SimpleMessageHandler implements MessageHandler {

	public void onMessage(ClientMessage message) {
		try {
			message.acknowledge();
			System.out.println("receive msg: " + message.getBodyBuffer().readString());
		} catch (HornetQException e) {
			e.printStackTrace();
		}
	}
}
