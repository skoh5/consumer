package kr.binz.hornetq.consumer;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompAMQConsumer {
	static final Logger LOG = LoggerFactory.getLogger(StompAMQConsumer.class);
	String queueName = "stomp.queue.test1";
	
	public void run() throws Exception {
		StompConnection connection = new StompConnection();
		connection.open("183.100.209.69", 5445);
		LOG.info("Stomp ActiveMQ Consumer opened");
		         
		connection.connect("admin", "admin");
		/*
		StompFrame connect = connection.receive();
		if (!connect.getAction().equals(Stomp.Responses.CONNECTED)) {
		    throw new Exception ("Not connected");
		}
		*/
		LOG.info("Stomp ActiveMQ Consumer connected");
		         
//		connection.subscribe(queueName, Subscribe.AckModeValues.CLIENT);
		connection.subscribe(queueName, Subscribe.AckModeValues.AUTO);
		int receivedCnt = 0;
		while (receivedCnt < 100) {
//			connection.begin("tx2");		     
			StompFrame message = connection.receive();
			LOG.info("RCV[{}]: {}", queueName, message.getBody());
//			connection.ack(message, "tx2");     
//			connection.commit("tx2");
		}
		         
		connection.disconnect();
		LOG.info("Stomp ActiveMQ Consumer closed");
	}
	
	public static void main(String[] args) throws Exception {
		new StompAMQConsumer().run();
	}
}
