package kr.binz.hornetq.consumer;

import java.net.SocketTimeoutException;
import java.util.HashMap;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompAMQConsumer {
	static final Logger LOG = LoggerFactory.getLogger(StompAMQConsumer.class);
	String queueName = "stomp.queue.test1";
	
	public void run(String selector) throws Exception {
		StompConnection connection = new StompConnection();
		connection.open("", 5445);
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
		
		if(selector != null) {
			HashMap<String,String> header = new HashMap<String,String>();
			header.put("selector", "agentKey='"+selector+"'");
			connection.subscribe(queueName, Subscribe.AckModeValues.AUTO, header);
			LOG.info("subscribe: {}", header.get("selector"));
		} else {
			connection.subscribe(queueName, Subscribe.AckModeValues.AUTO);
			LOG.info("subscribe");
		}
		
		int receivedCnt = 0;
		while (receivedCnt < 100) {
			try {
	//			connection.begin("tx2");		     
				StompFrame message = connection.receive();
				LOG.info("RCV[{}]: {}", queueName, message.getBody());
	//			connection.ack(message, "tx2");     
	//			connetion.commit("tx2");
			} catch (SocketTimeoutException e) {
				LOG.error("timeout");
			}
		}
		         
		connection.disconnect();
		LOG.info("Stomp ActiveMQ Consumer closed");
	}
	
	public static void main(String[] args) throws Exception {
		String selector = null;
		if(args.length > 0) {
			selector = args[0];
		}
		new StompAMQConsumer().run(selector);
	}
}
