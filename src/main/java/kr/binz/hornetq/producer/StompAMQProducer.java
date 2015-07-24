package kr.binz.hornetq.producer;

import java.util.HashMap;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompAMQProducer {
	static final Logger LOG = LoggerFactory.getLogger(StompAMQProducer.class);
	String address = "stomp.address.test";
	
	public void run(String selectors) throws Exception {
		StompConnection connection = new StompConnection();
		connection.open("", 5445);
		LOG.info("Stomp ActiveMQ Producer opened");         
		
		connection.connect("admin", "admin");
		/*
		StompFrame connect = connection.receive();
		if (!connect.getAction().equals(Stomp.Responses.CONNECTED)) {
		    throw new Exception ("Not connected");
		}
		*/
		LOG.info("Stomp ActiveMQ Producer connected");
		String[] arrSelector = null;
		if(selectors != null) {
			arrSelector = StringUtils.split(selectors, ",");
		}
		HashMap<String,String> header = new HashMap<String,String>(); 
		int sendCnt = 0;
		String msg = "";
		while(sendCnt < 100) {
//			connection.begin("tx1");
			msg = "message"+sendCnt;
			header.clear();
			header.put("content-length", String.valueOf(msg.length()));
			if(selectors != null) {
				header.put("selector", "agentKey="+arrSelector[sendCnt%arrSelector.length]);
				header.put("agentKey", arrSelector[sendCnt%arrSelector.length]);
			}
			//connection.send(address, msg, "tx1", header);			
			connection.send(address, msg, null, header);
//			connection.commit("tx1");
			LOG.info("Stomp ActiveMQ Producer send message: {}, {}", sendCnt++, header.get("selector"));
			Thread.sleep(3000L);
		}		         
		connection.disconnect();
		LOG.info("Stomp ActiveMQ Producer closed");
	}
	
	public static void main(String[] args) throws Exception {
		String selectors = null;
		if(args.length > 0) {
			selectors = args[0];
		}
		new StompAMQProducer().run(selectors);
	}
}
