package kr.binz.hornetq.consumer;

import java.util.Map;

import kr.binz.hornetq.producer.Producer;

import org.apache.commons.lang3.StringUtils;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Hello world!
 *
 */
public class Consumer {

	static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	
	private String key;
	private String host;
	private String address = "kr.binz";
	private String queueName = "queue.agent";
	private ServerLocator locator;
	private ClientSession session;
	private ClientSessionFactory factory;
	
	public Consumer(String host, String key) {
		this.host = host;
		this.key = key;
		if(StringUtils.isEmpty(key) == false) {
			queueName += "."+key;
		}		
		LOG.debug("Queue: {}", queueName);;
	}
	
	
	private void init() throws Exception {
		if(locator == null) {
			Map<String,Object> map = Maps.newHashMap();
			map.put("host", "165.243.31.56");
			map.put("port", 9090);
			Map<String,Object> map2 = Maps.newHashMap();
			map2.put("host", "165.243.31.58");
			map2.put("port", 9090);
			
			locator = HornetQClient.createServerLocatorWithoutHA(
					new TransportConfiguration(NettyConnectorFactory.class.getName(), map),
					new TransportConfiguration(NettyConnectorFactory.class.getName(), map2)
					);
			//locator.setReconnectAttempts(3);
		}
		
		factory = locator.createSessionFactory();		
		
		session = factory.createSession();
		QueueQuery queueQuery = session.queueQuery(SimpleString.toSimpleString(queueName));
		boolean isCreate = true;
		if(queueQuery.isExists()) {
			LOG.debug("queue already exists: {}", queueName);
			isCreate = false;
			try {
				session.deleteQueue(queueName);
				isCreate = true;
				LOG.debug("delete queue: {}", queueName);
			} catch (HornetQException e) {
				LOG.warn("delete queue fail: {}", queueName, e);
			}
		}
		try {
			if(isCreate) {
				if(StringUtils.isEmpty(key)) {
					session.createQueue(address, queueName, false);
					LOG.debug("create queue: "+queueName);
				} else {
					session.createQueue(address, queueName, Producer.KEY_NAME+"='"+key+"'", false);
					LOG.debug("create queue with filter: "+queueName);
				}
			}
		} catch (HornetQException e) {
			LOG.warn("create queue fail: {}", queueName, e);
		}
		session.start();
		LOG.debug("Session started");
		ClientConsumer consumer = session.createConsumer(queueName);
		consumer.setMessageHandler(new SimpleMessageHandler());
		LOG.debug("Consumer created");
	}
	
	private void clean() throws Exception {
		if(session != null) {
			session.close();
		}
		if(factory != null) {
			factory.close();
		}
	}
	
	private boolean checkConnection() {
		return session.isClosed() == false;
	}
	
	public void run() throws Exception {
		init();
		while(true) {
			Thread.sleep(1000);
			if(checkConnection() == false) {
				LOG.warn("connection closed. try reconnect.");				
				clean();
				init();
			}
		}
	}

	public static void main( String[] args ) {
		if(args.length < 1) {
			LOG.debug("Usage: Main <host> <key>" );
			System.exit(-1);
		}
		try {
			String consumerKey = null;
			if(args.length > 1) {
				consumerKey = args[1];
			}
			new Consumer(args[0], consumerKey).run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
