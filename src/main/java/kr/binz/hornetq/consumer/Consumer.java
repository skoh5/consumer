package kr.binz.hornetq.consumer;

import java.util.Map;

import kr.binz.hornetq.common.HostInfo;
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
	private HostInfo[] hosts;
	private String address = "stomp.address.test";
	private String queueName = "stomp.queue.test";
	private ServerLocator locator;
	private ClientSession session;
	private ClientSessionFactory factory;
	private volatile boolean isRun = true;
	
	public Consumer(String[] args) {
		key = args[0];
		String[] dests = StringUtils.split(args[1], ",");
		hosts = new HostInfo[dests.length];
		int idx = 0;
		for(String dest: dests) {
			hosts[idx++] = new HostInfo(dest);
		}
	}
	
	
	private void init() throws Exception {
		LOG.info("Consumer init: {}, {}", hosts, key);
		LOG.info("QueueName: {}", this.queueName);
		
		if(locator == null) {
			TransportConfiguration[] transConfigs = new TransportConfiguration[hosts.length];
			int idx = 0;
			Map<String,Object> map = null;
			for(HostInfo host: hosts) {
				map = Maps.newHashMap();
				map.put("host", host.getHost());
				map.put("port", host.getPort());
				transConfigs[idx++] = new TransportConfiguration(NettyConnectorFactory.class.getName(), map);
			}
			locator = HornetQClient.createServerLocatorWithHA(transConfigs);
			locator.setReconnectAttempts(-1);
		}
		
		factory = locator.createSessionFactory();		
		
		
		
		//session = factory.createSession();
		//session = factory.createSession(true, true, 0);
		session = factory.createSession(false, true, true, true);
		
		if(StringUtils.equals(key, "NULL") == false) {
			queueName += queueName+"."+key;
		}
		
		QueueQuery queueQuery = session.queueQuery(SimpleString.toSimpleString(queueName));
		boolean isCreate = true;
		if(queueQuery.isExists()) {
			LOG.debug("queue already exists: {}", queueName);
			isCreate = false;
			/*
			try {
				session.deleteQueue(queueName);
				isCreate = true;
				LOG.debug("delete queue: {}", queueName);
			} catch (HornetQException e) {
				LOG.warn("delete queue fail: {}", queueName, e);
			}
			*/
		}
		try {
			if(isCreate) {
				if(StringUtils.equals(key, "NULL")) {
					session.createQueue(address, queueName, true);					
					LOG.debug("create queue: {}", queueName);
				} else {
					session.createQueue(address, queueName, Producer.KEY_NAME+"='"+key+"'", true);
					LOG.debug("create queue with filter: {}, {}", queueName, key);
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
		while(isRun) {
			Thread.sleep(5000);
			if(checkConnection() == false) {
				LOG.warn("connection closed.");				
			}
		}
		clean();
	}

	public static void main( String[] args ) {
		if(args.length < 1) {
			LOG.debug("Usage: Main <key|'NULL'> <host:port,host:port...>" );
			System.exit(-1);
		}
		try {
			new Consumer(args).run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
