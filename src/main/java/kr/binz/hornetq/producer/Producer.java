package kr.binz.hornetq.producer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
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
public class Producer {
	
	static final Logger LOG = LoggerFactory.getLogger(Producer.class);
	
	public static final String KEY_NAME = "KEY";
	
	private String[] key;
	private String host;
	private String address = "stomp.address.test";
	private ServerLocator locator;
	private ClientSession session;
	private ClientSessionFactory factory;
	private ClientProducer producer;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public Producer(String host, String[] key) {
		this.host = host;
		this.key = key;
	}

	public class SendRunner implements Runnable {

		public void run() {
			ClientMessage message = null;
			String msg = null;
			int idx = 0;
			while(true) {
				if(checkConnection()) {
					msg = "Hello: "+ sdf.format(new Date());
					message = session.createMessage(false);
					//message.getBodyBuffer().writeString(msg);
					message.getBodyBuffer().writeBytes(msg.getBytes());
					message.putStringProperty("content-length", String.valueOf(msg.getBytes().length));
					//message.putStringProperty(KEY_NAME, key[idx%key.length]);
					try {
						producer.send(message);
						LOG.debug("send msg: "+ msg +" => "+key[idx%key.length]);
					} catch (Exception e) {
						LOG.error("send fail", e);
					}
					idx++;
				}
				try {
					Thread.sleep(5000);
				} catch (Exception e) {
					
				}
			}
		}
	}
	
	private void init() throws Exception {
		if(locator == null) {
			Map<String,Object> map = Maps.newHashMap();
			map.put("host", "183.100.209.69");
//			map.put("host", "localhost");
			map.put("port", 5445);
			/*
			Map<String,Object> map2 = Maps.newHashMap();
			map2.put("host", "165.243.31.58");
			map2.put("port", 9090);
			*/
			locator = HornetQClient.createServerLocatorWithoutHA(
					new TransportConfiguration(NettyConnectorFactory.class.getName(), map)
					//,new TransportConfiguration(NettyConnectorFactory.class.getName(), map2)
					);
			//locator.setReconnectAttempts(3);
		}
		
		factory = locator.createSessionFactory();		
		
		session = factory.createSession();
		session.start();
		LOG.debug("Session started");
		producer = session.createProducer(address);
		LOG.debug("Producer created: %s", address);
		
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
		new Thread(new SendRunner()).start();
		while(true) {
			Thread.sleep(1000);
			if(checkConnection() == false) {
				LOG.warn("connection closed. try reconnect.");
				clean();
				init();
			}
		}
	}
	
    public static void main( String[] args )  {
    	if(args.length < 2) {
			LOG.debug("Usage: Producer <host> <key>" );
			System.exit(-1);
		}
		try {
			String[] keys = new String[args.length-1];
			keys = Arrays.copyOfRange(args, 1, args.length);
			new Producer(args[0], keys).run();			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
