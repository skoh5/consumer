package kr.binz.hornetq.producer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import kr.binz.hornetq.common.HostInfo;

import org.apache.commons.lang3.StringUtils;
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
	
	private String[] keys;
	private HostInfo[] hosts;
	private String address = "stomp.address.test";
	private ServerLocator locator;
	private ClientSession session;
	private ClientSessionFactory factory;
	private ClientProducer producer;
	private volatile boolean isRun = true;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public Producer(String[] args) {
		keys = StringUtils.split(args[0], ",");
		String[] dests = StringUtils.split(args[1], ",");
		hosts = new HostInfo[dests.length];
		int idx = 0;
		for(String dest: dests) {
			hosts[idx++] = new HostInfo(dest);
		}
	}

	public class SendRunner implements Runnable {

		public void run() {
			ClientMessage message = null;
			String msg = null;
			int idx = 0;
			while(true) {
				if(checkConnection()) {
					msg = "["+idx+"]Hello: "+ sdf.format(new Date());
					message = session.createMessage(false);
					message.getBodyBuffer().writeString(msg);
					//message.getBodyBuffer().writeBytes(msg.getBytes());
					//message.putStringProperty("content-length", String.valueOf(msg.getBytes().length));
					message.putStringProperty(KEY_NAME, keys[idx%keys.length]);
					try {
						producer.send(message);
						LOG.debug("send msg: "+ msg +" => "+keys[idx%keys.length]);
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
		LOG.info("Producer init: {}, {}", hosts, keys);
		LOG.info("Address: {}", this.address);
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
		
		session = factory.createSession();
		session.start();
		LOG.debug("Session started");
		producer = session.createProducer(address);
		LOG.debug("Producer created: {}", address);
		
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
		while(isRun) {
			Thread.sleep(5000);
			if(checkConnection() == false) {
				LOG.warn("connection closed.");
			}
		}
		clean();
	}
	
    public static void main( String[] args )  {
    	if(args.length < 2) {
			LOG.debug("Usage: Producer <key,key,key...> <host:port,host:port,host:port...>" );
			System.exit(-1);
		}
		try {
			new Producer(args).run();			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
