package kr.binz.hornetq.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

public class HostInfo {
	private String host;
	private int port;
	
	public HostInfo(String dest) {
		String[] cols = StringUtils.split(dest, ":");
		this.host = cols[0];
		this.port = NumberUtils.toInt(cols[1], 5445);
	}
	
	public HostInfo(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public String getHost() {
		return this.host;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(host).append(":").append(port);
		return strBuf.toString();
	}
}
