package com.cProc.CDR.Data;

/** 
 * @author yangzhenyu
 * @date 2011 11 28 13:45:02
 * @version v1.0 

 * @TODO jetty start servlet
 */


import org.mortbay.jetty.Server;
import org.mortbay.xml.XmlConfiguration;

public class CDRDataServlet {
    //wzt 这个时程序的入口吗
	/**
	 * @param args
	 */
	public static final void main(String[] args) {
		try {
//			String confUrl = args[0];
//			Thread thread = new Thread(new DataService(confUrl));
//			thread.start();
			Server server = new Server();
			XmlConfiguration configuration = new XmlConfiguration(CDRDataServlet.class.getClassLoader().getResourceAsStream("JettyConfig.xml"));
			configuration.configure(server);
			server.stop();
			server.clearAttributes();
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}