package com.cProc.RPC.util;

import java.net.MalformedURLException;
import java.util.ArrayList;

import com.caucho.hessian.client.HessianProxyFactory;

public class TranTest {

	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println(System.currentTimeMillis());
			}

		});
	}

	/**
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String[] args) throws MalformedURLException {
		HessianProxyFactory factory = new HessianProxyFactory();
		factory.setConnectTimeout(5000);
		final ITran hello = (ITran) factory.create(ITran.class,
				"http://127.0.0.1:8080/tran");
		final ArrayList list = new ArrayList();
		for (int i = 1; i <= 5000; i++) {
			list.add("http://127.0.0.1:8080/tran");
		}
		System.out.println(System.currentTimeMillis());
		hello.sendSize(3823782, "http://127.0.0.1:8080/tran");
		

	}

}