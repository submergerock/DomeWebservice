package com.cProc.CDR.util;

import java.io.IOException;
import java.util.UUID;

public class MultipleThread {
	private static String url = "";
	public static void main(String[] args) throws IOException {
//		while(true){
			if(args!=null){
				url = "http://192.168.1.12:8080/JobServlet";
			}
			String sessionId = UUID.randomUUID().toString();
	//		String condition ="{\"command\":\"GET\", \"session\":\""+sessionId+"\", \"condition\":{\"protocol\":\"UDM\",\"MSISDN\":[8618663625533],\"time_range_list\":[{\"start_time\":1339295237,\"end_time\":1339295237}]}}";
	//		String buffer ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId+"\", \"condition\":{\"protocol\":\"UDM\",\"MSISDN\":[8618663625533],\"time_range_list\":[{\"start_time\":1339295237,\"end_time\":1339295237}]}}";
			String condition ="{\"command\":\"GET\", \"session\":\""+sessionId+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1331603329,\"end_time_s\":1331604736}],\"called_number\":\"10086\"}}";
			String buffer ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1331603329,\"end_time_s\":1331604736}],\"called_number\":\"10086\"}}";
			System.out.println("1 min : " + condition);
	//		buffer = "";
	//		condition = "" ;
			HttpClientThread thread = new HttpClientThread(url, condition,sessionId,buffer);
			thread.start();
//			try {
//				Thread.sleep(60*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//			//30min
//			String sessionId1 = UUID.randomUUID().toString();
//			String condition1 ="{\"command\":\"GET\", \"session\":\""+sessionId1+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1342977867}],\"called_number\":\"10086\"}}";
//			String buffer1 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId1+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1342977867}],\"called_number\":\"10086\"}}";
//			System.out.println("30 min : " + condition);
//			HttpClientThread thread1 = new HttpClientThread(url, condition1,sessionId1,buffer1);
//			thread1.start();
//			try {
//				Thread.sleep(120*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			//1h
//			String sessionId2 = UUID.randomUUID().toString();
//			String condition2 ="{\"command\":\"GET\", \"session\":\""+sessionId2+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1342979667}],\"called_number\":\"10086\"}}";
//			String buffer2 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId2+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1342979667}],\"called_number\":\"10086\"}}";
//			System.out.println("1 h : " + condition);
//			HttpClientThread thread2 = new HttpClientThread(url, condition2,sessionId2,buffer2);
//			thread2.start();
//			try {
//				Thread.sleep(120*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			//12h
//			String sessionId3 = UUID.randomUUID().toString();
//			String condition3 ="{\"command\":\"GET\", \"session\":\""+sessionId3+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1343019267}],\"called_number\":\"10086\"}}";
//			String buffer3 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId3+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1343019267}],\"called_number\":\"10086\"}}";
//			System.out.println("12 h : " + condition);
//			HttpClientThread thread3 = new HttpClientThread(url, condition3,sessionId3,buffer3);
//			thread3.start();
//			try {
//				Thread.sleep(360*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			//24h
//			String sessionId4 = UUID.randomUUID().toString();
//			String condition4 ="{\"command\":\"GET\", \"session\":\""+sessionId3+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1343062467}],\"called_number\":\"10086\"}}";
//			String buffer4 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId3+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1342976067,\"end_time_s\":1343062467}],\"called_number\":\"10086\"}}";
//			System.out.println("24 h : " + condition);
//			HttpClientThread thread4 = new HttpClientThread(url, condition4,sessionId4,buffer4);
//			thread4.start();
//			try {
//				Thread.sleep(480*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		String sessionId1 = UUID.randomUUID().toString();
//		String condition1 ="{\"command\":\"GET\", \"session\":\""+sessionId1+"\", \"condition\":{\"protocol\":\"BICC\",\"pair_elem_list\":[],\"time_range_list\":[{\"start_time_s\":1167926400,\"end_time_s\":1325779200}],\"calling_number\":\"10086\"}}";
//		String buffer1 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId1+"\", \"condition\":{\"protocol\":\"BICC\",\"pair_elem_list\":[],\"time_range_list\":[{\"start_time_s\":1167926400,\"end_time_s\":1325779200}],\"calling_number\":\"10086\"}}";
//		System.out.println(condition1);
//		HttpClientThread thread1 = new HttpClientThread(url, condition1,sessionId1,buffer1);
//		thread1.start();
//		String sessionId2 = UUID.randomUUID().toString();
//		String condition2 ="{\"command\":\"GET\", \"session\":\""+sessionId2+"\", \"condition\":{\"protocol\":\"IUCS\",\"time_range_list\":[{\"start_time_s\":1167926400,\"end_time_s\":1325779200}],\"calling_number\":\"10086\",\"cdr_type\":[1,2,3,4,5,6,7,8,12,13,14,18,19]}}";
//		String buffer2 ="{\"command\":\"CONTINUE\", \"session\":\""+sessionId2+"\", \"condition\":{\"protocol\":\"IUCS\",\"time_range_list\":[{\"start_time_s\":1167926400,\"end_time_s\":1325779200}],\"calling_number\":\"10086\",\"cdr_type\":[1,2,3,4,5,6,7,8,12,13,14,18,19]}}";
//		System.out.println(condition2);
//		HttpClientThread thread2 = new HttpClientThread(url, condition2,sessionId2,buffer2);
//		thread2.start();
	}

}
