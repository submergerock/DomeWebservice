package com.cProc.CDR.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;

import com.cProc.CDR.bean.ServletBean;


/** 
 * @author yangzhenyu
 * @date 2012 03 29 14:27:02
 * @version v2.1 

 * @TODO 预读文件线程（无数量限制，有多少读多少）
 */

public class DataFileThread extends Thread {
	private String sessionID ;
	private ServletBean bean;
	private String namenode;
	private ServletContext context;
	private static String initurl = "";
	private static String tempResultDir = "";
	private JobMonitor jobMonitor;
	private static long timeout = 0;
	private Configuration conf;
	
	static{
		try {
			Properties properties = new Properties();		
			properties.load(DataFileThread.class.getClassLoader().getResourceAsStream("FS.properties"));
			initurl = properties.getProperty("initurl");
			properties.load(new FileReader(initurl));
			tempResultDir = properties.getProperty("tempResultDir");
			timeout = Long.parseLong(properties.getProperty("timeout"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public DataFileThread(String sessionID,ServletBean bean,String namenode,ServletContext context,Configuration conf) throws IOException {
		// TODO Auto-generated constructor stub
		this.sessionID = sessionID;
		this.bean = bean;
		this.namenode = namenode;
		this.context = context;
		this.conf =conf;
		jobMonitor = new JobMonitor();
	}
	
	@Override
	public void run() {
		try {
			boolean fdataFile = false ;
			ServletBean bn = bean;
			int fNum = bn.getFileName();
			File file = null;
			File dir = null;
			Date date = new Date();
			Long starDate = date.getTime();
			while(!fdataFile){
				String path = tempResultDir+""+sessionID+"/"+fNum;
				file = new File(path);
//				System.out.println("bn.getFileName() : " + fNum);
				while (!file.exists()) {
					dir = new File(tempResultDir+""+sessionID+"/");
					boolean flagCon = false;
					String[] fileNames = dir.list();
					if(fileNames !=null && fileNames.length!=0){
						for (int i = 0; i < fileNames.length; i++) {
							if (fileNames[i].contains(".")) {
								flagCon = true;
								try {
									Thread.sleep(100);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								break;
							}
						}
						if(flagCon){
							file = new File(path);
							continue;
						}else{
							boolean jobStatus = jobMonitor.getJobStatus(sessionID, namenode, context, conf);
							if(jobStatus){
								fdataFile = true;
								break;
							}
						}
					}else{
						boolean jobStatus = jobMonitor.getJobStatus(sessionID, namenode, context, conf);
						if(jobStatus){
							fdataFile = true;
							break;
						}else{
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							continue;
						}
					}
					Date date1 = new Date();
					Long nowDate = date1.getTime();
					Long aLong = nowDate - starDate ;
//					System.out.println("sessionID : "+sessionID+" , nowDate - starDate : " + aLong );
					if(aLong >=timeout){
						System.out.println("@yzy =====================================================time out ! Thread stop ! " + sessionID);
						for(int k = 1 ;k <= fNum;k++){
							JobServlet.datafile.remove(sessionID+k);
						}
						JobServlet js = new JobServlet();
						if(js.DeleteDir(sessionID)){
							System.out.println("delete Dir success ! ");
	                    }else{
	                    	System.out.println("delete Dir error ! ");
	                    }
//						Thread.currentThread().stop();
						System.out.println();
						fdataFile = true;
						break;
					}
					if(fdataFile){
						break;
					}
					file = new File(path);
				}
				if(fdataFile){
					System.out.println("DataFileThread over !");
					break;
				}
//				System.out.println("file.getFileName() : " + file.getName());
				BufferedReader reader;
				reader = new BufferedReader(new FileReader(file));
				String str = null;
//				net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
//				List<List<String>> jRows = new ArrayList<List<String>>();
				String jRows = "";
//				int i = 0;
				if((str = reader.readLine())!=null){
					if(str.equals("")){
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}else{
						jRows = str;
					}
//					net.sf.json.JSONArray jrows = new net.sf.json.JSONArray();
//					List<String> jrows = new ArrayList<String>();
//					str = str.substring(1,str.length()-1);
//					String[] jStr = str.trim().split(",");
//					if(i==0)
//					System.out.println("sessionID is : " + sessionID + ", rows.length is : " + jStr.length);
//					for(int k = 0;k<jStr.length;k++){
//						jrows.add(jStr[k]);
//				    }
//					jRows.add(jrows);
//					i++;
				}
				JobServlet.datafile.put(sessionID+""+fNum,jRows);
//				if(fNum%3==0){
//					synchronized(Thread.currentThread()){
//						try {
//							Thread.currentThread().wait();
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				}
				fNum ++ ;
			}
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Thread over !!");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
