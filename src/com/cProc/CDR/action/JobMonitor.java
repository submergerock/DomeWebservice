package com.cProc.CDR.action;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntity;


import com.cProc.CDR.bean.ServletBean;
import com.cstor.service.CleanService;

public class JobMonitor {

	public static final Log LOG = LogFactory.getLog(JobMonitor .class.getName());
	public static Properties properties;
	public static String initurl;
	public static String tempResultDir;
	
	static{
		try {
			properties = new Properties();
			properties.load(JobMonitor.class.getClassLoader().getResourceAsStream("FS.properties"));
			initurl = properties.getProperty("initurl");
			properties.load(new FileReader(initurl));
			tempResultDir = properties.getProperty("tempResultDir");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean getJobStatus(String sessionId,String namenode,ServletContext context,Configuration conf) {
		String jobId = (String) context.getAttribute(sessionId);
//		LOG.info("@yzy jobId is : "+jobId);
		if(jobId == null || jobId.equals("")){
			return false;
		}
//		namenode = "172.3.2.103:8888";
		CProcFrameworkProtocol cProcFrameworkNode = null;
		InetSocketAddress cProcFrameworkNodeAddr = NetUtils.createSocketAddr(namenode);
		try {
			cProcFrameworkNode = (CProcFrameworkProtocol) RPC.getProxy(CProcFrameworkProtocol.class,CProcFrameworkProtocol.versionID, cProcFrameworkNodeAddr,conf, NetUtils.getSocketFactory(conf,CProcFrameworkProtocol.class));
			String type = cProcFrameworkNode.getJobEntityType(jobId);
//			LOG.info("@yzy jobId is : "+jobId+" , jobType is : ====================" + type );
//			RPC.stopProxy(cProcFrameworkNode);
			if (type .equals("SUCCESS")) {
				return true;
			}
			JobEntity jobEntity = cProcFrameworkNode.getJobEntity(jobId);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return true;
		}
//		finally{
//			RPC.stopProxy(cProcFrameworkNode);
//		}
		
		return false;
	}

	public ServletBean MonitorLocalDir(String sessionId,ServletBean bean,String namenode,ServletContext context,Configuration conf,long timeout) throws IOException{
	    bean.setFileName(bean.getFileName()+1);
	    String path = tempResultDir + ""+sessionId+"/"+bean.getFileName();
	    File dir = new File(tempResultDir+""+sessionId+"/");
	    File file = new File(path);
//	    LOG.info("bean.getFileName is : "+bean.getFileName());
//	    LOG.info("bean.getLocalFileName is : "+bean.getLocalFileName());
//	    LOG.info(path);
//	    LOG.info(tempResultDir);
	    long aLong = new Date().getTime();
	    //这里吗怎么会有cstor的jar包
		while (!file.exists()) {
			boolean flagCon = false;
			String[] fileNames = dir.list();
//			LOG.info("dir.list is : " + dir.list());
			if(fileNames !=null && fileNames.length!=0){
				for (int i = 0; i < fileNames.length; i++) {
//					LOG.info("System.currentTimeMillis() fileNames[i] =================================== "+System.currentTimeMillis());
					if (fileNames[i].contains(".")) {
						flagCon = true;
						break;
					}
				}
				if(flagCon){
					file = new File(path);
					continue;
				}else{
					boolean jobStatus = getJobStatus(sessionId, namenode, context,conf);
					if(jobStatus){
						LOG.info("Job is successs!!!   return null");
						CleanService cs = new CleanService(9999, sessionId);
						cs.clean();
						return null;
					}
				}
			}else{
				dir = new File(tempResultDir+""+sessionId+"/");
//				LOG.info(tempResultDir+""+sessionId+"/ ______________________"+dir.exists());
				if(!dir.exists()){
					boolean jobStatus = getJobStatus(sessionId, namenode, context,conf);					
					if(jobStatus){
						dir = new File(tempResultDir+""+sessionId+"/");
						if(dir.exists()){
							continue;
						}
						LOG.info("System.currentTimeMillis() Job =================================== "+System.currentTimeMillis());
						LOG.info("Job is successs!!!   return null");
						//cstor 的jar，什么意思呢
						CleanService cs = new CleanService(9999, sessionId);
						cs.clean();
						return null;
					}else{
						continue;
					}
				}else{
					continue;
				}
			}
			long bLong = new Date().getTime();
			if(bLong-aLong>=timeout){
				return null;
			}
			if(!file.exists()&&dir.exists()){
//				LOG.info("~~~~~~~~~~~~~~~~~~~yzy !file.exists~~~~~~~~~~~~~~~~~~~~~~~``");
			    if(getJobStatus(sessionId, namenode, context,conf)){
			    	LOG.info("!!!!!!!!!!!!!!!!!!yzy job zhuang tai geng xin wen ti !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			    	file = new File(path);
			    	if(!file.exists()){
//			    		LOG.info("end--------------------------"+sessionId+" ,path is : "+path);
			    		fileNames = dir.list();
						for (int i = 0; i < fileNames.length; i++) {
							if (fileNames[i].contains(".")) {
								flagCon = true;
								break;
							}
						}
						if(flagCon){
							continue;
						}
						while(true)
						try{
							CleanService cs = new CleanService(9999, sessionId);
							cs.clean();
//						    Socket socket = new Socket("localhost", 9999);
//						    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
//						    ArrayList<String> parmaStrings = new ArrayList<String>();
//							parmaStrings.add(sessionId);
//							parmaStrings.add(Protocol.SENDALLOVER+"");
//							parmaStrings.add(Protocol.CACHE+"");
//							writer.write(Protocol.createCommand(parmaStrings));
//						    writer.flush();
//						    writer.close();
						    return null;
						}catch (Exception e) {
							continue;
						}
			    	}else{
			    		continue;
			    	}
			    }
			}
			file = new File(path);
		}
	    bean.setLocalFileName(path);
	    return bean;

	}
}
