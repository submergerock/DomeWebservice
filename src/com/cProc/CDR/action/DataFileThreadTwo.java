package com.cProc.CDR.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cProc.CDR.bean.BlockQueueBean;


/** 
 * @author yangzhenyu
 * @date 2012 03 29 14:27:02
 * @version v2.1 

 * @TODO 预读文件线程（有数量限制）
 */

public class DataFileThreadTwo implements Runnable {
	public static final Log LOG = LogFactory.getLog(DataFileThreadTwo .class.getName());
	private static String initurl = "";
	private static String tempResultDir = "";
	private static String tempNotifyDir = "";
	private JobMonitor jobMonitor;
	private BlockingQueue<BlockQueueBean> bq = null;
	private Boolean isOver = false;
	private static long timeout = 60*1000;
	private int read_file_num = 10;
	
	static{
		try {
			Properties properties = new Properties();
			properties.load(DataFileThreadTwo.class.getClassLoader().getResourceAsStream("FS.properties"));
			initurl = properties.getProperty("initurl");
			timeout = Long.parseLong(properties.getProperty("timeout"));
			properties.load(new FileReader(initurl));
			tempResultDir = properties.getProperty("tempResultDir");
			tempNotifyDir = properties.getProperty("tempNotifyDir");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public DataFileThreadTwo(BlockingQueue<BlockQueueBean> bq,int read_file_num){
		this.bq = bq;
		this.jobMonitor = new JobMonitor();
		this.read_file_num = read_file_num;
	}
	
	@Override
	public void run() {
		while(!isOver){
			try {
				BlockQueueBean bqo = bq.take();
				//判断文件读取个数是否符合初始化设置数，不符合则不读取文件
				synchronized(JobServlet.READ_FILE_MAP){
					if(JobServlet.READ_FILE_MAP.containsKey(bqo.getSessionID())){
						read_file_num = JobServlet.READ_FILE_MAP.get(bqo.getSessionID());
					}else{
						break;
					}
				}
				if(bqo.getNum()<=read_file_num){
					if(!ReadFile(bqo)){
						LOG.info("sessionID : "+ bqo.getSessionID() +" , bqo.getNum() : "+bqo.getNum()+" read over !");
						bqo.setNum(bqo.getNum()+1);
						bq.add(bqo);
					}else{
						bqo = null;
					}
				}else{
					long read_file_time = 0;
					//判断文件预读时间是否超时，超时则不读取
					synchronized(JobServlet.READ_FILE_TIME){
						read_file_time = JobServlet.READ_FILE_TIME.get(bqo.getSessionID());
					}
					Date date = new Date();
					long nowDate = date.getTime() - read_file_time;
					if(nowDate< timeout){
						bq.add(bqo);
					}else{
						LOG.info("@yzy =====================================================time out ! read file time out !" + bqo.getSessionID());
//						String sessionID = bqo.getSessionID();
//						for(int k = 1 ;k <= bqo.getNum();k++){
//							JobServlet.datafile.remove(sessionID+""+k);
//						}
//						JobServlet.requestPool.finishRequest(sessionID);
//						JobServlet.READ_FILE_MAP.remove(sessionID);
						bqo = null;
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void putFile(String Name,File file) throws Exception{
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(file));
		String str = null;
		String jRows = "";
		if((str = reader.readLine())!=null){
			jRows = str;
		}
		JobServlet.datafile.put(Name,jRows);
		LOG.info("put file " + Name);
		jRows = null;
		str = null;
		reader.close();
	}
	
	public boolean ReadFile(BlockQueueBean bqo){
		String sessionID = bqo.getSessionID();
		int num = bqo.getNum();
		ServletContext context = bqo.getContext();
		Configuration conf = bqo.getConf();
		File file = null;
		File dir = null;
		Date date = new Date();
		Long starDate = date.getTime();
		String path = tempResultDir+""+sessionID+"/"+num;
		file = new File(path);
		LOG.info("bn.getFileName() : " + num);
		while (!file.exists()) {//文件不存在
			dir = new File(tempResultDir+""+sessionID+"/");
			boolean flagCon = false;
			//获取文件目录信息
			String[] fileNames = dir.list();
//			LOG.info("yzy1@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			if(fileNames !=null && fileNames.length!=0){
//				LOG.info("yzy2@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
				for (int i = 0; i < fileNames.length; i++) {
					if (fileNames[i].contains(".")) {
						flagCon = true;
						break;
					}
				}
				if(flagCon){
//					LOG.info("yzy3@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					file = new File(path);
					continue;
				}else{
					//1、目录下肯定有文件；2、没有.tmp文件;
					boolean jobStatus = jobMonitor.getJobStatus(sessionID, conf.get("hadoop.usrjar.ip"), context, conf);
//					LOG.info("yzy4@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					if(jobStatus){
						LOG.info("yzy5@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
//						try {
//							LOG.info("sleep");
//							Thread.currentThread().sleep(5000);
//						} catch (InterruptedException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						}
						File ifile = new File(path);
//						boolean ffffa = false;
						if(ifile.exists()){
////							int affff = num ;
//							File iifile = new File(tempNotifyDir+""+sessionID+"/");
//							if(iifile.exists()){
//								for(int k=0;k<iifile.listFiles().length;k++)
//								{
////									System.out.println("======="+iifile.listFiles()[k].getName().split("_")[0]);
//									if(iifile.listFiles()[k].getName().split("_")[0].equals(""+num)){
//										ffffa = true;
//									}
//								}
//							}
							try {
								putFile(sessionID+""+num, ifile);
								return false;
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}else{
							return true;
						}
//						if(!ffffa){
//							return true;
//						}
					}
				}
			}else{
				boolean jobStatus = jobMonitor.getJobStatus(sessionID, conf.get("hadoop.usrjar.ip"), context, conf);
//				LOG.info("yzy6@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
				if(jobStatus){
//					LOG.info("yzy7@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
					starDate = null;
					return true;
				}
			}
			Date date1 = new Date();
			Long nowDate = date1.getTime();
			Long aLong = nowDate - starDate ;
			if(aLong >=timeout){
				LOG.info("@yzy =====================================================time out ! Thread stop ! " + sessionID);
				for(int k = 1 ;k <= num;k++){
					JobServlet.datafile.remove(sessionID+""+k);
				}
//				JobServlet.requestPool.finishRequest(sessionID);
				JobServlet.READ_FILE_MAP.remove(sessionID);
				JobServlet.READ_FILE_TIME.remove(sessionID);
//				LOG.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+JobServlet.datafile.size());
				nowDate = null;
				aLong = null;
				starDate = null;
				return true;
			}
//			LOG.info("yzy8@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			file = new File(path);
		}
		LOG.info("file.getFileName() : " + file.getName());
		try {
			putFile(sessionID+""+num, file);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("===============================================================put " + sessionID+""+num);
		LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!getFileName : " + num + " , sessionID : " + sessionID);
		return false;
	}
}
