package com.cProc.CDR.util;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.cProc.CDR.action.JobServlet;

/** 
 * @author 杨震宇 
 * @date 2012 7 2 10:08:43
 * @version v1.0 
 * @TODO 数据缓存线程，作用：1、删除文件采用多线程模式；2、根据自定义配置数据文件存放时间
 */
public class DataCacheThread implements Runnable {

	private String tempNotifyDir;
	private String tempResultDir;
	private int CacheTime = 20000;//三十分钟1800000
	private Boolean isOver = false;
	
	/**
	 * 缓存数据容器
	 */
	public static Map<String,String> DATA_CACHE = new HashMap<String, String>();
	
	public DataCacheThread(String tempNotifyDir,String tempResultDir,int CacheTime){
		this.tempNotifyDir = tempNotifyDir;
		this.tempResultDir = tempResultDir;
		if(CacheTime ==0){
			this.CacheTime = CacheTime;
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(!isOver){
			Map<String,String> dataCache = new HashMap<String, String>();
			synchronized (DATA_CACHE) {
				dataCache = DATA_CACHE;
			}
			for(Map.Entry<String, String> entry:dataCache.entrySet()){
				System.out.println(entry.getKey()+"--->"+entry.getValue());    
				String str[] = entry.getValue().split(",");
				Date date = new Date();
				long nowDate = date.getTime() - Long.parseLong(str[0]);
				if(nowDate > CacheTime){
					System.out.println(entry.getKey() + " is deleteDir !");
//					DeleteDir(str[1]);
//					DATA_CACHE.remove(entry.getKey());
				}
			}
			try {
				Thread.sleep(10*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public boolean DeleteDir(String sessionID){
		boolean f = false;
		try{
	        File file = new File(tempNotifyDir+"/"+sessionID);
	        DeleteDir(file);
	        file = new File(tempResultDir+"/"+sessionID);
	        f = DeleteDir(file);
		}catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}
	
	public boolean DeleteDir(File file){
		boolean f = false;
		if(file.exists()){//判断文件是否存在
        	if(file.isFile()){//判断是否是文件
        		file.delete();//delete()方法 你应该知道 是删除的意思;
            }else if(file.isDirectory()){//否则如果它是一个目录
            	File files[] = file.listFiles();//声明目录下所有的文件 files[];
            	for(int i=0;i<files.length;i++){//遍历目录下所有的文件
            		files[i].delete();//把每个文件 用这个方法进行迭代
            	} 
            } 
        	file.delete(); 
        	f = true;
        }
		return f;
	}
	
	public static void main(String[] args) {
		DataCacheThread dt = new DataCacheThread("D:/123/321","D:/123/123",1800000);
		DATA_CACHE.put("select * from abc ",System.currentTimeMillis() + ",a");
		DATA_CACHE.put("select * from abc where 1=1",System.currentTimeMillis() + ",a");
		DATA_CACHE.put("select * from abc where 1=1 and 2=2",System.currentTimeMillis() + ",a");
		dt.run();
	}
	
}



