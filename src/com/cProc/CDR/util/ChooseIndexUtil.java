package com.cProc.CDR.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ChooseIndexUtil {
	/*
	 *智能选取索引 
	 */
	public static final Log LOG = LogFactory.getLog(ChooseIndexUtil.class.getName());
	/**
	 * FS.properties配置文件对象
	 */
	private static final Properties properties ;
	
	/**
	 * FS_hdfsIP信息配置
	 */
	private static String FS_hdfsIP ="hdfs://192.168.1.12:8000";
	
	/**
	 * tableInfo信息配置
	 */
	private static String TableInfo ="/smp/cdr/tableInfo/";
	
	static{
		properties = new Properties();
		try {
			properties.load(ChooseIndexUtil.class.getClassLoader().getResourceAsStream("FS.properties"));
			//FS_hdfsIP
	        FS_hdfsIP = properties.getProperty("FS");
	        LOG.info(FS_hdfsIP);
			//FS_hdfsIP
	        TableInfo = properties.getProperty("TableInfo");
	        LOG.info(TableInfo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 根据条件选取索引
	 */
	public String getIndexLoader(String condition,String tableName){
		String conditions[] = condition.split(",");
		//根据tableName选取索引文件
		File file = new File("./tableInfo/"+tableName+"/indexfile/");
		HashMap<String,Integer> tableMap = new HashMap<String,Integer>();
		int zhongjian = 0;
		String indexName = "";
		if(file.exists()){
			File files[] = file.listFiles();//声明目录下所有的文件 files[];
        	for(int i=0;i<files.length;i++){//遍历目录下所有的文件
        		System.out.println(files[i].getPath());
        		if(tableName.equals("abiscdr")){//没有见到abis的表 ？？？
        			tableName = "abis";
        		}
        		//怎样去匹配
        		int score = matchIndex(files[i].getPath(),tableName,conditions);
        		System.out.println("score " + score);
        		if(zhongjian<score){
        			zhongjian = score;
        			tableMap.put(files[i].getName(), zhongjian);
        			indexName = files[i].getName();
        		}
        		System.out.println("indexName " + indexName);
        	}
        	return indexName;
		}else{
			return null;
		}
	}
	
	/**
	 * 读取文件，匹配数据
	 */
	
	private int matchIndex(String path,String tableName,String[] conditions){
		try {
			/*
			tableName:bssap
			indexName:callingIndex
			other:calling_number:LONG
			time:start_time_s:INT
			other:cdr_type:BYTE
			other:imsi:LONG
			*/
			/*
			  tableName:bssap
              indexName:drillIndex
              time:end_time_s:INT
              other:cdr_type:BYTE
              other:cdr_result:BYTE
              other:imsi:LONG
              other:city_code:SHORT
              other:bsc_spc:INT
              other:msc_spc:INT
			 */
			int score = 10;
			int sourceFilter = 0;
			File indexFile = new File(path+"/index.frm");
			if(indexFile.exists()){
				BufferedReader reader;
				reader = new BufferedReader(new FileReader(indexFile));
				String str = "";
				//第一次读取时判断文件表名是否符合要求
				if((str = reader.readLine())!=null){
					if(!str.split(":")[1].equalsIgnoreCase(tableName)){
						LOG.info("matchIndex tableName error !");
						return 0;
					}
				}
				//第二次读取时判断索引文件名是否符合要求
				if((str = reader.readLine())!=null){
					if(!str.split(":")[1].equalsIgnoreCase(indexFile.getParentFile().getName())){
						LOG.info("matchIndex indexName error !");
						return 0;
					}
				}
				int state = 10;
				while((str = reader.readLine())!=null){
					if(!str.trim().equals("")){
						for(int i =0;i<conditions.length;i++){
							if(conditions[i].equalsIgnoreCase(str.split(":")[1].trim())){
								score +=10 + state;
								break;
							}else{
								//判断是否过滤源文件
								sourceFilter = 1;							
							}
						}
					}
					state -- ;
				}
				return score - sourceFilter;
			}else{
				return 0 ;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 加载hdfs上tableInfo的表数据
	 */
	public void getTableInfo(){
		Configuration conf = new Configuration();
		conf.set("fs.default.name", FS_hdfsIP);
		Path hdfspath = new Path(TableInfo);
		LOG.info("begin gettableInfo");
		try{
			FileSystem fs = FileSystem.get(conf);
			LOG.info("what happen ");
			if(!fs.exists(hdfspath)){
				fs.mkdirs(hdfspath);
			}
			if(DeleteDir("./tableInfo")){
				LOG.info("tableInfo delete Dir success ! ");
        	}else{
        		LOG.info("tableInfo delete Dir error ! ");
        	}
			fs.copyToLocalFile(hdfspath, new Path("./"));
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	//本地读取还是hdfs远程读取
	/**
	 * 扫描tableInfo信息
	 */
	private List<String> getTableInfo (String tableName) throws IOException{
		List<String> tableList = new ArrayList<String>();
		File file = new File("./tableInfo/"+tableName.toLowerCase()+"/table.frm");
		if(file.exists()){
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String str = "";
			//第一读取时判断文件表名是否符合要求
			if((str = reader.readLine())!=null){
				if(!str.split(":")[1].equalsIgnoreCase(tableName)){
					System.out.println("getTableInfo tableName error !");
					return null;
				}
			}
			while((str = reader.readLine())!=null){
				if(!str.trim().equals("")){
//					System.out.println(str.split(",")[0].split(":")[1].trim());
					tableList.add(str.split(",")[0].split(":")[1].trim());
				}
			}
			return tableList;
		}else{
			return null;
		}
	}
	
	public boolean DeleteDir(String fileName){
		boolean f = false;
		try{
	        File file = new File(fileName);
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
            		DeleteDir(files[i]);//把每个文件 用这个方法进行迭代
            	} 
            } 
        	file.delete(); 
        	f = true;
        }
		return f;
	}
	
	public static void main(String[] args) {
//		FS_hdfsIP = "hdfs://192.168.1.12:8000";
//		TableInfo = "/smp/cdr/tableInfo/";
//		getTableInfo();
//		String aaa = getIndexLoader("called_number,start_time_s,imsi,imei","bssap");
	}
}
