package com.cProc.CDR.Interface;

import java.io.InputStream;

/** 
 * @author yangzhenyu
 * @date 2012 06 01 10:32:33
 * @version v1.0 

 * @TODO CloudProcessFileSystem 业务交互api
 */

public interface CloudProcessFileSystem{
	
	/* 
	 * 入库接口
	 * param：String本地文件路径、hdfs路径、hdfs文件存放路径、hdfs通知目录路径
	 * return：boolean
	 */
	public boolean dataFileInput(String filename,String hdfsPath,String dataPath,String notifyPath);
	
	/*
	 * 入库接口
	 * 输入流、hdfs路径、本地文件路径、hdfs文件存放路径、hdfs通知目录路径
	 */
	public boolean dataFileInput(InputStream inputStream,String hdfsPath,String localNewFile,String dataPath,String notifyPath);
	
}
