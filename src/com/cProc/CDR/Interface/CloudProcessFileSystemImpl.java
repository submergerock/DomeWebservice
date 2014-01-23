package com.cProc.CDR.Interface;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/** 
 * @author yangzhenyu
 * @date 2012 06 01 10:32:33
 * @version v1.0 

 * @TODO CloudProcessFileSystemImpl 业务交互api
 */

public class CloudProcessFileSystemImpl implements CloudProcessFileSystem{
	
	public static final Log LOG = LogFactory.getLog(CloudProcessFileSystemImpl.class.getName());
	
	public boolean dataFileInput(String filename,String hdfsPath,String dataPath,String notifyPath) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);
			conf.set("dfs.replication", "2");
			Path hdfspath = new Path(dataPath);
			FileSystem fs = FileSystem.get(conf);
			if(!fs.exists(hdfspath)){
				fs.mkdirs(hdfspath);
			}
			hdfspath = new Path(notifyPath);
			if(!fs.exists(hdfspath)){
				fs.mkdirs(hdfspath);
			}
			String path = dataPath+UUID.randomUUID().toString()+".cdr";
			fs.copyFromLocalFile(new Path(filename), new Path(path));
			fs.mkdirs(new Path(notifyPath,path.replaceAll("/", "#")));
			fs.setPermission(new Path(notifyPath,path.replaceAll("/", "#")), new FsPermission((short) 777));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("CloudProcessFileSystem.dataFileInput error");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean dataFileInput(InputStream inputStream,String hdfsPath,String localNewFile,String dataPath,String notifyPath) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);
			conf.set("dfs.replication", "2");
			Path hdfspath = new Path(hdfsPath);
			FileSystem fs = FileSystem.get(conf);
			if(!fs.exists(hdfspath)){
				fs.mkdirs(hdfspath);
			}
			hdfspath = new Path(notifyPath);
			if(!fs.exists(hdfspath)){
				fs.mkdirs(hdfspath);
			}
			File file = new File(localNewFile+""+UUID.randomUUID().toString());
			if(!file.exists()){
				file.createNewFile();
				LOG.info("CloudProcessFileSystem.dataFileInput createNewFile ! ");
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			String str = "";
			while((str = br.readLine())!=null){
				bw.write(str);
				bw.newLine();
			}
			br.close();
			bw.close();
			String path = dataPath+UUID.randomUUID().toString()+".cdr";
			fs.copyFromLocalFile(new Path(file.getPath()), new Path(path));
			fs.mkdirs(new Path(notifyPath,path.replaceAll("/", "#")));
			fs.setPermission(new Path(notifyPath,path.replaceAll("/", "#")), new FsPermission((short) 777));
			file.delete();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("CloudProcessFileSystem.dataFileInput error");
			e.printStackTrace();
			return false;
		}
	}
}
