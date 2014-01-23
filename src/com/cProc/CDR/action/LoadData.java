package com.cProc.CDR.action;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class LoadData {

	private static final String HDFS = "hdfs://192.168.6.126:9000";
	private static final String DIR = "/usr/local/js/chinamobile/20130728/01/";

	private static final String DATAFILE = "/smp/cdr/bssap/datafile/";
	private static final String NOTIFY = "/smp/cdr_notify/";
	private static final AtomicInteger II = new AtomicInteger(1);
	private static FileSystem FS;

	static {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", HDFS);
		try {
			FS = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			public void run() {
				if (FS != null)
					try {
						FS.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

			}
		}));

	}

	public static void upload() throws IOException, InterruptedException {
		File[] files = new File(DIR).listFiles();
		System.out.println(DIR);
		System.out.println(files.length);
		for (int i = 0; i <files.length; i++) {
			 String path = DATAFILE + java.util.UUID.randomUUID().toString()
			 + ".cdr";
			// String path = DATAFILE + i + ".cdr";
			//String path = DATAFILE + files[i].getName();
			// FS.copyFromLocalFile(new Path(files[i].getAbsolutePath()),
			// new Path(path));
			
			FS.copyFromLocalFile(new Path(files[i].getAbsolutePath()),
					new Path(path));
		
			//Thread.sleep(1000*10);
			FS.mkdirs(new Path(NOTIFY, path.replaceAll("/", "#")));
			String pemission = "drwxrwxrwx";
			FsPermission PERMISSION = FsPermission.valueOf(pemission);
			FS.setPermission(new Path(NOTIFY, path.replaceAll("/", "#")),
					PERMISSION);
			System.out.println(II.getAndIncrement());
		}

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		   upload();
	}

}
