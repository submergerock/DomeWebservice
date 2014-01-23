package com.cProc.CDR.action;

import java.io.File;
import java.util.Map;
import java.util.Set;


public class CleanThread extends Thread
{
  private String resultDir;
  private String notifyDir;
  private final long TIME_OUT = 300000L;

  public CleanThread(String resultDir, String notifyDir) {
    this.resultDir = resultDir;
    this.notifyDir = notifyDir;
  }

  public void run() {
//    System.out.println("begin clean thread");
//		while (true) {
//			try {
//				Thread.sleep(TIME_OUT);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			System.out.println("begin to clean");
//			File resultDirFile = new File(this.resultDir);
//			if (resultDirFile.list() == null) {
//				System.out.println("no file to clean");
//			}
//
//			String[] resultDirs = resultDirFile.list();
//			long systemTime = System.currentTimeMillis();
//			for (int i = 0; i < resultDirs.length; ++i) {
//				String dir = this.resultDir + resultDirs[i];
//				String nDir = this.notifyDir + resultDirs[i];
//				File fileDir = new File(dir);
//				File nFileDir = new File(nDir);
//				long lastModifiedTime = fileDir.lastModified();
//				if (systemTime - lastModifiedTime >= 300000L) {
//					synchronized (JobServlet.DATA_CACHE_TIME) {
//						JobServlet.DATA_CACHE_TIME.remove(resultDirs[i]);
//					}
//					synchronized (JobServlet.DATA_CACHE) {
//						Set<Map.Entry<String, String>> entryseSet=JobServlet.DATA_CACHE.entrySet();
//						for(Map.Entry<String, String> entry:entryseSet) {
//							System.out.println(entry.getKey()+","+entry.getValue()+"," + resultDirs[i]);
//							if(entry.getValue().equals(resultDirs[i])){
//								JobServlet.DATA_CACHE.remove(entry.getKey());;
//							}
//						}
//					}
//					String[] files = fileDir.list();
//					for (int r = 0; r < files.length; ++r) {
//						File file = new File(dir + "/" + files[r]);
//						if (!(file.exists())) {
//							continue;
//						}
//						file.delete();
//						file = null;
//					}
//					String[] notifyFiles = nFileDir.list();
//					for (int r = 0; r < notifyFiles.length; ++r) {
//						File file = new File(nDir + "/" + notifyFiles[r]);
//						if (!(file.exists())) {
//							continue;
//						}
//						file.delete();
//						file = null;
//					}
//				}
//				File resFile = new File(dir);
//				File notiFile = new File(nDir);
//				resFile.delete();
//				notiFile.delete();
//				resFile = null;
//				notiFile = null;
//			}
//			System.out.println("clean success");
//		}
  }
}