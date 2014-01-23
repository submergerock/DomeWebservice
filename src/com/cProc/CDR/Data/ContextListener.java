package com.cProc.CDR.Data;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.cProc.CDR.util.ChooseIndexUtil;

public class ContextListener implements ServletContextListener {

	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub

	}

	public void contextInitialized(ServletContextEvent context) {
		System.out.println("context : " + context.toString());		
//		ChooseIndexUtil ciu = new ChooseIndexUtil();
//		ciu.getTableInfo();
		System.out.println("==========cProc jetty start !===========");
	}


}
