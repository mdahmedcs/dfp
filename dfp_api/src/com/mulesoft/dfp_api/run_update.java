package com.mulesoft.dfp_api;

import java.io.FileReader;
import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;
import com.opencsv.CSVReader;

public class run_update  {


	public void update() throws Exception {

		try {	    	
			new DFP_Manager().UpdateCompanies(new CSVReader(new FileReader("/Users/mbenfante/DFP_Update/DFP_duplicates.csv")));
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in onCall: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);						
		}
		
	
	}
}
