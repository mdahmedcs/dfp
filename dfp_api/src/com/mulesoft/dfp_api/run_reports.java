package com.mulesoft.dfp_api;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

public class run_reports implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		String reportType = "";
		MuleMessage message = null;
		
		try {	
	    	message = eventContext.getMessage();
	    	reportType = message.getPayloadAsString();		    	
    		SupportFuncs.logMessage("Starting DFP Reports for " + reportType + "...");
			new DFP_Manager().RunReport(reportType);
    		SupportFuncs.logMessage("Ending DFP Reports..." + reportType + "...");			
			
		} catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in onCall: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);												
		}

		return eventContext.getMessage();
	}
}
