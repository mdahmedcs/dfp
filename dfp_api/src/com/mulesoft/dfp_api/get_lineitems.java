package com.mulesoft.dfp_api;

import java.util.ArrayList;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

import com.google.gson.Gson;
import com.mulesoft.dfp_api.DFP_Manager.DfpTransaction;

public class get_lineitems  {

	
	public void getlineitems() throws Exception {
		
		String DFPTransactionRecord = "";
		ArrayList<DfpTransaction> dfpTransArrayList = null;
		try {
    		SupportFuncs.logMessage("Starting DFP GetLineItems...");			
			dfpTransArrayList = new DFP_Manager().GetLineItems();
			
			for (DfpTransaction dfpTrans : dfpTransArrayList) {
				//Send json to process queue	
				DFPTransactionRecord = new Gson().toJson(dfpTrans); 
	 	   		SupportFuncs.logMessage("Sending DFP LineItem data to queue: " + DFPTransactionRecord);				
				new SQSManager().sendMessage(DFPTransactionRecord);					
			}
    		SupportFuncs.logMessage("Ending DFP GetLineItems...");		
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in onCall: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);															
		}
		

	}	
}
