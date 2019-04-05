package com.mulesoft.dfp_api;

public class activate_dfp_pipeline 
{


	public void activate_pipeline() throws Exception {
		String awsScriptReports = "";		
		String awsScriptExtract = "";		
		String awsScriptInventory = "";		
    	String[] processType = {"EXTRACT", "REPORT", "INVENTORY"};
    	String sleepTime = "";
    	
    	final String EXTRACT = "EXTRACT";
    	final String REPORT = "REPORT";
    	final String INVENTORY = "INVENTORY";
      for(int i=0;i<processType.length;i++)
      {
		try {
        	sleepTime = SupportFuncs.getProperties().getProperty("sleep_time");
			awsScriptReports = SupportFuncs.getProperties().getProperty("aws_dfp_reports_script");
			awsScriptExtract = SupportFuncs.getProperties().getProperty("aws_dfp_extract_script");
			awsScriptInventory = SupportFuncs.getProperties().getProperty("aws_dfp_inventory_script");
			
			
	    	if (processType[i].equals("")) {
	 	   		String errorMsg = "Error occurred in onCall: Invalid process type...";
	 	   		SupportFuncs.logMessage(errorMsg);
	 	   		SupportFuncs.sendEmailMessage(errorMsg);										    		
	 		
	    	}
	    	
	    	//Provide enough time for files to be transferred to S3
	    	Thread.sleep(Integer.parseInt(sleepTime));
	    	
//			//Kick off AWS EMR Pipeline for either DFP Extract or Reports 
			if (processType[i].equals(EXTRACT)) {
			    new CLIManager().run(awsScriptExtract);
			    SupportFuncs.logMessage("Successfully started AWS EMR pipeline for DFP Extract...");
			}
			else if (processType[i].equals(REPORT)) {
			    new CLIManager().run(awsScriptReports);
			    SupportFuncs.logMessage("Successfully started AWS EMR pipeline for DFP Reports...");							
			}			
			else if (processType[i].equals(INVENTORY)) {
			    new CLIManager().run(awsScriptInventory);
			    SupportFuncs.logMessage("Successfully started AWS EMR pipeline for DFP Inventory...");							
			}				
		}
		catch (Exception e) {
			
		}
				
      }			
	}
}
