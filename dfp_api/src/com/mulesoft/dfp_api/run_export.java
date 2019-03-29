package com.mulesoft.dfp_api;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

public class run_export {

	public static final String STATUS = "Status";	
	public static final String UPDATE = "U";	
	public static final String DELETE = "D";	
	public static final String INSERT = "A";
	public static final String ADVERTISER = "ADVERTISER";
	public static final String LINE_ITEM = "LINE_ITEM";

	
public void export() {
	
    	ArrayList<HashMap<String, String>> wsCallResultArray = null;
    	ArrayList<HashMap<String, String>> dbCallResultArray = null;
    	String extractFile = "";
    	String extractFilePath = "";
    	String transferFilePath = "";
    	int sleepTime = 0;
    	String delimiter = "|";
    	BufferedReader br = null;
    	int numRecs = 0;
		String reportType = "";
		String[] type= {"ADVERTISER","LINE_ITEM" };
		MuleMessage message = null;
		PrintWriter out = null;
	
		for(int i=0;i<type.length;i++)
		{
			reportType=type[i];
		try {
	    	
	    	sleepTime = Integer.parseInt(SupportFuncs.getProperties().getProperty("mongo_sleep_time"));
	    	MongoManager mongoMgr = new MongoManager();
	    	DFP_Manager dfpMgr = new DFP_Manager();
	    	boolean bActiveCompaniesOnly = true;
	    	
			if (reportType.equals(ADVERTISER)) {
				extractFile = SupportFuncs.getProperties().getProperty("adv_extract_file").replace(".txt", "_" + new TimestampManager().getTimeStamp() + ".txt");				
		    	extractFilePath = SupportFuncs.getProperties().getProperty("file_extract_path") + "/" + extractFile;
		    	transferFilePath = SupportFuncs.getProperties().getProperty("file_path") + "/" + extractFile;
	 	   		SupportFuncs.logMessage("STARTING DFP " + reportType + " Extract to file: " + extractFilePath);				
		    	
		    	//Prepare extract file
		    	out = new PrintWriter(new BufferedWriter(new FileWriter(extractFilePath)));
				//write header record
				out.println(STATUS + delimiter + MongoManager.DFP_ADV_ID_COLUMN + delimiter + MongoManager.DFP_ADV_NAME_COLUMN + delimiter + MongoManager.DFP_ADV_EXT_ID_COLUMN);
				//store results from DFP query into array (All companies)
		    	wsCallResultArray = dfpMgr.exportAdvertisers(!bActiveCompaniesOnly);
		    	//Store results from DB query into array
		    	dbCallResultArray = mongoMgr.GetDfpAdvExportData();   		    	
		    	//Process records
	 	   		processAdvRecs(wsCallResultArray, dbCallResultArray, delimiter, out, mongoMgr, sleepTime);
				out.close();
			}
			else if (reportType.equals(LINE_ITEM)) {
				extractFile = SupportFuncs.getProperties().getProperty("li_extract_file").replace(".txt", "_" + new TimestampManager().getTimeStamp() + ".txt");				
		    	extractFilePath = SupportFuncs.getProperties().getProperty("file_extract_path") + "/" + extractFile;
		    	transferFilePath = SupportFuncs.getProperties().getProperty("file_path") + "/" + extractFile;
	 	   		SupportFuncs.logMessage("STARTING DFP " + reportType + " Extract to file: " + extractFilePath);				

		    	//Prepare extract file
		    	out = new PrintWriter(new BufferedWriter(new FileWriter(extractFilePath)));
				//write header record
				out.println(STATUS + delimiter + MongoManager.DFP_LI_ID_COLUMN + delimiter + MongoManager.DFP_LI_NAME_COLUMN + delimiter + MongoManager.DFP_LI_SF_ACCT_ID_COLUMN + delimiter + MongoManager.DFP_LI_SF_CAMP_ID_COLUMN);
				//store results from DFP query into array
		    	wsCallResultArray = dfpMgr.exportLineItems();
		    	//Store results from DB query into array
		    	dbCallResultArray = mongoMgr.GetDfpLineItemExportData();   		    	
		    	//Process records
	 	   		processLineItemRecs(wsCallResultArray, dbCallResultArray, delimiter, out, mongoMgr, sleepTime);				
				out.close();
			}
	    	
			if (new File(extractFilePath).exists()) {
				br = new BufferedReader(new FileReader(extractFilePath));			
				while (br.readLine() != null) {
					numRecs++;
				}
				br.close();

				if (numRecs == 1) {
		 	   		SupportFuncs.logMessage("Removing empty extract file: " + extractFilePath);
					new File(extractFilePath).delete();				
				}
				else {
					FileUtils.copyFile(new File(extractFilePath), new File(transferFilePath));
		 	   		SupportFuncs.logMessage("Successfully copied extract file for transfer: " + transferFilePath);	
					new File(extractFilePath).delete();				
				}				
			}
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in onCall: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);			
		}
		
			
	}
}
	private void processAdvRecs(ArrayList<HashMap<String, String>> wsCallResultArray, ArrayList<HashMap<String, String>> dbCallResultArray, String delimiter, PrintWriter out, MongoManager mongoMgr, int sleepTime) 
	{
    	boolean bIdsMatch = false;    	

    	try {
        	if (dbCallResultArray.size() > 0) {

     	   		SupportFuncs.logMessage("Comparing dfp query results with DB results...");
     	   		
        		//Compare all DFP query results with DB query results
    	    	for (HashMap<String, String> wsCallMap : wsCallResultArray) {
    	    		bIdsMatch = false;
    		    	for (HashMap<String, String> dbCallMap : dbCallResultArray) {
    		    		//Do Ids match?
    		    		if (wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString())) {
    		    			bIdsMatch = true;
    		    			//Do Names match?
    			    		if (wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString())) {
    			    			//Id and Name match between wsCall and dbCall - check to see if External Ids match
    				    		if (wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString())) {
    				    			//No Action (Id, Name and ExtId match between wsCall and dbCall so this record has not changed)
    				    		}
    				    		else {
    				    			//This is an UPDATE (Id match and External Id mismatch between wsCall and dbCall so this record has changed)
    								out.println(UPDATE + delimiter + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    						    			
    				    			mongoMgr.UpdateDfpAdvExportData(wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	  
    				     	   		SupportFuncs.logMessage("UPDATE: " + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());
    				    		}				    			
    			    		}
    			    		else {
    			    			//This is an UPDATE (Id match and Name mismatch between wsCall and dbCall so this record has changed)
    							out.println(UPDATE + delimiter + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    						    			
    			    			mongoMgr.UpdateDfpAdvExportData(wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	  
    			     	   		SupportFuncs.logMessage("UPDATE: " + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());
    			    		}
    			    		break;				    		
    		    		}
    		    	}
    		    	if (!bIdsMatch) {
    	    			//This is an INSERT (no Id match between wsCall and dbCall so this is a new record from wsCall)
    					out.println(INSERT + delimiter + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    						    						    		
    	    			mongoMgr.StoreDfpAdvExportData(wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    			
    	     	   		SupportFuncs.logMessage("INSERT: " + wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());
    		    	}
    	    	}	    	
        	}
        	else  {
    	    	for (HashMap<String, String> map : wsCallResultArray) {
    				out.println(INSERT + delimiter + map.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + delimiter + map.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + delimiter + map.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    						    						    		
        			mongoMgr.StoreDfpAdvExportData(map.get(MongoManager.DFP_ADV_ID_COLUMN).toString(), map.get(MongoManager.DFP_ADV_NAME_COLUMN).toString(), map.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());	    			
         	   		SupportFuncs.logMessage("INSERT: " + map.get(MongoManager.DFP_ADV_ID_COLUMN).toString() + " " + map.get(MongoManager.DFP_ADV_NAME_COLUMN).toString() + " " + map.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString());
        	        //give server time to write to DB
        	        Thread.sleep(sleepTime);
    	    	}
        	}	    		  		
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in processAdvRecs: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);			    		
    	}
	}
	
	private void processLineItemRecs(ArrayList<HashMap<String, String>> wsCallResultArray, ArrayList<HashMap<String, String>> dbCallResultArray, String delimiter, PrintWriter out, MongoManager mongoMgr, int sleepTime) 
	{
    	boolean bIdsMatch = false;    	

    	try {
        	if (dbCallResultArray.size() > 0) {

     	   		SupportFuncs.logMessage("Comparing dfp query results with DB results...");
     	   		
        		//Compare all DFP query results with DB query results
    	    	for (HashMap<String, String> wsCallMap : wsCallResultArray) {
    	    		bIdsMatch = false;
    		    	for (HashMap<String, String> dbCallMap : dbCallResultArray) {
    		    		//Do Ids match?
    		    		if (wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString())) {
    		    			bIdsMatch = true;
    		    			//Do Names match?
    			    		if (wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString())) {
    			    			//Id and Name match between wsCall and dbCall - check to see if External Ids match
    				    		if (wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString())) {
    				    			//Id, Name and SF AcctId match between wsCall and dbCall - check to see if SF CampIds match
        				    		if (wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString())) {
        				    			//No Action (Id, Name, SF AcctId and SF CampId match between wsCall and dbCall so this record has not changed) 
        				    		}  
        				    		else {
        				    			//This is an UPDATE (SF CampId mismatch between wsCall and dbCall so this record has changed)
        								out.println(UPDATE + delimiter + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    						    			
        				    			mongoMgr.UpdateDfpLineItemExportData(wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	  
        				     	   		SupportFuncs.logMessage("UPDATE: " + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());        				    			
        				    		}
    				    		}
    				    		else {
    				    			//This is an UPDATE (SF AcctId mismatch between wsCall and dbCall so this record has changed)
    								out.println(UPDATE + delimiter + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    						    			
    				    			mongoMgr.UpdateDfpLineItemExportData(wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	  
    				     	   		SupportFuncs.logMessage("UPDATE: " + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString()+ " " + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());
    				    		}				    			
    			    		}
    			    		else {
    			    			//This is an UPDATE (Id match and Name mismatch between wsCall and dbCall so this record has changed)
    							out.println(UPDATE + delimiter + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    						    			
    			    			mongoMgr.UpdateDfpLineItemExportData(wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	  
    			     	   		SupportFuncs.logMessage("UPDATE: " + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());
    			    		}
    			    		break;				    		
    		    		}
    		    	}
    		    	if (!bIdsMatch) {
    	    			//This is an INSERT (no Id match between wsCall and dbCall so this is a new record from wsCall)
    					out.println(INSERT + delimiter + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + delimiter + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    						    						    		
    	    			mongoMgr.StoreDfpLineItemExportData(wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString(), wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    			
    	     	   		SupportFuncs.logMessage("INSERT: " + wsCallMap.get(MongoManager.DFP_LI_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + " " + wsCallMap.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());
    		    	}
    	    	}	    	
        	}
        	else  {
    	    	for (HashMap<String, String> map : wsCallResultArray) {
    				out.println(INSERT + delimiter + map.get(MongoManager.DFP_LI_ID_COLUMN).toString() + delimiter + map.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + delimiter + map.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + delimiter + map.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    						    						    		
        			mongoMgr.StoreDfpLineItemExportData(map.get(MongoManager.DFP_LI_ID_COLUMN).toString(), map.get(MongoManager.DFP_LI_NAME_COLUMN).toString(), map.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString(), map.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());	    			
         	   		SupportFuncs.logMessage("INSERT: " + map.get(MongoManager.DFP_LI_ID_COLUMN).toString() + " " + map.get(MongoManager.DFP_LI_NAME_COLUMN).toString() + " " + map.get(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN).toString() + " " + map.get(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN).toString());
        	        //give server time to write to DB
        	        Thread.sleep(sleepTime);
    	    	}
        	}	    		  		
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in processLineItemRecs: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);			    		
    	}
	}
}
