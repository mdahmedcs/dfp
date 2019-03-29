package com.mulesoft.dfp_api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

import com.google.api.ads.admanager.lib.client.AdManagerSession;

public class sync_salesdata  {

	public static final String STATUS = "Status";	
	public static final String UPDATE = "U";	
	public static final String UPDATE_ID = "U_ID";	
	public static final String UPDATE_NAME = "U_NAME";	
	public static final String DELETE = "D";	
	public static final String INSERT = "A";
	public static final String NO_CHANGE = "NC";
	public static final String NOT_EXIST_IN_SF = "NOT_EXIST_IN_SF";
	
//	public static final int MAX_SYNC_REQUESTS = 50;


	public void sync() throws Exception {
		
    	ArrayList<HashMap<String, String>> wsCallResultArray = null;
    	ArrayList<Map<String, String>> dbCallResultArray = null;
    	ArrayList<Map<String, String>> dbCallResultAllArray = null;
    	ArrayList<SyncData> sdArrayList = null;
    	DFP_Manager dfpMgr = null;
    	ArrayList<ExceptionData> exceptionArrayList = null;    	
    	List<String> duplicateSalesforceAccountsList = null;
    	Set<String> advertiserNameDupListDeduped = null;
    	List<String> advertiserNameList = new ArrayList<String>();
    	boolean bActiveCompaniesOnly = true;    
    	int MAX_SYNC_REQUESTS = 0;

		try {
			MAX_SYNC_REQUESTS = Integer.parseInt(SupportFuncs.getProperties().getProperty("max_sync_requests"));

	    	MongoManager mongoMgr = new MongoManager();
	    	dfpMgr = new DFP_Manager();

 	   		SupportFuncs.logMessage("STARTING DFP Salesdata Sync - max sync requests: " + MAX_SYNC_REQUESTS);				
	    				
			//Dynamically create list of duplicated Salesforce accounts 
 	   		dbCallResultAllArray = mongoMgr.GetSalesforceAccountExportData(null);   	
			SupportFuncs.logMessage("Number of total accounts found in Salesforce: " + dbCallResultAllArray.size());				    							  			  
 	   		
	    	for (Map<String, String> dbCallMap : dbCallResultAllArray) {
	    		advertiserNameList.add(dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString().toLowerCase());	    		
	    	}			
	    	duplicateSalesforceAccountsList = (List<String>) findDuplicates(advertiserNameList);
	    	
	    	if (duplicateSalesforceAccountsList.size() > 0) {
		    	//Dedup list of duplicated Salesforce accounts 
		    	advertiserNameDupListDeduped = new HashSet<String>();	    	
		    	advertiserNameDupListDeduped.addAll(duplicateSalesforceAccountsList);
		    	duplicateSalesforceAccountsList.clear();
		    	duplicateSalesforceAccountsList.addAll(advertiserNameDupListDeduped);
		    	Collections.sort(duplicateSalesforceAccountsList);
		    	
		    	//Log and email list of duplicated Salesforce accounts 
	 	   		String errorMsg = "The following accounts are duplicated in Salesforce, and are exempt from DFP-Salesforce synchronization:\n\n";
		    	for (String dupAccount : duplicateSalesforceAccountsList) {
	 	   			errorMsg += dupAccount + "\n";
		    	}	    		
	 	   		SupportFuncs.logMessage(errorMsg);
	 	   		SupportFuncs.sendEmailMessage(errorMsg);	    			    		
	    	}
	    	else {
	    		duplicateSalesforceAccountsList = null;
	    	}
 	   		
			//store results from DFP query into array (all companies)
	    	wsCallResultArray = dfpMgr.exportAdvertisers(!bActiveCompaniesOnly);
	    	
	    	if (wsCallResultArray.size() > 0) {
				SupportFuncs.logMessage("Number of companies found in DFP: " + wsCallResultArray.size());				    							  			  

		    	//Store results from DB query into array, (only non-duplicated accounts)
		    	dbCallResultArray = mongoMgr.GetSalesforceAccountExportData(duplicateSalesforceAccountsList);   	
				SupportFuncs.logMessage("Number of non-duplicated accounts found in Salesforce: " + dbCallResultArray.size());				    							  			  
		    			    	
		    	//Compare Salesforce accounts to DFP advertisers, return results of comparison process
		    	sdArrayList = processAdvRecsForUpdIns(wsCallResultArray, dbCallResultArray);

		    	if (sdArrayList.size() > MAX_SYNC_REQUESTS) {
		 	   		SupportFuncs.logMessage(sdArrayList.size() + " sync requests exceed maximum configured value of " + MAX_SYNC_REQUESTS);						    		
		    	}
		    	else {
		 	   		SupportFuncs.logMessage("Processing " + sdArrayList.size() + " sync requests...");				

		 	   		//Reconcile DFP advertisers  
		 			AdManagerSession session = null;
					session = dfpMgr.getAdManagerSession();
			    	
			    	for (SyncData sd : sdArrayList) {
			    		
			    		switch (sd.status) {
			    			case INSERT:
			    				dfpMgr.addCompany(session, sd.companyName, sd.companyId);
			    				break;
			    			case UPDATE_ID:
			    				dfpMgr.updateCompanyIdbyName(session, sd.companyName, sd.companyId);	    				
			    				break;
			    			case UPDATE_NAME:
			    				dfpMgr.updateCompanyNamebyId(session, sd.companyName, sd.companyId);	    				
			    				break;	    					    			
			    		}	    			    			    		
			    	}		    		
		    	}	 
	    	}
	    	else {
	 	   		String errorMsg = "Error occurred in onCall: No advertisers were exported from DFP for company sync with SF...";
	 	   		SupportFuncs.logMessage(errorMsg);	  
	 	   		SupportFuncs.sendEmailMessage(errorMsg);									
	    	}
	    	
  
	    	//Clear out DFP results array
	    	wsCallResultArray.clear();
	    	
			//store results from DFP query into array (get onlu active companies) 
	    	wsCallResultArray = dfpMgr.exportAdvertisers(bActiveCompaniesOnly);	    	

	    	if (wsCallResultArray.size() > 0) {
		    	//Compare DFP advertisers to all Salesforce accounts (including duplicated stored from earlier), record exception cases
		    	exceptionArrayList = processAdvRecsForExceptionCases(wsCallResultArray, dbCallResultAllArray);
		    	
		    	if (exceptionArrayList.size() > 0) {
			    	//Email exception cases...
		 	   		String errorMsg = "The following companies/advertisers exist in DFP but not in Salesforce:\n";
		 	   		for (ExceptionData exData : exceptionArrayList) {
		 	   			errorMsg += exData.companyName + " " + exData.companyId + " " + exData.externalId + "\n";
		 	   		}
		 	   		SupportFuncs.logMessage(errorMsg);
		 	   		SupportFuncs.sendEmailMessage(errorMsg);	    		
		    	}
		    	else {
		    		SupportFuncs.logMessage("All tracked companies/advertisers in DFP also exist in Salesforce...");
		    	}	    		
	    	}
	    	else {
	 	   		String errorMsg = "Error occurred in onCall: No advertisers were exported from DFP for checking against SF...";
	 	   		SupportFuncs.logMessage(errorMsg);	  
	 	   		SupportFuncs.sendEmailMessage(errorMsg);										    		
	    	}

	    		

									    	
 	   		SupportFuncs.logMessage("ENDING DFP Salesdata Sync");				
 	   		
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in onCall: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);									
		}
				
		
	}

	
	private ArrayList<ExceptionData> processAdvRecsForExceptionCases(ArrayList<HashMap<String, String>> wsCallResultArray, ArrayList<Map<String, String>> dbCallResultArray) 
	{

		boolean bIDsMatch = false;
		boolean bNamesMatch = false;
		ArrayList<ExceptionData> exceptionDataArrayList = null;


    	try {
    		exceptionDataArrayList = new ArrayList<ExceptionData>();    		
    		
 	   		SupportFuncs.logMessage("Comparing DFP results with SF results...");
 	   		
    		//Compare all DFP query results with SF DB query results
	    	for (HashMap<String, String> wsCallMap : wsCallResultArray) {
	    		bIDsMatch = false;
	    		bNamesMatch = false;

		    	for (Map<String, String> dbCallMap : dbCallResultArray) {
		    		if (wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_ID_COLUMN).toString())) {
		    			//ID Matches	
		    			bIDsMatch = true;
		    			if (wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString())) {
		    				//Name Matches
		    	    		bNamesMatch = true;
		    	    		break;
		    			}
		    		}
		    		else {
		    			//ID Doesn't Match	
		    			if (wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString())) {
		    				//Name Matches
		    	    		bNamesMatch = true;
		    			}		
		    		}	    		
		    	}
		    	
		    	//Store DFP record if matches exception rule
	    		if (!bIDsMatch && !bNamesMatch) {
	    			ExceptionData eData = new ExceptionData();
	    			eData.companyId = wsCallMap.get(MongoManager.DFP_ADV_ID_COLUMN).toString();
	    			eData.companyName = wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString();
	    			eData.externalId = wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString();
	    			exceptionDataArrayList.add(eData);
	    		}
	    	}	    		       		
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in processAdvRecsForExceptionCases: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);			    		
    	}
    	
    	return exceptionDataArrayList;
	}
	
	private ArrayList<SyncData> processAdvRecsForUpdIns(ArrayList<HashMap<String, String>> wsCallResultArray, ArrayList<Map<String, String>> dbCallResultArray) 
	{
		boolean bIDsMatch = false;
		boolean bNamesMatch = false;
		ArrayList<SyncData> sdArrayList = null;
		
    	try {
    		sdArrayList = new ArrayList<SyncData>();
    		
 	   		SupportFuncs.logMessage("Comparing SF results with DFP results...");
 	   		
    		//Compare all SF query results with DFP query results
	    	for (Map<String, String> dbCallMap : dbCallResultArray) {
	    		bIDsMatch = false;
	    		bNamesMatch = false;
		    	for (HashMap<String, String> wsCallMap : wsCallResultArray) {
		    		//Ids match
		    		if (wsCallMap.get(MongoManager.DFP_ADV_EXT_ID_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_ID_COLUMN).toString())) {
		    			bIDsMatch = true;
		    			
		    			//Names Match
			    		if (wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString())) {
			    			bNamesMatch = true;
			    			//No Action required...
			    			break;
			    		}
		    		}
		    		//Ids do not match		    		
		    		else {
		    			//Names Match
			    		if (wsCallMap.get(MongoManager.DFP_ADV_NAME_COLUMN).toString().equals(dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString())) {
			    			bNamesMatch = true;
			    			break;
			    		}		    			
		    		}
		    	}
//		    	String auditMsg = dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString() + " " + dbCallMap.get(MongoManager.SF_ACCT_ID_COLUMN).toString();
//		    	String fileMsg = delimiter + dbCallMap.get(MongoManager.SF_ACCT_ID_COLUMN).toString() + delimiter + dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString();

		    	SyncData sd = new SyncData();
		    	sd.companyId = dbCallMap.get(MongoManager.SF_ACCT_ID_COLUMN).toString();
		    	sd.companyName = dbCallMap.get(MongoManager.SF_ACCT_NAME_COLUMN).toString();
		    	
//		    	if (bIDsMatch && bNamesMatch) {
//	    			//No Action required...
//					out.println(NO_CHANGE + delimiter + fileMsg);	    						    						    		
//					SupportFuncs.logMessage("NO_CHANGE: " + auditMsg);
//					sd.status = NO_CHANGE;
//		    	}
		    	if (!bIDsMatch && !bNamesMatch) {
//					out.println(INSERT + delimiter + fileMsg);	    						    						    		
//					SupportFuncs.logMessage("INSERT: " + auditMsg);
					sd.status = INSERT;
			    	sdArrayList.add(sd);
			    }
		    	else if (bIDsMatch && !bNamesMatch) {
//					out.println(UPDATE_NAME + delimiter + fileMsg);	    						    						    		
//					SupportFuncs.logMessage("UPDATE_NAME: " + auditMsg);
					sd.status = UPDATE_NAME;
			    	sdArrayList.add(sd);
		    	}
		    	else if (!bIDsMatch && bNamesMatch) {
//					out.println(UPDATE_ID + delimiter + fileMsg);	    						    						    		
//					SupportFuncs.logMessage("UPDATE_ID: " + auditMsg);
					sd.status = UPDATE_ID;
			    	sdArrayList.add(sd);
		    	}    		
	    	}	    		       		
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in processAdvRecsForUpdIns: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);			    		
    	}
    	return sdArrayList;
	}
	
	private <T> Collection<T> findDuplicates(Collection<T> list) {

		Collection<T> duplicates = new ArrayList<T>();
	    Set<T> uniques = new HashSet<T>();

	    for(T t : list) {
	        if(!uniques.add(t)) {
	            duplicates.add(t);
	        }
	    }

	    return duplicates;
	}	
	
	public class SyncData {
		
		public SyncData() {
			status = "";
			companyName = "";
			companyId = "";
		}
		
		public String status;
		public String companyName;
		public String companyId;
	}
	
	public class ExceptionData {
		
		public ExceptionData() {
			status = "";
			companyName = "";
			companyId = "";
			externalId = "";
		}
		
		public String status;
		public String companyName;
		public String companyId;
		public String externalId;
	}	
	
}
