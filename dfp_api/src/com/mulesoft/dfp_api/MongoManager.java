package com.mulesoft.dfp_api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoManager {
	
	//Salesforce MongoDB resources
	private final String SF_DB = "sf_data";
	private final String SF_ACCOUNT_EXPORT_TABLE = "account_export";
	public final static String SF_ACCT_ID_COLUMN = "acct_id";
	public final static String SF_ACCT_NAME_COLUMN = "acct_name";

	public ArrayList<Map<String, String>> GetSalesforceAccountExportData(List<String> duplicateSalesforceAccountsList)
	{
		ArrayList<Map<String, String>> sfDataArrayList = null;
    	Map<String, String> SFDataMap = null;
		
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		sfDataArrayList = new ArrayList<Map<String, String>>();
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		MongoCursor<org.bson.Document> cursor = null;
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(SF_DB);
    			table = db.getCollection(SF_ACCOUNT_EXPORT_TABLE);
    			cursor = table.find(new org.bson.Document()).iterator();
    			while (cursor.hasNext()) {
    				org.bson.Document d = cursor.next();
					SFDataMap = new HashMap<String, String>();						
					SFDataMap.put(SF_ACCT_NAME_COLUMN, (d.get(SF_ACCT_NAME_COLUMN) != null) ? d.get(SF_ACCT_NAME_COLUMN).toString() : "");
					SFDataMap.put(SF_ACCT_ID_COLUMN, (d.get(SF_ACCT_ID_COLUMN) != null) ? d.get(SF_ACCT_ID_COLUMN).toString() : "");				
       				//Only consider Salesforce accounts which are not duplicated, if provided
					if (duplicateSalesforceAccountsList != null) {
						if (!duplicateSalesforceAccountsList.contains(SFDataMap.get(SF_ACCT_NAME_COLUMN).toLowerCase())) {
	    					sfDataArrayList.add(SFDataMap);       					
	       				}
					}
					else {
    					sfDataArrayList.add(SFDataMap);       											
					}
    			}
    			cursor.close();
    			serverConnection.close();  									
        	}  
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}        	
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in GetSalesforceAccountExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}
    	
		return sfDataArrayList;	
	}	
	
	//MongoDB resources
	private final String DFP_DB = "dfp_data";
	private final String DFP_ADV_EXPORT_TABLE = "adv_export";
	private final String DFP_LI_EXPORT_TABLE = "li_export";
	public final static String DFP_ADV_NAME_COLUMN = "adv_name";
	public final static String DFP_ADV_ID_COLUMN = "adv_id";
	public final static String DFP_ADV_EXT_ID_COLUMN = "adv_ext_id";
	public final static String DFP_LI_NAME_COLUMN = "li_name";
	public final static String DFP_LI_ID_COLUMN = "li_id";
	public final static String DFP_LI_SF_ACCT_ID_COLUMN = "li_sf_acct_id";
	public final static String DFP_LI_SF_CAMP_ID_COLUMN = "li_sf_camp_id";


	public void StoreDfpAdvExportData(String advId, String advName, String advExtId)
	{
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		org.bson.Document doc = null;
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_ADV_EXPORT_TABLE);
    			doc = new org.bson.Document(DFP_ADV_ID_COLUMN, advId).append(DFP_ADV_NAME_COLUMN, advName).append(DFP_ADV_EXT_ID_COLUMN, advExtId);
    			table.insertOne(doc);			    			
    			serverConnection.close();
        		SupportFuncs.logMessage("Successfully stored following record in local storage: ADV Id: " + advId + " ADV Name: " + advName + " ADV Ext Id: " + advExtId);
        	}    
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}    	
        }
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in StoreDfpAdvExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}  	
	}
	
	public void UpdateDfpAdvExportData(String advId, String advName, String advExtId)
	{
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		org.bson.Document doc = null;
        		org.bson.Document docToUpdate = null;
        		org.bson.Document docUpdateData = null;       		
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_ADV_EXPORT_TABLE);
    			docToUpdate = new org.bson.Document(DFP_ADV_ID_COLUMN, advId);
    			docUpdateData = new org.bson.Document("$set", new org.bson.Document(DFP_ADV_NAME_COLUMN, advName).append(DFP_ADV_EXT_ID_COLUMN, advExtId));
    			table.updateOne(docToUpdate, docUpdateData);
    			serverConnection.close();
        		SupportFuncs.logMessage("Successfully updated following record in local storage: DFP ADV Name: " + advName + " ADV Id: " + advId + " ADV Ext Id: " + advExtId);
        	}    
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}    	
        }
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in UpdateDfpAdvExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}  	
	}		
	
	public ArrayList<HashMap<String, String>> GetDfpAdvExportData()
	{
		ArrayList<HashMap<String, String>> DfpAdvArrayList = null;
		HashMap<String, String> advData = null;
		
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		DfpAdvArrayList = new ArrayList<HashMap<String, String>>();
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		MongoCursor<org.bson.Document> cursor = null;
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_ADV_EXPORT_TABLE);
    			cursor = table.find(new org.bson.Document()).iterator();
    			while (cursor.hasNext()) {
    				org.bson.Document d = cursor.next();
    				advData = new HashMap<String, String>();
    				advData.put(DFP_ADV_ID_COLUMN, (d.get(DFP_ADV_ID_COLUMN) != null) ? d.get(DFP_ADV_ID_COLUMN).toString() : "");
    				advData.put(DFP_ADV_NAME_COLUMN, (d.get(DFP_ADV_NAME_COLUMN) != null) ? d.get(DFP_ADV_NAME_COLUMN).toString() : "");
    				advData.put(DFP_ADV_EXT_ID_COLUMN, (d.get(DFP_ADV_EXT_ID_COLUMN) != null) ? d.get(DFP_ADV_EXT_ID_COLUMN).toString() : "");
    				DfpAdvArrayList.add(advData);
    			}
    			cursor.close();
    			serverConnection.close();  									
        	}  
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}        	
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in GetDfpAdvExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}
    	
		return DfpAdvArrayList;	
	}
	
	public void StoreDfpLineItemExportData(String liId, String liName, String liSfAcctId, String liSfCampId)
	{
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		org.bson.Document doc = null;
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_LI_EXPORT_TABLE);
    			doc = new org.bson.Document(DFP_LI_ID_COLUMN, liId).append(DFP_LI_NAME_COLUMN, liName).append(DFP_LI_SF_ACCT_ID_COLUMN, liSfAcctId).append(DFP_LI_SF_CAMP_ID_COLUMN, liSfCampId);
    			table.insertOne(doc);			    			
    			serverConnection.close();
        		SupportFuncs.logMessage("Successfully stored following record in local storage: LI Id: " + liId + " LI Name: " + liName + " LI Sf Acct Id: " + liSfAcctId + " LI Sf Camp Id: " + liSfCampId);
        	}    
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}    	
        }
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in StoreDfpLineItemExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}  	
	}
	
	public void UpdateDfpLineItemExportData(String liId, String liName, String liSfAcctId, String liSfCampId)
	{
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		org.bson.Document doc = null;
        		org.bson.Document docToUpdate = null;
        		org.bson.Document docUpdateData = null;       		
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_LI_EXPORT_TABLE);
    			docToUpdate = new org.bson.Document(DFP_LI_ID_COLUMN, liId);
    			docUpdateData = new org.bson.Document("$set", new org.bson.Document(DFP_LI_NAME_COLUMN, liName).append(DFP_LI_SF_ACCT_ID_COLUMN, liSfAcctId).append(DFP_LI_SF_CAMP_ID_COLUMN, liSfCampId));
    			table.updateOne(docToUpdate, docUpdateData);
    			serverConnection.close();
        		SupportFuncs.logMessage("Successfully updated following record in local storage: LI Id: " + liId + " LI Name: " + liName + " LI Sf Acct Id: " + liSfAcctId + " LI Sf Camp Id: " + liSfCampId);
        	}    
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}    	
        }
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in UpdateDfpLineItemExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}  	
	}
	
	public ArrayList<HashMap<String, String>> GetDfpLineItemExportData()
	{
		ArrayList<HashMap<String, String>> DfpLineItemArrayList = null;
		HashMap<String, String> lineItemData = null;
		
    	String mongoDBHost = SupportFuncs.getProperties().getProperty("pt_mongodb_host");
    	String mongoDBPort = SupportFuncs.getProperties().getProperty("pt_mongodb_port");
    	
    	try {
        	if (mongoDBHost != null && mongoDBPort != null) {
        		DfpLineItemArrayList = new ArrayList<HashMap<String, String>>();
        		MongoClient serverConnection = null;
        		MongoDatabase db = null;
        		MongoCollection<org.bson.Document> table = null;
        		MongoCursor<org.bson.Document> cursor = null;
    			serverConnection = new MongoClient(mongoDBHost, Integer.parseInt(mongoDBPort));		
    			db = serverConnection.getDatabase(DFP_DB);
    			table = db.getCollection(DFP_LI_EXPORT_TABLE);
    			cursor = table.find(new org.bson.Document()).iterator();
    			while (cursor.hasNext()) {
    				org.bson.Document d = cursor.next();
    				lineItemData = new HashMap<String, String>();
    				lineItemData.put(DFP_LI_ID_COLUMN, (d.get(DFP_LI_ID_COLUMN) != null) ? d.get(DFP_LI_ID_COLUMN).toString() : "");
    				lineItemData.put(DFP_LI_NAME_COLUMN, (d.get(DFP_LI_NAME_COLUMN) != null) ? d.get(DFP_LI_NAME_COLUMN).toString() : "");
    				lineItemData.put(DFP_LI_SF_ACCT_ID_COLUMN, (d.get(DFP_LI_SF_ACCT_ID_COLUMN) != null) ? d.get(DFP_LI_SF_ACCT_ID_COLUMN).toString() : "");
    				lineItemData.put(DFP_LI_SF_CAMP_ID_COLUMN, (d.get(DFP_LI_SF_CAMP_ID_COLUMN) != null) ? d.get(DFP_LI_SF_CAMP_ID_COLUMN).toString() : "");
    				DfpLineItemArrayList.add(lineItemData);
    			}
    			cursor.close();
    			serverConnection.close();  									
        	}  
        	else  {
        		SupportFuncs.logMessage("Local storage is not setup on this server...");
        	}        	
    	}
    	catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in GetDfpLineItemExportData: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);    		
    	}
    	
		return DfpLineItemArrayList;	
	}	
	
}
