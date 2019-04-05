package com.mulesoft.dfp_api;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.lang.Exception;

import com.opencsv.CSVReader;
import com.google.api.ads.common.lib.soap.axis.AxisModule;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;

import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.auth.OfflineCredentials.ForApiBuilder;

//import com.google.api.ads.common.lib.auth.OfflineCredentials.ForApiBuilder;





import com.google.api.ads.admanager.lib.client.AdManagerSession;


import com.google.api.client.auth.oauth2.Credential;


import com.google.common.collect.Iterables;
import com.google.api.ads.admanager.axis.factory.AdManagerServices;


import com.google.api.ads.admanager.lib.utils.ReportCallback;




import com.google.api.ads.admanager.axis.utils.v201902.DateTimes;
import com.google.api.ads.admanager.axis.utils.v201902.ReportDownloader;
import com.google.api.ads.admanager.axis.utils.v201902.StatementBuilder;


import com.google.api.ads.admanager.axis.v201902.ReportServiceInterface;
import com.google.api.ads.admanager.axis.v201902.Stats;



import com.google.api.ads.admanager.axis.v201902.TimeZoneType;
import com.google.api.ads.admanager.axis.v201902.Column;
import com.google.api.ads.admanager.axis.v201902.Company;
import com.google.api.ads.admanager.axis.v201902.CompanyCreditStatus;
import com.google.api.ads.admanager.axis.v201902.CompanyPage;
import com.google.api.ads.admanager.axis.v201902.CompanyServiceInterface;
import com.google.api.ads.admanager.axis.v201902.CompanyType;
import com.google.api.ads.admanager.axis.v201902.DateRangeType;
import com.google.api.ads.admanager.axis.v201902.DateTime;
import com.google.api.ads.admanager.axis.v201902.Dimension;
import com.google.api.ads.admanager.axis.v201902.DimensionAttribute;
import com.google.api.ads.admanager.axis.v201902.ExportFormat;
import com.google.api.ads.admanager.axis.v201902.LineItem;
import com.google.api.ads.admanager.axis.v201902.LineItemPage;
import com.google.api.ads.admanager.axis.v201902.LineItemServiceInterface;
import com.google.api.ads.admanager.axis.v201902.Order;
import com.google.api.ads.admanager.axis.v201902.OrderPage;
import com.google.api.ads.admanager.axis.v201902.OrderServiceInterface;
import com.google.api.ads.admanager.axis.v201902.ReportDownloadOptions;
import com.google.api.ads.admanager.axis.v201902.ReportJob;
import com.google.api.ads.admanager.axis.v201902.ReportQuery;
import com.google.api.ads.admanager.axis.v201902.ReportQueryAdUnitView;
import com.google.api.ads.common.lib.conf.AdsApiConfiguration;
public class DFP_Manager {
	
	private enum DFP_REPORT_TYPE {
		EXTRACT_DAILY,
		EXTRACT_SUPPLEMENT,
		EXTRACT_SUPPLEMENT2,
		EXTRACT_YIELD,
		EXTRACT_FUTURE,
		EXTRACT_FUTURE_NEXT_12_MONTHS,
		EXTRACT_INVENTORY,
		EXTRACT_INVENTORY_NEXT_12_MONTHS
	};
	  	
	private enum DFP_REPORT_STATUS {
		NOT_STARTED,
		IN_PROGRESS,
		COMPLETED,
		FAILED
	}	
	
	private DFP_REPORT_STATUS dfpReportStatus = DFP_REPORT_STATUS.NOT_STARTED;
	private String reportContents = "";	 
	private String reportType = "";	 
	private String filePath = "";
	private String sleepTime = "";

	
	private int countLinesInString(String data)
	{
		int lines = 0;
		Scanner scanner = null;
		
		try {
			scanner = new Scanner(data);
			while (scanner.hasNextLine()) {
			  scanner.nextLine();
			  lines++;
			}
			scanner.close();			
	
			SupportFuncs.logMessage("datastring is " + data.length() + " bytes");
			SupportFuncs.logMessage("datastring consists of " + lines + " lines");
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in countLinesInString: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);				
		}
		return lines;
	}
	
	private ArrayList<String> returnStringArrayList(String data)
	{
		ArrayList<String> stringArrayList = null;
		Scanner scanner = null;

		try {
			scanner = new Scanner(data);
			while (scanner.hasNextLine()) {
				if (stringArrayList == null) {
					stringArrayList = new ArrayList<String>();
				}
				stringArrayList.add(scanner.nextLine());
			}
			scanner.close();				
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in returnStringArrayList: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);							
		}
				
		return stringArrayList;		
	}
	
	public ArrayList<HashMap<String, String>> exportLineItems() 
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		ArrayList<HashMap<String, String>> dfpAdvArrayList = null;
		
		try {
			session = getAdManagerSession();
			if (session != null) {
				AdManagerServices = new AdManagerServices();
				dfpAdvArrayList = runAndExportLineItems(AdManagerServices, session);
			}
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in exportLineItems: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
		return dfpAdvArrayList;
	}
	
	public long getLatestStartDateTimeForAdvertiser(int companyId)
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		DateTime dtOrderStartDate = null;
		Calendar cOrderStartdate = null;
		Calendar cLatestOrderStartDate = null;
		
		try {
			session = getAdManagerSession();
			AdManagerServices = new AdManagerServices();
			
		    // Get the OrderService.
		    OrderServiceInterface orderService =
		        AdManagerServices.get(session, OrderServiceInterface.class);

		    // Create a statement to select all orders.
		    StatementBuilder statementBuilder = new StatementBuilder()
			        .where("advertiserId = :advertiserId")
			        .withBindVariableValue("advertiserId", companyId)		    		
		    		.limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

		    // Default for total result set size.
		    int totalResultSetSize = 0;
			
		    do {
		    	// Get orders by statement.
		        OrderPage page = orderService.getOrdersByStatement(statementBuilder.toStatement());

		        if (page.getResults() != null) {
		        	totalResultSetSize = page.getTotalResultSetSize();
		        	int i = page.getStartIndex();
		        	for (Order order : page.getResults()) {
		        		System.out.printf("%d) Order with ID %d and name '%s' was found.%n", i++, order.getId(), order.getName());
		        		dtOrderStartDate = order.getStartDateTime();
		        		cOrderStartdate = DateTimes.toCalendar(dtOrderStartDate);
		        		if (cLatestOrderStartDate != null) {
		        			if (cOrderStartdate.after(cLatestOrderStartDate)) {
			        			cLatestOrderStartDate = cOrderStartdate;
			        		}
		        		}        			
		        		else {
		        			cLatestOrderStartDate = cOrderStartdate;
		        		}
		        	}
		        }	
		        statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
		    
		    } while (statementBuilder.getOffset() < totalResultSetSize);
		}    
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in getLatestStartDateTimeForAdvertiser: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																					
		}
		
		return cLatestOrderStartDate.getTimeInMillis();
	}
	
	public int getNumOrdersForAdvertiser(int companyId)
	{
		int numOrders = 0;
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		
		try {
			session = getAdManagerSession();
			AdManagerServices = new AdManagerServices();
			
		    // Get the OrderService.
		    OrderServiceInterface orderService =
		        AdManagerServices.get(session, OrderServiceInterface.class);

		    // Create a statement to select all orders.
		    StatementBuilder statementBuilder = new StatementBuilder()
			        .where("advertiserId = :advertiserId")
			        .withBindVariableValue("advertiserId", companyId)		    		
		    		.limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

		    // Default for total result set size.
		    int totalResultSetSize = 0;
			
		    do {
		    	// Get orders by statement.
		        OrderPage page = orderService.getOrdersByStatement(statementBuilder.toStatement());

		        if (page.getResults() != null) {
		        	totalResultSetSize = page.getTotalResultSetSize();
		        	int i = page.getStartIndex();
		        	for (Order order : page.getResults()) {
		        		System.out.printf("%d) Order with ID %d and name '%s' was found.%n", i++, order.getId(), order.getName());
		        		numOrders+=1;
		        	}
		        }	
		        statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
		    
		    } while (statementBuilder.getOffset() < totalResultSetSize);
		}    
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in getNumOrdersForAdvertiser: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																					
		}
		
		return numOrders;
	}
	
	public void addCompany(AdManagerSession session, String companyName, String salesforceId) 
	{
		AdManagerServices AdManagerServices = null;
		Company company = null;
		Company[] companies = null;
		
		try {
			AdManagerServices = new AdManagerServices();
			CompanyServiceInterface companyService = null;

			// Get the CompanyService.
		    companyService = AdManagerServices.get(session, CompanyServiceInterface.class);
		    
		    // Create a new company.
		    company = new Company();
		    company.setName(companyName);
		    company.setType(CompanyType.ADVERTISER);
		    company.setExternalId(salesforceId);
		    company.setCreditStatus(CompanyCreditStatus.ACTIVE);

		    // Create the company on the server.
		    companies = companyService.createCompanies(new Company[] { company});
		    
		    if (companies != null) {
		        for (Company createdCompany : companies) {
		        	SupportFuncs.logMessage("A company with ID " + createdCompany.getId() + " name: " + createdCompany.getName() + " externalId: " + createdCompany.getExternalId() + " type: " + createdCompany.getType() + " has been created.");
		        }		    	
		    }
		    else {
		    	SupportFuncs.logMessage("Error occurred attempting to create company with name: " + companyName + " and externalId: " + salesforceId);	
		    }

		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in addCompany: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
	}	
	
	public void updateCompanyIdbyName(AdManagerSession session, String companyName, String salesforceId) 
	{
		AdManagerServices AdManagerServices = null;
		Company company = null;
		Company[] companies = null;
		
		try {
			AdManagerServices = new AdManagerServices();
			CompanyServiceInterface companyService = null;
			
		    // Get the CompanyService.
		    companyService = AdManagerServices.get(session, CompanyServiceInterface.class);
			
		    // Create a statement to only select a single company by ID.
		    StatementBuilder statementBuilder = new StatementBuilder()
		        .where("name = :name")
		        .orderBy("id ASC")
		        .limit(1)
		        .withBindVariableValue("name", companyName);
		    
		    // Get the company.
		    CompanyPage page =
		        companyService.getCompaniesByStatement(statementBuilder.toStatement());
		    
		    company = Iterables.getOnlyElement(Arrays.asList(page.getResults()));			
		    
		    // Update an existing company.
		    company.setExternalId(salesforceId);

		    // Update the company on the server.
		    companies = companyService.updateCompanies(new Company[] { company});
		    
		    if (companies != null) {
		        for (Company updatedCompany : companies) {
		        	SupportFuncs.logMessage("The company with ID " + updatedCompany.getId() + " name: " + updatedCompany.getName() + " externalId: " + updatedCompany.getExternalId() + " type: " + updatedCompany.getType() + " has been updated.");
		        }		    	
		    }
		    else {
		    	SupportFuncs.logMessage("Error occurred attempting to update company with name: " + companyName + " and externalId: " + salesforceId);	
		    }

		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in updateCompanyIdbyName: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
	}	
	
	public void updateCompanyNamebyId(AdManagerSession session, String companyName, String salesforceId) 
	{
		AdManagerServices AdManagerServices = null;
		Company company = null;
		Company[] companies = null;
		
		try {
			AdManagerServices = new AdManagerServices();
			CompanyServiceInterface companyService = null;
			
		    // Get the CompanyService.
		    companyService = AdManagerServices.get(session, CompanyServiceInterface.class);
			
		    // Create a statement to only select a single company by ID.
		    StatementBuilder statementBuilder = new StatementBuilder()
		        .where("externalId = :externalId")
		        .orderBy("id ASC")
		        .limit(1)
		        .withBindVariableValue("externalId", salesforceId);
		    
		    // Get the company.
		    CompanyPage page =
		        companyService.getCompaniesByStatement(statementBuilder.toStatement());
		    
		    company = Iterables.getOnlyElement(Arrays.asList(page.getResults()));			
		    
		    // Update an existing company.
		    company.setName(companyName);

		    // Create the company on the server.
		    companies = companyService.updateCompanies(new Company[] { company});
		    
		    if (companies != null) {
		        for (Company updatedCompany : companies) {
		        	SupportFuncs.logMessage("The company with ID " + updatedCompany.getId() + " name: " + updatedCompany.getName() + " externalId: " + updatedCompany.getExternalId() + " type: " + updatedCompany.getType() + " has been updated.");
		        }		    	
		    }
		    else {
		    	SupportFuncs.logMessage("Error occurred attempting to update company with name: " + companyName + " and externalId: " + salesforceId);	
		    }

		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in updateCompanyNamebyId: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
	}	
	
	public ArrayList<HashMap<String, String>> exportAdvertisers(boolean bActiveCompaniesOnly) 
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		ArrayList<HashMap<String, String>> dfpAdvArrayList = null;
		
		try {
			session = getAdManagerSession();
			if (session != null) {
				AdManagerServices = new AdManagerServices();
				
				dfpAdvArrayList = runAndExportAdvertisers(AdManagerServices, session, bActiveCompaniesOnly);
			}
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in exportAdvertisers: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
		return dfpAdvArrayList;
	}
	
//	public ArrayList<HashMap<String, String>> exportAdvertisers() 
//	{
//		AdManagerSession session = null;
//		AdManagerServices AdManagerServices = null;
//		ArrayList<HashMap<String, String>> dfpAdvArrayList = null;
//		
//		try {
//			session = getAdManagerSession();
//			if (session != null) {
//				AdManagerServices = new AdManagerServices();
//				dfpAdvArrayList = runAndExportAdvertisers(AdManagerServices, session, "");
//			}
//		}
//		catch (Exception e) {
// 	   		String exceptionMsg = "Exception occurred in exportAdvertisers: " + SupportFuncs.getExceptionError(e);
// 	   		SupportFuncs.logMessage(exceptionMsg);
// 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
//		}
//		return dfpAdvArrayList;
//	}
	
	public ArrayList<DfpTransaction> GetLineItems() 
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		ArrayList<DfpTransaction> dfpTransArrayList = null;
		
		try {
			session = getAdManagerSession();
			if (session != null) {
				AdManagerServices = new AdManagerServices();
				dfpTransArrayList = runAndGetLineItems(AdManagerServices, session);
				
			}
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in GetLineItems: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
		return dfpTransArrayList;
	}
	
	public void UpdateCompanies(CSVReader reader) 
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		
		try {
			session = getAdManagerSession();
			if (session != null) {
				AdManagerServices = new AdManagerServices();
				runAndUpdateCompanies(AdManagerServices, session, reader);
			}
		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in UpdateCompanies: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}		
	}
	
	public void RunReport(String reportTypeInput) 
	{
		AdManagerSession session = null;
		AdManagerServices AdManagerServices = null;
		ArrayList<String> stringArrayList = null;
				
		try {
			reportType = reportTypeInput;
			session = getAdManagerSession();
			
			if (session != null) {
				AdManagerServices = new AdManagerServices();
	        	filePath = SupportFuncs.getProperties().getProperty("file_path");
	        	sleepTime = SupportFuncs.getProperties().getProperty("sleep_time");
    			String reportContents_CURRENT_AND_NEXT_MONTH = "";
    			String reportContents_NEXT_12_MONTHS = "";
			    
			    switch (reportType) {
			    	case "EXTRACT":
		    			filePath += "/dfpextract_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_DAILY);
			    		break;	    	
			    	case "SUPPLEMENT":
		    			filePath += "/dfpextractsupplement_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_SUPPLEMENT);
			    		break;	    	
			    	case "SUPPLEMENT2":
		    			filePath += "/dfpextractsupplement2_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_SUPPLEMENT2);
			    		break;	    	
			    	case "FUTURE":
		    			filePath += "/dfpextractfuture_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_FUTURE);
		    			if (dfpReportStatus == DFP_REPORT_STATUS.COMPLETED) {
			    			reportContents_CURRENT_AND_NEXT_MONTH = reportContents;		    				
		    			}
		    			else {
					    	SupportFuncs.logMessage("Error occurred in CreateDFPReports: Future Report creation did not return with a successful status: " + dfpReportStatus);				    				
		    				return; //failed
		    			}
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_FUTURE_NEXT_12_MONTHS);		    			
			    		break;	    	
			    	case "INVENTORY":
		    			filePath += "/dfpinventory_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_INVENTORY);
		    			if (dfpReportStatus == DFP_REPORT_STATUS.COMPLETED) {
			    			reportContents_CURRENT_AND_NEXT_MONTH = reportContents;		    				
			    			countLinesInString(reportContents);
		    			}
		    			else {
					    	SupportFuncs.logMessage("Error occurred in CreateDFPReports: Inventory Report creation did not return with a successful status: " + dfpReportStatus);				    				
		    				return; //failed
		    			}
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_INVENTORY_NEXT_12_MONTHS);		    			
			    		break;	    	
			    	case "YIELD":
		    			filePath += "/dfpyield_" + getTimeStamp() + ".csv";
		    			runAndCreateReportFile(AdManagerServices, session, DFP_REPORT_TYPE.EXTRACT_YIELD);
			    		break;	    	
			    }
			    String reportContentsFirstLine = "";
			    String[] fieldsArray = null;
			    int fieldIndex = 0;
	            String reportContentsFirstLineNew = "";
				
			    if (dfpReportStatus == DFP_REPORT_STATUS.COMPLETED) {
			    	//SupportFuncs.logMessage(reportContents);
			    	//SupportFuncs.logMessage("All Done");	
			    	//Manipulate report data prior to writing to file
		            switch (reportType) {
				    	case "EXTRACT":
				    		reportContents = cleanRecords(reportContents, "-");				    		
				    		reportContents = cleanRecords(reportContents, "-");				    		
				    	case "SUPPLEMENT":
				    	case "SUPPLEMENT2":
				    	case "YIELD":
					    	//Make header more friendly
					        reportContentsFirstLine = reportContents.substring(0, reportContents.indexOf("\n"));
				            fieldsArray = reportContentsFirstLine.split(",");
				            for (String sField : fieldsArray) 
				            {
				            	fieldsArray[fieldIndex++] = sField.substring(sField.indexOf(".") + 1);
				            }
				            fieldIndex = 0;
				            for (String sField : fieldsArray)
				            {
				            	reportContentsFirstLineNew += fieldsArray[fieldIndex++] + ",";
				            }
				            reportContentsFirstLineNew = reportContentsFirstLineNew.substring(0, reportContentsFirstLineNew.length() - 1);
				            reportContents = reportContents.replace(reportContentsFirstLine, reportContentsFirstLineNew);
				            //Make data more friendly
				            reportContents = filterRecords(reportContents, "_Remarketing", "Ad unit 1");
				    		reportContents = filterRecords(reportContents, "Default", "Ad unit 1");
				    		reportContents = filterDateRecords(reportContents, "Unlimited", "ORDER_END_DATE_TIME");
				    		break;	
				    	case "INVENTORY":
				    	case "FUTURE":
				    		reportContents_NEXT_12_MONTHS = reportContents;
			    			countLinesInString(reportContents);
			        		StringBuilder reportContents_CURRENT_AND_NEXT_MONTH_WITHOUT_HEADER = new StringBuilder();
//			                String reportContents_CURRENT_AND_NEXT_MONTH_WITHOUT_HEADER = "";
			        		StringBuilder reportContents_NEXT_12_MONTHS_WITHOUT_CURRENT_AND_NEXT_MONTHS = new StringBuilder();
//			                String reportContents_NEXT_12_MONTHS_WITHOUT_CURRENT_AND_NEXT_MONTHS = "";
		                    String filterText3by3PreRollPattern1 = '"' + "3 x 3,3 x 3v" + '"';
		                    String filterText3by3PreRollPattern2 = "3 x 3v";
		                    String replacementText3by3PreRoll = "3x3";			                
			                int x = 0;
			                ArrayList monthList = new ArrayList();

			                //Add next year - current month to remove these records from data
			                Calendar date = Calendar.getInstance();  
			                date.setTime(new Date());  
			                Format f = new SimpleDateFormat("yyyy-MM");  
			                date.add(Calendar.YEAR,1);  
			                //SupportFuncs.logMessage(f.format(date.getTime()));  
			                
			                monthList.add(f.format(date.getTime()));

			                //Get months involved in CURRENT_AND_NEXT_MONTH report and remove header
			                //Initialize arrayIndex to 0 for Inventory data, change to 2 for Future data
			                int arrayIndex = 0;
			                if (reportType.equals("FUTURE")) {
			                	arrayIndex = 2;			                	
			                }
			                stringArrayList = returnStringArrayList(reportContents_CURRENT_AND_NEXT_MONTH);
//			                for (String myString : reportContents_CURRENT_AND_NEXT_MONTH.split("\n"))
			                for (String myString : stringArrayList)
			                {
			                    if (x++ != 0) 
			                    {               
			                        String[] recsArray = myString.split(",");
			                        if (!monthList.contains(recsArray[arrayIndex]))
			                        {
			                            monthList.add(recsArray[arrayIndex]);
			                        }
			                        reportContents_CURRENT_AND_NEXT_MONTH_WITHOUT_HEADER.append(myString + "\n");
//			                        reportContents_CURRENT_AND_NEXT_MONTH_WITHOUT_HEADER += myString + "\n";
			                    }                
			                }

			                //Remove records from NEXT_12_MONTHS matching CURRENT_AND_NEXT_MONTH report
			                stringArrayList = returnStringArrayList(reportContents_NEXT_12_MONTHS);
//			                for (String myString : reportContents_NEXT_12_MONTHS.split("\n"))

			                for (String myString : stringArrayList)
			                {
			                    String[] recsArray = myString.split(",");
			                    if (!monthList.contains(recsArray[arrayIndex]))
			                    {
			                        reportContents_NEXT_12_MONTHS_WITHOUT_CURRENT_AND_NEXT_MONTHS.append(myString + "\n");
//			                        reportContents_NEXT_12_MONTHS_WITHOUT_CURRENT_AND_NEXT_MONTHS += myString + "\n";
			                    }
			                }

			                // Concatenate both report files into one
			                reportContents_NEXT_12_MONTHS = reportContents_NEXT_12_MONTHS_WITHOUT_CURRENT_AND_NEXT_MONTHS.toString() + reportContents_CURRENT_AND_NEXT_MONTH_WITHOUT_HEADER.toString();

			                //Process column headers from NEXT_12_MONTHS report stripping off substring up to period 
			                reportContentsFirstLine = reportContents_NEXT_12_MONTHS.substring(0, reportContents_NEXT_12_MONTHS.indexOf("\n"));
			                fieldsArray = reportContentsFirstLine.split(",");
			                for (String sField : fieldsArray) 
			                {
			                    fieldsArray[fieldIndex++] = sField.substring(sField.indexOf(".") + 1);
			                }
			                fieldIndex = 0;
			                for (String sField : fieldsArray)
			                {
			                    reportContentsFirstLineNew += fieldsArray[fieldIndex++] + ",";
			                }
			                reportContentsFirstLineNew = reportContentsFirstLineNew.substring(0, reportContentsFirstLineNew.length() - 1);
			                reportContentsFirstLineNew = reportContentsFirstLineNew.replace("MONTH_AND_YEAR", "MONTH");
			                reportContents_NEXT_12_MONTHS = reportContents_NEXT_12_MONTHS.replace(reportContentsFirstLine, reportContentsFirstLineNew);
			                //Apply filters for Future or Inventory data
			                if (reportType.equals("FUTURE")) {
				                reportContents_NEXT_12_MONTHS = FixMonthColumnFuture(reportContents_NEXT_12_MONTHS);			                	
			                }
			                else {
				                reportContents_NEXT_12_MONTHS = FixMonthColumnInventory(reportContents_NEXT_12_MONTHS);			                	
			                }
			                reportContents_NEXT_12_MONTHS = filterRecords(reportContents_NEXT_12_MONTHS, "_Remarketing", "Ad unit 1");	
			                //Apply filter just for Inventory data
			                if (!reportType.equals("FUTURE")) {
			                	reportContents_NEXT_12_MONTHS = filterRecords(reportContents_NEXT_12_MONTHS, "Default", "Ad unit 1");
			                }
			                //Apply "3 x 3" replacement to both Inventory and Future
		                    reportContents_NEXT_12_MONTHS = reportContents_NEXT_12_MONTHS.replace(filterText3by3PreRollPattern1, replacementText3by3PreRoll);
		                    reportContents_NEXT_12_MONTHS = reportContents_NEXT_12_MONTHS.replace(filterText3by3PreRollPattern2, replacementText3by3PreRoll);
			                reportContents = reportContents_NEXT_12_MONTHS;
				    		break;	
			            }
		        	FileWriter fstream = new FileWriter(filePath);
		        	BufferedWriter out = new BufferedWriter(fstream);
		        	out.write(reportContents);
		        	out.close();	
		        	SupportFuncs.logMessage("Successfully created file: " + filePath + " for operation: " + reportType);
				}
				else {
			    	SupportFuncs.logMessage("Error occurred in RunReport: Report creation did not return with a successful status: " + dfpReportStatus);		
				}				
			}
			else {
	 	   		String errorMsg = "Error occurred in RunReport: Could not successfully get DFP Session";
	 	   		SupportFuncs.logMessage(errorMsg);
	 	   		SupportFuncs.sendEmailMessage(errorMsg);																			
			}


		}
		catch (Exception e) {
 	   		String exceptionMsg = "Exception occurred in RunReport: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);															
		}
	}
	
	public AdManagerSession getAdManagerSession() 
	{
		Credential oAuth2Credential = null;
		AdManagerSession session = null;

		try {				    
		   /* oAuth2Credential = new OfflineCredentials.Builder()
			        .forApi(Api.AD_MANAGER)
			        .fromFile("/opt/mule/conf/ads.properties")
			        .build()
			        .generateCredential(); */
			        
	  oAuth2Credential = new OfflineCredentials.Builder().forApi(Api.AD_MANAGER).fromFile("ads.properties").build().generateCredential();
		//	oAuth2Credential = new OfflineCredentials.Builder().forApi(Api.AD_MANAGER).fro

		
		    // Construct a AdManagerSession.
		    session = new AdManagerSession.Builder()
		        .fromFile()
		        .withOAuth2Credential(oAuth2Credential)
		        .build();
			
		}
		catch (Exception e) {
			e.printStackTrace();
 	   		String exceptionMsg = "Exception occurred in getAdManagerSession: " + SupportFuncs.getExceptionError(e);
 	   		SupportFuncs.logMessage(exceptionMsg);
 	   		SupportFuncs.sendEmailMessage(exceptionMsg);																		
		}
		return session;
	}
	
	  private void runAndCreateReportFile(AdManagerServices AdManagerServices, AdManagerSession session, DFP_REPORT_TYPE reportType) {
		  try {
			  runReport(AdManagerServices, session, reportType);
			  while (dfpReportStatus == DFP_REPORT_STATUS.IN_PROGRESS) {
				  Thread.sleep(Integer.parseInt(sleepTime));
			  }
			  return;
		  }
		  catch (Exception e) {
				String exceptionMsg = "Exception occurred in runAndCreateReportFile: " + SupportFuncs.getExceptionError(e);
				SupportFuncs.logMessage(exceptionMsg);
				SupportFuncs.sendEmailMessage(exceptionMsg);	    		
		  } 	    
	  }

	  private ArrayList<HashMap<String, String>> runAndExportLineItems(AdManagerServices AdManagerServices, AdManagerSession session)
		      throws Exception {

		  ArrayList<HashMap<String, String>> dfpLineItemArrayList = new ArrayList<HashMap<String, String>>();
		  HashMap<String, String> dfpLineItemData = null;
		  String extId = "";
		  String[] parts = null;
		  
		  // Get the LineItemService.
		  LineItemServiceInterface lineItemService =
				  AdManagerServices.get(session, LineItemServiceInterface.class);

		  // Create a statement to select all line items.
		  StatementBuilder statementBuilder = new StatementBuilder()
				  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

		  // Default for total result set size.
		  int totalResultSetSize = 0;
		    
		  do {
			  // Get line items by statement.
		      LineItemPage page = lineItemService.getLineItemsByStatement(statementBuilder.toStatement());

		      if (page.getResults() != null) {
		    	  totalResultSetSize = page.getTotalResultSetSize();
		          int i = page.getStartIndex();
		          for (LineItem lineItem : page.getResults()) {		        	  
		        	  dfpLineItemData = new HashMap<String, String>();
		        	  dfpLineItemData.put(MongoManager.DFP_LI_ID_COLUMN, (lineItem.getId() != null) ? lineItem.getId().toString() : "");
		        	  dfpLineItemData.put(MongoManager.DFP_LI_NAME_COLUMN, (lineItem.getName() != null) ? lineItem.getName() : "");
		        	  extId = (lineItem.getExternalId() != null) ? lineItem.getExternalId() : "";
		        	  if (!extId.equals("")) {
			        	  if (extId.contains("~")) {
				        	  parts = lineItem.getExternalId().split("~");
				        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN, (parts[0] != null) ? parts[0] : "");
				        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN, (parts[1] != null) ? parts[1] : "");
			        	  }		
			        	  else {
				        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN, (lineItem.getExternalId() != null) ? lineItem.getExternalId() : "");
				        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN, "");			        		  
			        	  }
		        	  }
		        	  else {
			        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_ACCT_ID_COLUMN, "");
			        	  dfpLineItemData.put(MongoManager.DFP_LI_SF_CAMP_ID_COLUMN, "");			        		  		        		  
		        	  }

		        	  dfpLineItemArrayList.add(dfpLineItemData);		        	  
		          }
		      }

		      statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
		      
		  } while (statementBuilder.getOffset() < totalResultSetSize);

		  SupportFuncs.logMessage("Number of LineItems found: " + totalResultSetSize);				    				
		      
		  return dfpLineItemArrayList;
	  }	  
	  
	  private ArrayList<HashMap<String, String>> runAndExportAdvertisers(AdManagerServices AdManagerServices, AdManagerSession session, boolean bActiveCompaniesOnly)
		      throws Exception {

		  ArrayList<HashMap<String, String>> dfpAdvArrayList = new ArrayList<HashMap<String, String>>();
		  HashMap<String, String> dfpAdvData = null;
    	  Stats stats = null;
    	  String[] parts = null;
    	  StatementBuilder statementBuilder = null;
    	  String dfpWhiteList = "";

		  try {
			  dfpWhiteList = SupportFuncs.getProperties().getProperty("accounts_in_dfp_not_in_salesforce");

			  // Get the CompanyService.
	    	  CompanyServiceInterface companyService =
					  AdManagerServices.get(session, CompanyServiceInterface.class);

			 // Create a statement to select companies based upon criteria...
			  if (bActiveCompaniesOnly) {	
				  if (!dfpWhiteList.equals("")) {
					  statementBuilder = new StatementBuilder()
						      .where("creditStatus = 'ACTIVE' AND NOT id IN " + dfpWhiteList)				  
							  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);					  
				  }
				  else {
					  statementBuilder = new StatementBuilder()
						      .where("creditStatus = 'ACTIVE'")				  
							  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);					  
				  }
			  }
			  else {
				  statementBuilder = new StatementBuilder()
						  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);
				  
			  }

			  // Default for total result set size.
			  int totalResultSetSize = 0;
			    
			  do {
				  // Get line items by statement.
			      CompanyPage page = companyService.getCompaniesByStatement(statementBuilder.toStatement());

			      if (page.getResults() != null) {
			    	  totalResultSetSize = page.getTotalResultSetSize();
			          int i = page.getStartIndex();
			          for (Company company : page.getResults()) {
			        	  dfpAdvData = new HashMap<String, String>();
			        	  dfpAdvData.put(MongoManager.DFP_ADV_ID_COLUMN, (company.getId() != null) ? company.getId().toString() : "");
			        	  dfpAdvData.put(MongoManager.DFP_ADV_NAME_COLUMN, (company.getName() != null) ? company.getName() : "");
			        	  dfpAdvData.put(MongoManager.DFP_ADV_EXT_ID_COLUMN, (company.getExternalId() != null) ? company.getExternalId() : "");
			        	  dfpAdvArrayList.add(dfpAdvData);		        	  
			          }
			      }

			      statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
			      
			  } while (statementBuilder.getOffset() < totalResultSetSize);

//			  if (!dfpWhiteList.equals("")) {
			  if (bActiveCompaniesOnly) {
				  SupportFuncs.logMessage("Number of active company items found in DFP: " + totalResultSetSize);				    							  			  
			  }
			  else {
				  SupportFuncs.logMessage("Number of total company items found in DFP: " + totalResultSetSize);				    							  
			  }			  
		  }
		  catch (Exception e) {
    		  String exceptionMsg = "Exception occurred in runAndExportAdvertisers: " + SupportFuncs.getExceptionError(e);
    		  SupportFuncs.logMessage(exceptionMsg);
    		  SupportFuncs.sendEmailMessage(exceptionMsg);						    		  			  
		  }

		  return dfpAdvArrayList;
	  }
	  
	  private void runAndUpdateCompanies(AdManagerServices AdManagerServices, AdManagerSession session, CSVReader reader)
		      throws Exception {

    	  String companyName = "";
    	  String accountName = "";
    	  String accountId = "";
    	  int line = 0;
    	  Company company = null;
    	  CompanyPage page = null;
    	  Company[] companies = null;
    	  PrintWriter out = null;
		  
    	  try {
    		  out = new PrintWriter(new BufferedWriter(new FileWriter("/Users/mbenfante/DFP_Update/DFP_duplicates.txt")));				

    		  // Get the CompanyService.
    		  CompanyServiceInterface companyService =
    				  AdManagerServices.get(session, CompanyServiceInterface.class);

        	  //Process incoming company records, if found in DFP update company name and external ID
    	      String [] nextLine;
    	      while ((nextLine = reader.readNext()) != null) {
    	    	  if (line++ > 0) {
    	    		  //Read record from file
    		    	  companyName = nextLine[0];
    		    	  accountName = nextLine[1];
    		    	  accountId = nextLine[2];	 
    		    	  
    				  // Create a statement to select by company name
    				  StatementBuilder statementBuilder = new StatementBuilder()
    						  .where("name = '" + companyName + "'")
    						  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

    				  page = companyService.getCompaniesByStatement(statementBuilder.toStatement());
    				  if (page != null && page.getTotalResultSetSize() > 0) {
    					  company = Iterables.getOnlyElement(Arrays.asList(page.getResults()));
    					  if (company != null) {
    						  // Update the name.
//    						  company.setName(accountName);
    						  company.setExternalId(accountId);
    						    
    						  // Update the company on the server.
    						  try {
        						  companies = companyService.updateCompanies(new Company[] {company});    							  
        						  if (companies != null) {
        							  for (Company updatedCompany : companies) {
        								  String msg = "Company with ID: " + updatedCompany.getId() + ", name: " + updatedCompany.getName() + ", and externalId: " + updatedCompany.getExternalId() + " was updated.";							  
        								  SupportFuncs.logMessage(msg);
        								  out.println(msg);
        							  }								  
        						  }
        						  else {
        					 	   		String errorMsg = "Company with name: " + companyName + " Failed update due to runAndUpdateCompanies error: updateCompanies returned null value";
        					 	   		SupportFuncs.logMessage(errorMsg);		
        					 	   		out.println(errorMsg);
        						  }	
    						  }
    						  catch (Exception innerEx) {
    				    		  String exceptionMsg = "Company with name: " + companyName + " Failed update due to exception: " + SupportFuncs.getExceptionError(innerEx);
    				    		  SupportFuncs.logMessage(exceptionMsg);
								  out.println(exceptionMsg);
    						  }    						  				  
    					  }		
    					  else {
    				 	   		String errorMsg = "Company with name: " + companyName + " Failed update due to runAndUpdateCompanies error: getOnlyElement returned null value";
    				 	   		SupportFuncs.logMessage(errorMsg);						  
					 	   		out.println(errorMsg);    							
    					  }
    				  }
    				  else {
    			 	   		String errorMsg = "Company with name: " + companyName + " Failed update due to runAndUpdateCompanies error: getCompaniesByStatement returned null value or 0 results";
    			 	   		SupportFuncs.logMessage(errorMsg);
				 	   		out.println(errorMsg);    						
    				  }
    	    	  }
    	      }    
    	  }
    	  catch (Exception e) {
    		  String exceptionMsg = "Exception occurred in runAndUpdateCompanies: " + SupportFuncs.getExceptionError(e);
    		  SupportFuncs.logMessage(exceptionMsg);
    		  SupportFuncs.sendEmailMessage(exceptionMsg);						    		  
    	  }
    	  finally {
    	      out.close();    		  
    	  }
	  }
	  
	  private ArrayList<DfpTransaction> runAndGetLineItems(AdManagerServices AdManagerServices, AdManagerSession session)
		      throws Exception {

		  ArrayList<DfpTransaction> dfpTransArrayList = new ArrayList<DfpTransaction>();
		  DfpTransaction dfpTrans = null;
    	  Stats stats = null;
    	  String[] parts = null;
		  
		  // Get the LineItemService.
		  LineItemServiceInterface lineItemService =
				  AdManagerServices.get(session, LineItemServiceInterface.class);

		  // Create a statement to select all line items.
		  StatementBuilder statementBuilder = new StatementBuilder()
				  .where("externalId LIKE '%~%'")
				  .limit(StatementBuilder.SUGGESTED_PAGE_LIMIT);

		  // Default for total result set size.
		  int totalResultSetSize = 0;
		    
		  do {
			  // Get line items by statement.
		      LineItemPage page = lineItemService.getLineItemsByStatement(statementBuilder.toStatement());

		      if (page.getResults() != null) {
		    	  totalResultSetSize = page.getTotalResultSetSize();
		          int i = page.getStartIndex();
		          for (LineItem lineItem : page.getResults()) {	
		    		  SupportFuncs.logMessage("Found lineitem: " + lineItem.getName() + " " + lineItem.getId() + " " + lineItem.getExternalId());				    				
		        	  dfpTrans = new DfpTransaction();
		        	  stats = lineItem.getStats();
		        	  if (stats != null) {
			        	  dfpTrans.clickCount = stats.getClicksDelivered();
			        	  parts = lineItem.getExternalId().split("~");
			        	  if (parts != null && parts.length > 1) {
				        	  dfpTrans.campaignId = parts[1];
				        	  dfpTransArrayList.add(dfpTrans);				        		  			        		  
			        	  }
		        	  }       	  
		          }
		      }

		      statementBuilder.increaseOffsetBy(StatementBuilder.SUGGESTED_PAGE_LIMIT);
		      
		  } while (statementBuilder.getOffset() < totalResultSetSize);

		  SupportFuncs.logMessage("Number of LineItems found: " + totalResultSetSize);				    				
		      
		  return dfpTransArrayList;
	  }

	  private void runReport(AdManagerServices AdManagerServices, AdManagerSession session, DFP_REPORT_TYPE reportType)
	      throws Exception {
	    // Get the ReportService.

	    ReportServiceInterface reportService = (ReportServiceInterface) AdManagerServices.get(session, ReportServiceInterface.class);

	    // Create report query.
	    ReportQuery reportQuery = new ReportQuery();
		TimeZoneType tzType = null;
	    	    
	    switch (reportType) {
	    	case EXTRACT_DAILY: //Formerly Delivery1 in .NET code version (dfpextract.csv)
	    	    reportQuery.setDimensions(new Dimension[] {Dimension.DATE, Dimension.ADVERTISER_NAME, Dimension.CREATIVE_NAME, Dimension.CREATIVE_SIZE, Dimension.CREATIVE_TYPE, Dimension.LINE_ITEM_NAME, Dimension.LINE_ITEM_TYPE, Dimension.ORDER_NAME, Dimension.AD_UNIT_NAME, Dimension.ADVERTISER_ID, Dimension.CREATIVE_ID, Dimension.LINE_ITEM_ID, Dimension.ORDER_ID, Dimension.AD_UNIT_ID });    
	    	    reportQuery.setDimensionAttributes(new DimensionAttribute[] {DimensionAttribute.CREATIVE_CLICK_THROUGH_URL, DimensionAttribute.LINE_ITEM_CREATIVE_START_DATE, DimensionAttribute.LINE_ITEM_CREATIVE_END_DATE, DimensionAttribute.LINE_ITEM_START_DATE_TIME, DimensionAttribute.LINE_ITEM_END_DATE_TIME, DimensionAttribute.LINE_ITEM_COST_PER_UNIT, DimensionAttribute.LINE_ITEM_DELIVERY_PACING, DimensionAttribute.LINE_ITEM_GOAL_QUANTITY, DimensionAttribute.LINE_ITEM_PRIORITY, DimensionAttribute.LINE_ITEM_LIFETIME_IMPRESSIONS, DimensionAttribute.LINE_ITEM_LIFETIME_CLICKS, DimensionAttribute.ORDER_START_DATE_TIME, DimensionAttribute.ORDER_END_DATE_TIME, DimensionAttribute.AD_UNIT_CODE });    
//	    	    reportQuery.setColumns(new Column[] {Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS, Column.TOTAL_LINE_ITEM_LEVEL_CLICKS, Column.TOTAL_LINE_ITEM_LEVEL_CTR, Column.TOTAL_ACTIVE_VIEW_ELIGIBLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_RATE, Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE, Column.RICH_MEDIA_BACKUP_IMAGES, Column.RICH_MEDIA_DISPLAY_TIME, Column.RICH_MEDIA_AVERAGE_DISPLAY_TIME, Column.RICH_MEDIA_EXPANSIONS, Column.RICH_MEDIA_EXPANDING_TIME, Column.RICH_MEDIA_INTERACTION_TIME, Column.RICH_MEDIA_INTERACTION_COUNT, Column.RICH_MEDIA_INTERACTION_RATE, Column.RICH_MEDIA_AVERAGE_INTERACTION_TIME, Column.RICH_MEDIA_INTERACTION_IMPRESSIONS, Column.RICH_MEDIA_MANUAL_CLOSES, Column.RICH_MEDIA_FULL_SCREEN_IMPRESSIONS, Column.RICH_MEDIA_VIDEO_INTERACTIONS, Column.RICH_MEDIA_VIDEO_INTERACTION_RATE, Column.RICH_MEDIA_VIDEO_PLAYES, Column.RICH_MEDIA_VIDEO_COMPLETES, Column.RICH_MEDIA_VIDEO_REPLAYS, Column.RICH_MEDIA_VIDEO_VIEW_TIME, Column.RICH_MEDIA_VIDEO_VIEW_RATE, Column.VIDEO_VIEWERSHIP_START, Column.VIDEO_VIEWERSHIP_FIRST_QUARTILE, Column.VIDEO_VIEWERSHIP_MIDPOINT, Column.VIDEO_VIEWERSHIP_THIRD_QUARTILE, Column.VIDEO_VIEWERSHIP_COMPLETE, Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE, Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME, Column.VIDEO_VIEWERSHIP_COMPLETION_RATE, Column.VIDEO_VIEWERSHIP_ENGAGED_VIEW, Column.VIDEO_VIEWERSHIP_AUTO_PLAYS, Column.VIDEO_VIEWERSHIP_CLICK_TO_PLAYS, Column.VIDEO_INTERACTION_AVERAGE_INTERACTION_RATE });	    	    
	    	    reportQuery.setColumns(new Column[] {Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS, Column.TOTAL_LINE_ITEM_LEVEL_CLICKS, Column.TOTAL_LINE_ITEM_LEVEL_CTR, Column.TOTAL_ACTIVE_VIEW_ELIGIBLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS, Column.TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS_RATE, Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE, Column.RICH_MEDIA_BACKUP_IMAGES, Column.RICH_MEDIA_DISPLAY_TIME, Column.RICH_MEDIA_AVERAGE_DISPLAY_TIME, Column.RICH_MEDIA_EXPANSIONS, Column.RICH_MEDIA_EXPANDING_TIME, Column.RICH_MEDIA_INTERACTION_TIME, Column.RICH_MEDIA_INTERACTION_COUNT, Column.RICH_MEDIA_INTERACTION_RATE, Column.RICH_MEDIA_AVERAGE_INTERACTION_TIME, Column.RICH_MEDIA_INTERACTION_IMPRESSIONS, Column.RICH_MEDIA_MANUAL_CLOSES, Column.RICH_MEDIA_FULL_SCREEN_IMPRESSIONS, Column.RICH_MEDIA_VIDEO_INTERACTIONS, Column.RICH_MEDIA_VIDEO_INTERACTION_RATE, Column.RICH_MEDIA_VIDEO_PLAYES, Column.RICH_MEDIA_VIDEO_COMPLETES, Column.RICH_MEDIA_VIDEO_REPLAYS, Column.RICH_MEDIA_VIDEO_VIEW_TIME, Column.RICH_MEDIA_VIDEO_VIEW_RATE, Column.VIDEO_VIEWERSHIP_START, Column.VIDEO_VIEWERSHIP_FIRST_QUARTILE, Column.VIDEO_VIEWERSHIP_MIDPOINT, Column.VIDEO_VIEWERSHIP_THIRD_QUARTILE, Column.VIDEO_VIEWERSHIP_COMPLETE, Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_RATE, Column.VIDEO_VIEWERSHIP_AVERAGE_VIEW_TIME, Column.VIDEO_VIEWERSHIP_COMPLETION_RATE, Column.VIDEO_VIEWERSHIP_ENGAGED_VIEW, Column.VIDEO_VIEWERSHIP_AUTO_PLAYS, Column.VIDEO_VIEWERSHIP_CLICK_TO_PLAYS });	    	    
	    	    reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);   
	    	//    reportQuery.setDateRangeType(DateRangeType.YESTERDAY);	    
		   	reportQuery.setDateRangeType(DateRangeType.CUSTOM_DATE);		        
	         reportQuery.setStartDate(
	                  DateTimes.toDateTime("2019-04-02T00:00:00", "America/New_York").getDate());
	    	    reportQuery.setEndDate(
	    	                DateTimes.toDateTime("2019-04-03T00:00:00", "America/New_York").getDate()); 	    	    
	    		break;
	    	case EXTRACT_SUPPLEMENT: //Formerly Delivery2 in .NET code version (dfpextractsupplement.csv)
	    		reportQuery.setDimensions(new Dimension[] {Dimension.DATE, Dimension.LINE_ITEM_NAME, Dimension.CUSTOM_CRITERIA, Dimension.LINE_ITEM_ID, Dimension.CUSTOM_TARGETING_VALUE_ID, Dimension.CREATIVE_ID, Dimension.AD_UNIT_ID});
	    	    reportQuery.setDimensionAttributes(new DimensionAttribute[] {DimensionAttribute.LINE_ITEM_DELIVERY_INDICATOR });    
	    		reportQuery.setColumns(new Column[] {Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS, Column.TOTAL_LINE_ITEM_LEVEL_CLICKS, Column.TOTAL_LINE_ITEM_LEVEL_CTR });
		    	reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);    
		 //  	reportQuery.setDateRangeType(DateRangeType.YESTERDAY);		        
		    	reportQuery.setDateRangeType(DateRangeType.CUSTOM_DATE);		        
		    	reportQuery.setStartDate(
		   			DateTimes.toDateTime("2019-04-02T00:00:00", "America/New_York").getDate());
		    	reportQuery.setEndDate(
		    			DateTimes.toDateTime("2019-04-03T00:00:00", "America/New_York").getDate());		    	
	    		break;    		
	    	case EXTRACT_SUPPLEMENT2: //Remove Dimension.CUSTOM_CRITERIA  and Dimension.CUSTOM_TARGETING_VALUE_ID.
	    		reportQuery.setDimensions(new Dimension[] {Dimension.DATE, Dimension.LINE_ITEM_NAME, Dimension.LINE_ITEM_ID, Dimension.CREATIVE_ID, Dimension.AD_UNIT_ID});
	    	    reportQuery.setDimensionAttributes(new DimensionAttribute[] {DimensionAttribute.LINE_ITEM_DELIVERY_INDICATOR });    
	    		reportQuery.setColumns(new Column[] {Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS, Column.TOTAL_LINE_ITEM_LEVEL_CLICKS, Column.TOTAL_LINE_ITEM_LEVEL_CTR });
		    	reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);    
		   // 	reportQuery.setDateRangeType(DateRangeType.YESTERDAY);		        
		    	reportQuery.setDateRangeType(DateRangeType.CUSTOM_DATE);		        
		        reportQuery.setStartDate(
    		              DateTimes.toDateTime("2019-04-02T00:00:00", "America/New_York").getDate());
		      reportQuery.setEndDate(
		              DateTimes.toDateTime("2019-04-03T00:00:00", "America/New_York").getDate());		    	
	    		break;    		
	    	case EXTRACT_YIELD: //Formerly Delivery4 in .NET code version (dfpyield.csv)
	    		reportQuery.setDimensions(new Dimension[] {Dimension.DATE, Dimension.AD_UNIT_NAME});
	    		reportQuery.setColumns(new Column[] {Column.TOTAL_INVENTORY_LEVEL_UNFILLED_IMPRESSIONS});
		    	reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);    	    		
	    	    reportQuery.setDateRangeType(DateRangeType.LAST_MONTH);   		
	    		break;
	    	case EXTRACT_FUTURE: //Formerly own solution in .NET code version (dfpextractfuture.csv)
	    	case EXTRACT_FUTURE_NEXT_12_MONTHS: //Formerly own solution in .NET code version (dfpextractfuture.csv)
	    		reportQuery.setDimensions(new Dimension[] {Dimension.AD_REQUEST_AD_UNIT_SIZES, Dimension.AD_UNIT_NAME, Dimension.MONTH_AND_YEAR, Dimension.ADVERTISER_NAME, Dimension.LINE_ITEM_NAME, Dimension.AD_UNIT_ID, Dimension.ADVERTISER_ID, Dimension.LINE_ITEM_ID, Dimension.LINE_ITEM_TYPE});
		    	reportQuery.setDimensionAttributes(new DimensionAttribute[] {DimensionAttribute.LINE_ITEM_START_DATE_TIME, DimensionAttribute.LINE_ITEM_END_DATE_TIME, DimensionAttribute.LINE_ITEM_DELIVERY_PACING, DimensionAttribute.LINE_ITEM_GOAL_QUANTITY});    
	    		reportQuery.setColumns(new Column[] {Column.SELL_THROUGH_SELL_THROUGH_RATE, Column.SELL_THROUGH_RESERVED_IMPRESSIONS, Column.SELL_THROUGH_AVAILABLE_IMPRESSIONS, Column.SELL_THROUGH_FORECASTED_IMPRESSIONS});
		    	reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);    
		    	if (reportType == DFP_REPORT_TYPE.EXTRACT_FUTURE) {
		    	    reportQuery.setDateRangeType(DateRangeType.CURRENT_AND_NEXT_MONTH);   				    		
		    	}
		    	else {
		    	    reportQuery.setDateRangeType(DateRangeType.NEXT_12_MONTHS); 
		    	}
	    		break;
	    	case EXTRACT_INVENTORY://Formerly own solution in .NET code version (dfpinventory.csv)
	    	case EXTRACT_INVENTORY_NEXT_12_MONTHS://Formerly own solution in .NET code version (dfpinventory.csv)
	    		reportQuery.setDimensions(new Dimension[] {Dimension.MONTH_AND_YEAR, Dimension.AD_REQUEST_AD_UNIT_SIZES, Dimension.AD_UNIT_NAME, Dimension.AD_REQUEST_CUSTOM_CRITERIA});
//	    		reportQuery.setDimensions(new Dimension[] {Dimension.MONTH_AND_YEAR, Dimension.AD_REQUEST_AD_UNIT_SIZES, Dimension.AD_UNIT_NAME});
	    		reportQuery.setColumns(new Column[] {Column.SELL_THROUGH_SELL_THROUGH_RATE, Column.SELL_THROUGH_RESERVED_IMPRESSIONS, Column.SELL_THROUGH_AVAILABLE_IMPRESSIONS, Column.SELL_THROUGH_FORECASTED_IMPRESSIONS});
		    	reportQuery.setAdUnitView(ReportQueryAdUnitView.HIERARCHICAL);   		    	
		    	if (reportType == DFP_REPORT_TYPE.EXTRACT_INVENTORY) {
		    	    reportQuery.setDateRangeType(DateRangeType.CURRENT_AND_NEXT_MONTH);   		
		    	}
		    	else {
		    	    reportQuery.setDateRangeType(DateRangeType.NEXT_12_MONTHS);   				    		
//		    	    reportQuery.setDateRangeType(DateRangeType.NEXT_QUARTER);   				    		
		    	}
	    		break;  		
	    }
	        
	    // Create report job.
	    ReportJob reportJob = new ReportJob();
	    reportJob.setReportQuery(reportQuery);

	    // Run report job.
	    try {
	    	reportJob = reportService.runReportJob(reportJob);
	    	dfpReportStatus = DFP_REPORT_STATUS.IN_PROGRESS;
	    }
	    catch (Exception e) {
	    	dfpReportStatus = DFP_REPORT_STATUS.FAILED;
			String exceptionMsg = "Exception occurred in runReport: " + SupportFuncs.getExceptionError(e);
			SupportFuncs.logMessage(exceptionMsg);
			SupportFuncs.sendEmailMessage(exceptionMsg);	    		
	    }

	    // Create report downloader.
	    final ReportDownloader reportDownloader =
	        new ReportDownloader(reportService, reportJob.getId());

	    reportDownloader.whenReportReady(new ReportCallback() {
	      public void onSuccess() {	    	
	        try {       
	        	ReportDownloadOptions options = new ReportDownloadOptions();
	        	options.setExportFormat(ExportFormat.CSV_DUMP);
	        	options.setUseGzipCompression(true);        	
	        	reportContents = reportDownloader.getReportAsCharSource(options).read();      	        	
//	        	reportContents = reportDownloader.getReport(ExportFormat.CSV_DUMP);
		    	//SupportFuncs.logMessage(reportContents);
		    	dfpReportStatus = DFP_REPORT_STATUS.COMPLETED;
 	          
	        } catch (IOException e) {
		    	dfpReportStatus = DFP_REPORT_STATUS.FAILED;
				String exceptionMsg = "Exception occurred in reportDownloader.whenReportReady.OnSuccess: " + SupportFuncs.getExceptionError(e);
				SupportFuncs.logMessage(exceptionMsg);
				SupportFuncs.sendEmailMessage(exceptionMsg);	    		
	        }
	      }

	      public void onInterruption() {
	    	  dfpReportStatus = DFP_REPORT_STATUS.FAILED;
	    	  SupportFuncs.logMessage(" Error occurred in reportDownloader.whenReportReady: Report download interupted...");		
	      }

	      public void onFailure() {
		      dfpReportStatus = DFP_REPORT_STATUS.FAILED;
	    	  SupportFuncs.logMessage(" Error occurred in reportDownloader.whenReportReady: Report download failed...");		
	      }

	      public void onException(Exception e) {
	    	  dfpReportStatus = DFP_REPORT_STATUS.FAILED;
	    	  String exceptionMsg = "Exception occurred in reportDownloader.whenReportReady.OnException: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		
	      }
	    });
	  }

      private String filterDateRecords(String rptData, String criteria, String filterDateColumn)
      {
          int recPos = 0;
          int criteriaFieldPos = 0;
//          String retRptData = "";
          StringBuilder retRptData = new StringBuilder();
		  List content = null;

          
          try {
              //Split report data into records
              String[] rptRecs = rptData.split("\n");

              //Write header record to return data
              retRptData.append(rptRecs[recPos] + "\n");
              
    		  CSVReader reader = new CSVReader(new InputStreamReader(IOUtils.toInputStream(rptData, "UTF-8")));
    		  content = reader.readAll();      
    		  
    		  for (Object object : content) {
    			  String[] rptRecFieldsArray = (String[]) object;
            	  if (recPos == 0)
            	  {
            		  //Determine position of filter column
            		  int fieldPos = 0;
            		  for (String sField : rptRecFieldsArray)
            		  {
            			  if (sField.equals(filterDateColumn))
            			  {
            				  criteriaFieldPos = fieldPos;
            				  break;
            			  }
            			  fieldPos += 1;
            		  }
            		  recPos += 1;
            		  continue;
            	  }

            	  if (rptRecFieldsArray[criteriaFieldPos].indexOf(criteria) == -1)
            	  {
            		  //Write data record to return data after applied filter
            		  retRptData.append(rptRecs[recPos] + "\n");
            	  }
            	  else
            	  {
            		  //Found criteria so replace...
            		  retRptData.append(rptRecs[recPos].replace(criteria, "9999-12-31T23:59:00-04:00") + "\n");
            	  }
            	  
            	  recPos += 1;            	          	  
    		  }
           	  
          }
          catch (Exception e) {
	    	  String exceptionMsg = "Exception occurred in filterDateRecords: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		        	  
          }

          return retRptData.toString();
      }

      private String filterRecords(String rptData, String criteria, String filterColumn)
      {
          int recPos = 0;
          int criteriaFieldPos = 0;
//          String retRptData = "";
          StringBuilder retRptData = new StringBuilder();
          
		  List<String[]> content = null;
          
          try {
              //Split report data into records
              String[] rptRecs = rptData.split("\n");

              //Write header record to return data
//              retRptData = rptRecs[recPos] + "\n";
              retRptData.append(rptRecs[recPos] + "\n");

    		  CSVReader reader = new CSVReader(new InputStreamReader(IOUtils.toInputStream(rptData, "UTF-8")));
    		  content = reader.readAll();      
    		  
    		  for (Object object : content) {
    			  String[] rptRecFieldsArray = (String[]) object;

                  if (recPos == 0)
                  {
                      //Determine position of filter column
                      int fieldPos = 0;
                      for (String sField : rptRecFieldsArray)
                      {
                          if (sField.equals(filterColumn))
                          {
                              criteriaFieldPos = fieldPos;
                              break;
                          }
                          fieldPos += 1;
                      }
                      recPos += 1;
                      continue;
                  }
                  reader.close();
                  
                  if (rptRecFieldsArray[criteriaFieldPos].indexOf(criteria) == -1)
                  {
                      //Write data record to return data after applied filter
//                      retRptData += rptRecs[recPos] + "\n";
                	  retRptData.append(rptRecs[recPos] + "\n");
                  }

                  recPos += 1;    			  
    		  }              
        	  
          }
          catch (Exception e) {
	    	  String exceptionMsg = "Exception occurred in filterRecords: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		        	          	  
          }

          return retRptData.toString();
      }
      
      private String cleanRecords(String rptData, String criteria)
      {
          int recPos = 0;
          int criteriaFieldPos = 0;
//          String retRptData = "";
          StringBuilder retRptData = new StringBuilder();

		  List content = null;
          
          try {
              //Split report data into records
              String[] rptRecs = rptData.split("\n");

              //Write header record to return data
              retRptData.append(rptRecs[recPos] + "\n");

    		  CSVReader reader = new CSVReader(new InputStreamReader(IOUtils.toInputStream(rptData, "UTF-8")));
    		  content = reader.readAll();      
    		  
    		  for (Object object : content) {
    			  String[] rptRecFieldsArray = (String[]) object;

                  if (recPos == 0)
                  {
                      recPos += 1;
                      continue;
                  }
                  else {
                      retRptData.append(rptRecs[recPos].replace("," + criteria + ",", ",0,") + "\n");                	  
                  }
                  
                  recPos += 1;    			  
    		  }              
        	  
          }
          catch (Exception e) {
	    	  String exceptionMsg = "Exception occurred in cleanRecords: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		        	          	  
          }

          return retRptData.toString();
      }
      
      public static String FixMonthColumnFuture(String rptData)
      {
          int recPos = 0;
//          String retRptData = "";
          StringBuilder retRptData = new StringBuilder();

		  List content = null;

		  try {
	          //Split report data into records
	          String[] rptRecs = rptData.split("\n");

	          //Write header record to return data
	          retRptData.append(rptRecs[recPos] + "\n");

			  CSVReader reader = new CSVReader(new InputStreamReader(IOUtils.toInputStream(rptData, "UTF-8")));
			  content = reader.readAll();      

	   		  for (Object object : content) {
    			  String[] rptRecFieldsArray = (String[]) object;
    			  
                  if (recPos == 0)
                  {
                      recPos += 1;
                      continue;
                  }
                  //Write data record to return data after removing year from year-month string
                  if (rptRecFieldsArray[2].contains("-"))
                  {
                      String[] yearMonthArray = rptRecFieldsArray[2].split("-");
                      rptRecs[recPos] = rptRecs[recPos].replace("," + yearMonthArray[0] + "-" + yearMonthArray[1] + ",", "," + yearMonthArray[1] + ",");
                  }
                  retRptData.append(rptRecs[recPos] + "\n");

                  recPos += 1;    			  
	   		  }
		  }
		  catch (Exception e) {
	    	  String exceptionMsg = "Exception occurred in FixMonthColumnFuture: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		        	          	  			  
		  }

          return retRptData.toString();
      }
      
      private String FixMonthColumnInventory(String rptData)
      {
          int recPos = 0;
//          String retRptData = "";
          StringBuilder retRptData = new StringBuilder();

		  List content = null;

		  try {
	          //Split report data into records
	          String[] rptRecs = rptData.split("\n");

	          //Write header record to return data
	          retRptData.append(rptRecs[recPos] + "\n");
	          
			  CSVReader reader = new CSVReader(new InputStreamReader(IOUtils.toInputStream(rptData, "UTF-8")));
			  content = reader.readAll();      
	          
	   		  for (Object object : content) {
    			  String[] rptRecFieldsArray = (String[]) object;
 
    			  if (recPos == 0)
                  {
                      recPos += 1;
                      continue;
                  }
                  //Write data record to return data after removing year from year-month string
                  if (rptRecFieldsArray[0].contains("-")) 
                  {
                      String[] yearMonthArray = rptRecFieldsArray[0].split("-");
                      rptRecs[recPos] = rptRecs[recPos].replace(yearMonthArray[0] + "-", "");
                  }
                  retRptData.append(rptRecs[recPos] + "\n");

                  recPos += 1;
    			  
	   		  }	          
			  
		  }
		  catch (Exception e) {
	    	  String exceptionMsg = "Exception occurred in FixMonthColumnFuture: " + SupportFuncs.getExceptionError(e);
	    	  SupportFuncs.logMessage(exceptionMsg);
	    	  SupportFuncs.sendEmailMessage(exceptionMsg);	    		        	          	  			  			  
		  }

          return retRptData.toString();
      }      
      
	  private String getTimeStamp() {
		  // Create an instance of SimpleDateFormat used for formatting 
		  // the string representation of date (month/day/year)
		  DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HHmmss");

		  // Get the date today using Calendar object.
		  Date today = Calendar.getInstance().getTime();        
		  // Using DateFormat format method we can create a string 
		  // representation of a date with the defined format.
		  String reportDate = df.format(today);
		  return reportDate;
	  }	
	  
	  public class DfpTransaction {
		  public String campaignId;
		  public long clickCount;
			
		  public DfpTransaction() {
			  campaignId = "";
			  clickCount = 0;
		  }
	  }	  
	  
	  public class DfpCompanyRecord {
		  public String dfpCompanyName;
		  public String sfAccountName;
		  public String sfAccountId;
			
		  public DfpCompanyRecord() {
			  dfpCompanyName = "";
			  sfAccountName = "";
			  sfAccountId = "";
		  }
	  }
	  
	  public class CustomConfiguration extends org.apache.commons.configuration.PropertiesConfiguration{

		
		  
	  }
}


