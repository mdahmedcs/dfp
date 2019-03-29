package com.mulesoft.dfp_api;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.sun.mail.smtp.SMTPTransport;

public class SupportFuncs {
	public static void logMessage(String msg)
	{
		System.out.println("mule-app-dfp_api|" + getTimeStamp() + "|" + msg);
	}
	
	public static String getExceptionError(Exception e)
	{
		String errorString = "";
		
		if (e.getMessage() != null) 
		{
			errorString = e.getMessage() + " " + e.toString();
		}
		else
		{
			errorString = e.toString();
		}
		return errorString;
	}	
	
	public static Timestamp getTimeStamp() {
		java.util.Date ex_date= new java.util.Date();
		return new Timestamp(ex_date.getTime());
	}

	public static void sendEmailMessageWithAttachment(String message, String fileName)
	{
		try {
			Properties app_prop = getProperties();

	        Properties props = System.getProperties();
	        props.put("mail.transport.protocol", "smtp");
	        
	        props.put("mail.smtp.host", app_prop.getProperty("smtp_host"));
	        props.put("mail.smtps.auth","true");
	        Session session = Session.getInstance(props, null);
	        Message msg = new MimeMessage(session);
	        MimeBodyPart messageBodyPart = new MimeBodyPart();
	        DataSource source = new FileDataSource(fileName);
	        messageBodyPart.setDataHandler(new DataHandler(source));
	        messageBodyPart.setFileName(fileName);
	        Multipart multipart = new MimeMultipart();
	        multipart.addBodyPart(messageBodyPart);
	        msg.setFrom(new InternetAddress(app_prop.getProperty("email_from")));;
	        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(app_prop.getProperty("email_to"), false));
	        msg.setSubject(app_prop.getProperty("email_subject"));
	        msg.setText(message);
	        msg.setContent(multipart);
	        msg.setHeader("X-Mailer", "Email alert from Mulesoft ESB Instance");
	        msg.setSentDate(new Date());
	        SMTPTransport t = (SMTPTransport)session.getTransport("smtp");
	        t.connect(app_prop.getProperty("smtp_host"), Integer.parseInt(app_prop.getProperty("smtp_port")), "", "");
	        t.sendMessage(msg, msg.getAllRecipients());
	        logMessage("Response from email server: " + t.getLastServerResponse());
	        t.close();;
		}
		catch (Exception e)
		{
			logMessage("Exception occurred sending email: " + e.getMessage());
		}       	
	}
	
	public static void sendEmailMessage(String message)
	{
		try {
			Properties app_prop = getProperties();

	        Properties props = System.getProperties();
	        props.put("mail.transport.protocol", "smtp");
	        
	        props.put("mail.smtp.host", app_prop.getProperty("smtp_host"));
	        props.put("mail.smtps.auth","true");
	        Session session = Session.getInstance(props, null);
	        Message msg = new MimeMessage(session);
	        msg.setFrom(new InternetAddress(app_prop.getProperty("email_from")));;
	        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(app_prop.getProperty("email_to"), false));
	        msg.setSubject(app_prop.getProperty("email_subject"));
	        msg.setText(message);
	        msg.setHeader("X-Mailer", "Email alert from Mulesoft ESB Instance");
	        msg.setSentDate(new Date());
	        SMTPTransport t = (SMTPTransport)session.getTransport("smtp");
	        t.connect(app_prop.getProperty("smtp_host"), Integer.parseInt(app_prop.getProperty("smtp_port")), "", "");
	        t.sendMessage(msg, msg.getAllRecipients());
	        logMessage("Response from email server: " + t.getLastServerResponse());
	        t.close();;
		}
		catch (Exception e)
		{
			logMessage("Exception occurred sending email: " + e.getMessage());
		}       	
	}

	public static Properties getProperties() 
	{
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("/opt/mule/conf/dfp_api_config-override.properties"));
		} catch (Exception e) {
			String exceptionMsg = "Exception occurred in getProperties: " + getExceptionError(e);
			logMessage(exceptionMsg);
			sendEmailMessage(exceptionMsg);	    		
		}
		return prop;
	}
	
	public static String getStringFromInputStream(InputStream is) {
		 
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
 
		String line;
		try {
 
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
 
		} catch (IOException e) {
			String exceptionMsg = "Exception occurred in getStringFromInputStream: " + getExceptionError(e);
			logMessage(exceptionMsg);
			sendEmailMessage(exceptionMsg);	    		
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					String exceptionMsg = "Exception occurred in getStringFromInputStream: " + getExceptionError(e);
					logMessage(exceptionMsg);
					sendEmailMessage(exceptionMsg);	    		
				}
			}
		}
 
		return sb.toString();
 
	}	
}
