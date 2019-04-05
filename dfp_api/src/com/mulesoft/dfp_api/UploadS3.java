package com.mulesoft.dfp_api;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;


import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class UploadS3 {


public void cmd_upload_files() throws InterruptedException
	{
		File directory = new File("/opt/mule/file_upload/dfp/s3");
		ArrayList<String> filepath = new ArrayList<String>();  //store file paths
		try 
		{
			File[] listfile = directory.listFiles();
			for (File file : listfile)
			{
				if (file.isFile()) 
				{
					filepath.add(file.getPath());
					
				}
			}
			System.out.println(filepath);
			for (String s: filepath)
			{
				if(s.contains("dfpextractfuture"))
				{
					File f = new File(s);    //although string s contains file path, xfer_mgr does not take string argument instead take file argument. Therefore creating File and passing string s. both s and f contain file path
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/future";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
				//	XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("dfpextractsupplement_"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/supplement";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("dfpextractsupplement2_"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/supplement2";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("dfpyield"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/yield";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			        //XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("dfpinventory"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/inventory";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("advertiser_export"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/advertisers";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("line_item"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/line-items";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
				else if(s.contains("dfpextract_"))
				{
					File f = new File(s);    
					TransferManager xfer_mgr = TransferManagerBuilder.standard().build();
					String bucket_name="abm-dw/data-import/dfp/extract";
					String key_name=f.getName();
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
			
			
			}
		
		
				Thread.sleep(1000);
		
		} 
		catch (AmazonServiceException e) 
		{
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
		
		public String cmd_archive_files()
		{
			
			TimestampManager tm = new TimestampManager();
			File [] files = new File("/opt/mule/file_upload/dfp/s3").listFiles();
		
			String sb = "";
			for(File file: files)
			{	
			
			String file_name="/opt/mule/file_upload/dfp/s3/"+file.getName()+" ";
			sb=sb+file_name;
		}
			return "sudo zip /opt/mule/file_upload/dfp/archive/archive-dfp-"+tm.getTimeStamp().substring(0, 10)+".zip "+sb;
		}
		
		public String cmd_remove_files()
		{
			
			
			File [] files = new File("/opt/mule/file_upload/dfp/s3").listFiles();
		
			String sb = "";
			for(File file: files)
			{	
			
			String file_name="/opt/mule/file_upload/dfp/s3/"+file.getName()+" ";
			sb=sb+file_name;
		}
			return "sudo rm "+sb;
		}
		
}
	    	  
			
			
	
