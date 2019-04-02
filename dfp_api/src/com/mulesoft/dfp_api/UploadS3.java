package com.mulesoft.dfp_api;
import java.io.File;
import java.util.ArrayList;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class UploadS3 {


public void uploadfilesons3()
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
					String key_name="future.csv";
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
					String key_name="supplement.csv";
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
					String key_name="supplement2.csv";
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
					String key_name="yield.csv";
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
					String key_name="inventory.csv";
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
					String key_name="advertisers.csv";
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
					String key_name="line_items.txt";
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
					String key_name="extract.csv";
					System.out.println("uploaded successfully "+s);
					@SuppressWarnings("unused")
					Upload xfer=xfer_mgr.upload(bucket_name, key_name,f);
			//		XferMgrProgress.showTransferProgress(xfer);
					// XferMgrProgress.waitForCompletion(xfer);
				}
			}
		} 
		catch (AmazonServiceException e) 
		{
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}
}