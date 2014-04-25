import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.io.BufferedWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.BlobMetadata;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.contains;
import static org.jclouds.Constants.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.openstack.swift.SwiftKeystoneClient;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.s3.S3Client;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

class RejectedExecutionHandlerImpl1 implements RejectedExecutionHandler { 
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    	//only for debugging
     }  
 } 

class MyMonitorThread1 implements Runnable  
{
	  private ThreadPoolExecutor executor;
	 
	  MyMonitorThread1(ThreadPoolExecutor executor) {
		this.executor = executor;
	  }
	 
	  @Override
	  public void run() {
		  long first_timestamp = 0;
		  long last_timestamp = 0;
		  long total_req_served = 0;
		  long last_req_served = 0;
		  while(true) {    
			  try {
                    Thread.sleep(60000);  // Take performance check every 30sec
                    total_req_served = this.executor.getCompletedTaskCount();
                    first_timestamp = System.currentTimeMillis();
                    if (total_req_served-last_req_served > 0) {
                    	System.out.println("TPS " +((total_req_served-last_req_served)*1000)/(first_timestamp-last_timestamp) + "  Avg Timetaken per request" + (first_timestamp-last_timestamp)/(total_req_served-last_req_served) + " ms" + "CurrentPoolSize: " + this.executor.getPoolSize() +  "Task Active: " +  this.executor.getActiveCount());
                    }
                    last_req_served = this.executor.getCompletedTaskCount();
                	last_timestamp = System.currentTimeMillis();
			  } catch (Exception e) {
              }  
		  }
	  }
}

class MyRunnable1 implements Runnable {
	  private final long count;
	  private long count1;
	  private long count2;
	  
	  MyRunnable1(long count) {
	    this.count = count;
	    this.count1 = count+1;
	    this.count2 = count+2;
	  }

	  @Override
	  public void run() {
	    String key = "objkey" + UUID.randomUUID();;
	    
        try {
		@SuppressWarnings("deprecation")
   		Blob blob = Example8.blobStore.blobBuilder(key).payload(Example8.file).build();
   		Example8.blobStore.putBlob(Example8.containerName+count, blob);
        	Blob recv_object = Example8.blobStore.getBlob(Example8.containerName+count, key);
        	Example8.blobStore.removeBlob(Example8.containerName+count, key);
        } catch (Exception ace) {
        	System.out.println("Request failed for objkey " + key + "    " + ace);
		ace.printStackTrace(System.out);
        }
	} 
}

public class Example8 {
  public static String containerName = "container-view";
  public static String accessKey = "ed2c3cc529";
  public static String secretKey = "5183";
  public static BlobStore blobStore;
  
  //Default params 
  public static int fileSize = 5*1024; //  5KB data
  public static int duration = 300;// sec
  public static int maxThreads =10; // max allowed parallel sessions(threads)
  public static int minThreads = 10;  // starting pool of requests, we can keep it same as max for fixed pool
  public static File file = null;
	
 public static final Map<String, ApiMetadata> allApis = Maps.uniqueIndex(Apis.viewableAs(BlobStoreContext.class),
      Apis.idFunction());
 public static final Map<String, ProviderMetadata> appProviders = Maps.uniqueIndex(Providers.viewableAs(BlobStoreContext.class),
      Providers.idFunction());   
 public static final Set<String> allKeys = ImmutableSet.copyOf(Iterables.concat(appProviders.keySet(), allApis.keySet()));
 
 /*
  * Amazon S3 client view
  */
 static BlobStoreContext getS3ClientView() {
		  // Check if a provider is present ahead of time
		  checkArgument(contains(allKeys, "s3"), "provider %s not in supported list: %s", "s3", allKeys);
		  
		  // Proxy check
		  Properties overrides = new Properties(); 
		  overrides.setProperty(PROPERTY_PROXY_HOST, "70.10.15.10"); 
		  overrides.setProperty(PROPERTY_PROXY_PORT, "8080");
		  overrides.setProperty(PROPERTY_TRUST_ALL_CERTS, "true"); 
		  overrides.setProperty(PROPERTY_RELAX_HOSTNAME, "true"); 
		  
	      
	       return ContextBuilder.newBuilder("s3")
                .credentials("AKIAIRZDKFYJDM5T2A", "LaNmauVOJUj9v9UrVHcKZp")
                .endpoint("http://s3.amazonaws.com")
                .overrides(overrides)
                //.buildView(S3BlobStoreContext.class);
                .buildView(BlobStoreContext.class);
 }
 
 /*
  * SWIFT client view
  */
 
 static BlobStoreContext getSwiftClientView() {
		  // Check if a provider is present ahead of time
		  checkArgument(contains(allKeys, "swift-keystone"), "provider %s not in supported list: %s", "swift-keystone", allKeys);
		  
	       return ContextBuilder.newBuilder("swift-keystone")
	                  .credentials("test:tester", "test123")
	                  .endpoint("http://10.41.54.19:5000/v2.0/")
	                  .buildView(BlobStoreContext.class);
	   }
 /*
  * Amazon S3 client api
  */
 static S3Client getS3Client() {
		  // Check if a provider is present ahead of time
		  checkArgument(contains(allKeys, "s3"), "provider %s not in supported list: %s", "s3", allKeys);
		  
		  // Proxy check
		  Properties overrides = new Properties(); 
		  overrides.setProperty(PROPERTY_PROXY_HOST, "70.10.15.10"); 
		  overrides.setProperty(PROPERTY_PROXY_PORT, "8080");
		  overrides.setProperty(PROPERTY_TRUST_ALL_CERTS, "true"); 
		  overrides.setProperty(PROPERTY_RELAX_HOSTNAME, "true");
		  
		  
	      return ContextBuilder.newBuilder("s3")
	                           .endpoint("http://s3.amazonaws.com")
	    		  			   .overrides(overrides)
	                           .buildApi(S3Client.class);
	   }
 /*
  * SWIFT client api
  */
 
 static SwiftKeystoneClient getSwiftClient() {
		  // Check if a provider is present ahead of time
		  checkArgument(contains(allKeys, "swift-keystone"), "provider %s not in supported list: %s", "swift-keystone", allKeys);
		  
	      return ContextBuilder.newBuilder("swift-keystone")
	                           .credentials("admin:admin", "admin")
	                           .endpoint("http://70.5.51.42:5000/v2.0/")
	                           .buildApi(SwiftKeystoneClient.class);
	   }

  
  /*
   * Main function 
   */
 public static void main(String[] args) {
	 if(args.length > 0) {
		 if (args.length < 5) {
			System.out.println("Need 5 args in sequence swiftBucketName fileSize maxThreads minThreads (<=maxThreds) duration(in sec)");
		 	System.exit(0);
		 }
		 containerName = args[0];
		 fileSize = Integer.parseInt(args[1]);
		 maxThreads = Integer.parseInt(args[2]);
		 minThreads = Integer.parseInt(args[3]);
		 duration = Integer.parseInt(args[4]);
	 }   
	    
	 // Sample file
	 try {
		 file = File.createTempFile("jcloud-s3-test", ".txt");
		 BufferedWriter writer = null;
		 try {
			 writer = new BufferedWriter(new FileWriter(file));
			 for (int i = 0; i < fileSize; i++) {
				 writer.write("0");
			 }
		 } finally {
			 if (writer != null) try { writer.close(); } catch (IOException ignore) {}
		 }
	 } catch (Exception e) {
	 }

	 /*
	  * Common operations
	  */
	 BlobStoreContext context = getSwiftClientView();
	 //BlobStoreContext context = getS3ClientView();

	 /*
	  * Create Basic Containers
	  */
	 try {
		 //Thread.sleep(30000); 
		 //Get BlobStore
		 blobStore = context.getBlobStore();

		 //PUT Container
		 for (int i = 0 ; i <= 102 ; i++) {
			 blobStore.createContainerInLocation(null, containerName+i);
			 System.out.println("PUT Container for S3/Swift Service -> " + containerName+i);
		 }

	 } catch (Exception e ) {
		e.printStackTrace();
	 } finally {
		 context.close();
	 }
	      
	System.out.println("Setup completed. Test started with "+ maxThreads +" threads pool and payload " + fileSize +" for "+ duration +"secs.....");
	
        RejectedExecutionHandlerImpl1 rejectionHandler = new RejectedExecutionHandlerImpl1(); 
	ThreadFactory threadFactory = Executors.defaultThreadFactory(); 
	ThreadPoolExecutor executorPool = new ThreadPoolExecutor(minThreads, maxThreads, 30, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), threadFactory, rejectionHandler);
	MyMonitorThread1 monitor = new MyMonitorThread1(executorPool);
	Thread monitorThread = new Thread(monitor); 
	monitorThread.start();
	
	/*
	 * Starting Test execution
	 */
	int i = 0;
    long first_timestamp = System.currentTimeMillis();
    for (long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(duration); stop>System.nanoTime();) {
    	if (i > 99) {
    		i = 0;
    	}
    	try {
			Runnable worker = new MyRunnable1(i);
			executorPool.execute(worker);
			i++;
    	} catch (Exception e){
        	System.out.println("Executor failed " + e );
	}
    }
    
    System.out.println("Test iteration finished. Waiting for threads to finish.....");
    executorPool.shutdown(); 
    
    // Wait until all threads are finish
    try {
    	executorPool.awaitTermination(5,TimeUnit.MINUTES);
	} catch (Exception e) {
	}
	long last_timestamp = System.currentTimeMillis();
	
	try {
		System.out.println(":: Test Output ::");
		System.out.println("Total requests serverd " + executorPool.getCompletedTaskCount() + " in " +  (last_timestamp-first_timestamp) + " ms. Average TPS = " + executorPool.getCompletedTaskCount()/((last_timestamp-first_timestamp)/1000) + " Average Time taken per req " +  (last_timestamp-first_timestamp)/(executorPool.getCompletedTaskCount()) + " ms ");
		Thread.sleep(5000); //for last few results from monitor
		System.exit(0);
	} catch (Exception e) {
	}
		
  }
}
