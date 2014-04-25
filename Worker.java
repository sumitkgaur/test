import java.io.BufferedWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
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
//import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;



public class Worker implements Runnable 
{
  public boolean running = false;
  public static String containerName = "container-view";
  public static BlobStore blobStore;

  //Default params
  public static int fileSize = 5*1024; //  5KB data
  public static int duration = 600;// sec
  public static int maxThreads =1; // max allowed parallel sessions(threads)
  public static int minThreads = 1;  // starting pool of requests, we can keep it same as max for fixed pool
  public static File file = null;

  private long counter = 0;  
  private long BucketCounter = 0;  
  private long Del_BucketCounter=0;
  private long del_counter=0;

  public Worker (long b_count, long count, long d_b_count, long d_count)
  {
    this.counter = count;
    this.BucketCounter = b_count;
    this.del_counter = d_count;
    this.Del_BucketCounter = d_b_count;
    Thread thread = new Thread(this);
    thread.start();
  }
 static BlobStoreContext getSwiftClientView() {
                  // Check if a provider is present ahead of time

               return ContextBuilder.newBuilder("swift-keystone")
                          .credentials("service:swiftte", "cloudlabte")
                          .endpoint("http://10.41.54.19:5000/v2.0/")
                          .buildView(BlobStoreContext.class);
           }

  
  public static void main (String[] args) throws InterruptedException
  {
    List<Worker> Workers = new ArrayList<Worker>();
    
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
                e.printStackTrace();
         }

         BlobStoreContext context = getSwiftClientView();
         try {
                 //Thread.sleep(30000);
                 //Get BlobStore
                 blobStore = context.getBlobStore();

                 //PUT Container
                 for (int i = 0 ; i < 500 ; i++) {
                         blobStore.createContainerInLocation(null, containerName+i);
                         System.out.println("PUT Container for Swift Service -> " + containerName+i);
                 }

         } catch (Exception e ) {
                e.printStackTrace();
         } finally {
                 context.close();
         }

    //Date start = new Date();
    long first_timestamp = System.currentTimeMillis();
    long bucket_num = 0;
    long object_num = 0;
    long d_bucket_num = 0;
    long d_object_num = 0;
    long old_object_num = 0;
    long loop_count = 0;
    long loop_first_timestamp = 0;
    long loop_last_timestamp = 0;
    long loop_difference = 0;
    for (long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(duration); stop>System.nanoTime();) {

    for (int i=0; i<maxThreads; i++)
    {
       if (bucket_num >= 500 )
	bucket_num = 0;

	if ( object_num >= 5000) {
       		if (d_bucket_num >= 500 )
			d_bucket_num = 0;
	}

       Workers.add(new Worker(bucket_num,object_num,d_bucket_num,d_object_num)); 
       object_num++;
       bucket_num++;
	if (object_num >= 5000) {
		d_bucket_num++;
		d_object_num++;
	}
    }
    
    // We must force the main thread to wait for all the Workers
    //  to finish their work before we check to see how long it
    //  took to complete
    for (Worker Worker : Workers)
    {
      while (Worker.running)
      {
        Thread.sleep(20);
      }
    }

    if (loop_count%50 == 0) {
	loop_last_timestamp = System.currentTimeMillis();
	loop_difference = loop_last_timestamp - loop_first_timestamp;
	System.out.println ("Approax TPS is " + (object_num-old_object_num)*1000/loop_difference + "Time taken by 50 loops" + loop_difference);
	loop_first_timestamp = loop_last_timestamp;
	old_object_num = object_num;
    }
    loop_count++;

    }
    long last_timestamp = System.currentTimeMillis();
    //Date end = new Date();
    
    long difference = last_timestamp - first_timestamp;
    
    System.out.println ("This whole process took: " + difference/1000 + " seconds and TPS is " + object_num*1000/difference + " Total Objects PUT " + object_num + "Total Objects  GET and DEL" + d_object_num);
    System.exit(0);
  }
  
  @Override
  public void run() 
  {
    this.running = true;
    String key = "objkey" + counter;
    String del_key = "objkey" + del_counter;
    try 
    {
		@SuppressWarnings("deprecation")
                Blob blob = Worker.blobStore.blobBuilder(key).payload(Worker.file).build();
                Worker.blobStore.putBlob(Worker.containerName+BucketCounter, blob);
		if (counter > 5000) {
		    //System.out.println (" Del_BucketCounter" + Del_BucketCounter + "del_key " + del_key);
                    Worker.blobStore.getBlob(Worker.containerName+Del_BucketCounter, del_key);
                    Worker.blobStore.removeBlob(Worker.containerName+Del_BucketCounter, del_key);
		}

    }
    catch (Exception e) 
    {
	System.out.println ("Error for key" + del_key);
      e.printStackTrace();

    }
    this.running = false;
  }

}
