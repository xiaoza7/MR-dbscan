package relabel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.net.URI;
import java.net.URL;


public class relabelMapper extends Mapper<LongWritable,Text,Text,Text>{
	 HashMap<String,String>hp=new HashMap<String,String>();
	public  void setup(Context context){//initing global val--hashmap;localcluste<------>globalcluster
		
		 Configuration conf = context.getConfiguration();
		    // hardcoded or set it in the jobrunner class and retrieve via this key
		  System.out.println("it is in setup begining");
		// String location = conf.get("job.core.file");// 暂时设置成,later要改
		 String location = "hdfs://192.168.2.240:9000/lmq/globalMr12/part-r-00000";//暂时写死啊，为了实验
		    if (location != null) {
		    	BufferedReader br = null;
		        try {
		            FileSystem fs = FileSystem.get(URI.create(location),conf);//该国
		            Path path = new Path(location);
		            // Will read from a MR output
		           // FileStatus[] fss = fs.listStatus(path);
				   /* for (FileStatus status : fss) {
				        Path new_path = status.getPath();
				        // Ignore _SUCCESS file
				        if (new_path.getName().contains("_SUCCESS")) continue; */
		                FSDataInputStream fis = fs.open(path);
		                br = new BufferedReader(new InputStreamReader(fis));
		                String line = null;
		                while ((line = br.readLine()) != null && line.trim().length() > 0) {
		                	StringTokenizer tokenizer = new StringTokenizer(line);
		                	String globalId = tokenizer.nextToken();
		                	  System.out.println("it is already reading files");
		                	    while(tokenizer.hasMoreTokens())
		                	      {String localId = tokenizer.nextToken();//globalId
		                         // cores.add(c);
		                	      if(!hp.containsKey(localId)){
		                	    	          hp.put(localId, globalId);
		                	                }
		                	            }
		                }
		            }
		        
		        catch (IOException e) {
		            //handle
		        } 
		        finally {
		            //IOUtils.closeQuietly(br);
		            IOUtils.closeStream(br);
		        }
		    }
		    Iterator iter = hp.entrySet().iterator();
			while (iter.hasNext()) {
			  Map.Entry entry = (Map.Entry) iter.next();
			 // System.out.println("is in netx");
			  String key = (String) entry.getKey();  //BE CAUTIOUS,IS IT RIGHT
			//Object val = entry.getValue();
			   String val= (String) entry.getValue();
			 System.out.println(key+" "+val);
			}
		    
	}
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		
		
		String line=value.toString();
		//System.out.println(line); 
		StringTokenizer itr=new StringTokenizer(line);
	 //   String id=itr.nextToken();//获得p.id作为key值
	    ArrayList<String> temp=new ArrayList<String>();
	    while(itr.hasMoreTokens()){
	    	temp.add(itr.nextToken());
	    }
	    //获取
	    if(temp.size()==4){
	    	if(temp.get(1).equals("incore")||temp.get(0).equals("bordercore")){
	    		//System.out.println();  
	    		String sline=hp.get(temp.get(2));// get the localID,be cautious that whether hashmap has the key?
	    		System.out.println(sline);  
	    		context.write(new Text(temp.get(0)),new Text(sline));
	    	}
	    	
	    }else if(temp.size()==3){//分别写入即可
	    	 if(temp.get(2).equals("Noise")){
	    			System.out.println("xhsjh"); 
	    		 context.write(new Text(temp.get(0)),new Text("Noise"));
	    		 
	    	 }
	    	
	    	
	    }
	    	
	    			
	}

}
