package localcluster;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class localMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		String line=value.toString();//获取数据块ID
	    // System.out.println("is in map");
	    StringTokenizer itr=new StringTokenizer(line);
	    String id=itr.nextToken();
	    String sline="";
	     while(itr.hasMoreTokens()){
	    	 // int id=Integer.parseInt(itr.nextToken());//获取数据ID，进行切分
	    	      //int i=0;
	    	 sline=sline+" "+itr.nextToken();
	    	 
	     }
	    	  System.out.println(sline);  
	    	     context.write(new Text(id), new Text(sline));//按partion号集合数据,包含inner,outer rectangle;
	    	       
	     	
		
	}
	
	

}
