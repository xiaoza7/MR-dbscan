package global;
import java.util.*;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Utils.Graph;
public class globalMapper extends Mapper<LongWritable,Text,Text,Text>{  //此maper几乎不做任何处理
	
	/*public void setup(Context context) {//设置全局 图 结构,setup，整个task运行前之初始化一次,初始化
	    Configuration conf = context.getConfiguration();
	    Graph newGraph=new Graph();
	   // conf.set(name, value);
	    
	}*/
	
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		
		
		String line=value.toString();
		//根据标志，有区别的inner和outer,然后根据p.id为key，p.localclusterid为值，写到reducer
		StringTokenizer itr=new StringTokenizer(line);
	    String id=itr.nextToken();//获得p.id作为key值
	    String sline="";
	     //while(itr.hasMoreTokens()){
	    	 // int id=Integer.parseInt(itr.nextToken());//获取数据ID，进行切分
	    	      //int i=0;
	    	sline=itr.nextToken();
	    	//可以判inner,outer
	    	if(sline.equals("incore")){
	   
	    		context.write(new Text(id),new Text(itr.nextToken()) ); // 写到reducer中,仅有p.id和p.clusterId
	    		//System.out.print(line);
	    		
	    	}else if(sline.equals("bordercore")){
	    		context.write(new Text(id),new Text(itr.nextToken()) ); // 写到reducer中
	    		
	    	}
	    	 
		
	}


}
