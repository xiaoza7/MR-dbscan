package Partition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class partionReducer extends Reducer<Text,Text,Text,Text>{
	   
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
	    // int sum=0;int min=289343559; int max=0;
		Iterator<Text> ite=values.iterator();
		while(ite.hasNext()){
			
			 //StringTokenizer itr=new StringTokenizer(ite.next().toString());
			 context.write(key, new Text(ite.next()));//简单的写出,partion的id+该行数据
			 /* int id=Integer.parseInt(itr.nextToken());
			  if(id<min) min=id;
			  if(id>max) max=id;
			sum++;*/
		}
		//context.write(key, new Text(String.valueOf(sum)+" "+String.valueOf(min)+" "+String.valueOf(max)));//统计每一个partition的大小,及范围
	}

}
