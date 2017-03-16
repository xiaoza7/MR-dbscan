package localcluster;
import java.util.*;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import Utils.*;
public class localReducer extends Reducer<Text,Text,Text,Text>{
	//public  Vector<point>set;
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
		Iterator<Text> ite=values.iterator();
		Vector<point>set=new Vector<point>();
		
		while(ite.hasNext()){
			String line=ite.next().toString();
			point p=new point(line);
			p.partionId=Integer.parseInt(key.toString());
			
			set.add(p);
			
		}
		
		//开始本地聚类
		dbcluster maindb=new dbcluster(set,Integer.parseInt(key.toString()));
		maindb.RunDbScan(200, 28);//参数或许可以修改，reduce 可以读取context获取全局信息,暂且写死。
		String writecontent;
		
		//多类型数据输出同一个文件，以类型区别
		for(int i=0;i<set.size();i++){
			String pointflags = null;
			point p=set.get(i);
			if(p.iscore&&!p.isNoise)
			  {pointflags="core";}
			else if(p.isborders){
				pointflags="border";
				
			}
			//首先是无论什么标志,均写出
			String pclusid=String.valueOf(p.clusterId);
			writecontent=pclusid+" "+pointflags;//poin的clusterid+p.flag;
			context.write(new Text(String.valueOf(p.id)),new Text(writecontent) );//key:p.id, value:poin的clusterid+p.flag;
			//判定inner或者outer,再次写出，为合并各个localcluster做好准备
			if(p.iscore&&p.isInner){
				context.write(new Text(String.valueOf(p.id)),new Text("incore"+" "+writecontent) );	
			}
			else if(!p.isNoise&&p.isOuter){
				context.write(new Text(String.valueOf(p.id)),new Text("bordercore"+" "+writecontent) );	
				
			}
			
			//上面的都写出到同一个文件中去了,长度不同，读取时注意
			
		}
		
	}
	
	

}



