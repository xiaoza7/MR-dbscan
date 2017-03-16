package relabel;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import Utils.point;

public class relabelReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException{
		Iterator<Text> ite=values.iterator();
		//Vector<point>set=new Vector<point>();
		
		while(ite.hasNext()){
			String line=ite.next().toString();
			//do nothing but write directly
			context.write(key, new Text(line));
		
		   }

}
}
