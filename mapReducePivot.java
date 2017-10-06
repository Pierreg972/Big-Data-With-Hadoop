import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class mapReducePivot {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, MapWritable> {

		private Text txt = new Text();

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, MapWritable> output) throws IOException {
		
			int index = 0;
			MapWritable tab = new MapWritable();
			String str = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(str,",");
			while (tokenizer.hasMoreTokens()) {
				txt.set(tokenizer.nextToken());
				tab.put(key,txt);
				output.collect(index, tab);
				index++;
			}
		}
	}

	 public static class Reduce extends MapReduceBase implements Reducer< IntWritable, MapWritable, IntWritable, Text> {

		 
		 public void reduce(IntWritable key, Iterator<MapWritable> values, OutputCollector<IntWritable,Text> output) throws IOException {
			 int count=0;
			 String word= new String();
			 MapWritable tab = new MapWritable();
			 while (values.hasNext()) {
				 for (  Entry<IntWritable,Text> map : values.next().entrySet()) {
					 tab.put(map.getKey(), map.getValue());}
			 }
			 word=word+tab.get(0);
			 for(count=1;count<tab.size;count++){
				 word=word+","+tab.get(count);
			 }
			 output.collect(key, new Text(word));
		 }
	 }
}
