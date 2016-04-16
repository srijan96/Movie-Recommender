import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRNormalize {

	public static class MRMapper extends Mapper<Object, Text, Text, Text>{
		private Text keyOut = new Text();
		private Text valOut = new Text();		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] in = value.toString().split(",");
			if(in.length >= 3)
			{
				keyOut.set(in[0]);
				valOut.set(in[1]+","+in[2]);
				context.write(keyOut,valOut);
			}
		}
	}

	public static class MRReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();	
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			ArrayList<Text> cache = new ArrayList<Text>();
			float mean = 0;
			int count = 0;	
			for(Text val:values)	
			{
				mean = mean + Float.parseFloat(val.toString().split(",")[1]);
				count = count + 1;
				cache.add(new Text(val.getBytes()));
			}
			mean = mean / count;
			for(Text val:cache)
			{
				String[] arr = val.toString().split(",");
				String v = arr[1];
				String[] A = v.split("\\.");
				v = A[0]+"."+A[1];
				String norm = Float.toString(Float.parseFloat(v) - mean);
				result.set(arr[0]+"\t"+norm);
				context.write(key,result);
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "movie rate");
		job.setJarByClass(MRNormalize.class);
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
