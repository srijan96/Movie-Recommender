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

public class MRContrib {

	public static class MRMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			int numUsers = Integer.parseInt(conf.get("users"));
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			if(fileName.equals("out.csv"))
			{
				String[] in = value.toString().split("\\s");
				if(in.length >= 2 && in[0].split(",").length >= 2)
				{
					String[] k = in[0].split(",");
					context.write(new Text(in[0]) , new Text(in[1]));
					context.write(new Text(k[1]+","+k[0]) , new Text(in[1]));					
				}
			}
			else if(fileName.equals("ratings.csv"))
			{
				String[] IN = value.toString().split(",");
				if(IN.length >= 3)
				{
					int cur = Integer.parseInt(IN[0]);
					for(int i=1;i<= numUsers;i++)
					{
						if(i != cur)
							context.write(new Text(Integer.toString(i)+","+IN[0]),new Text(IN[1]+","+IN[2]));
					}
				}
			}
		}
	}

	public static class MRReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			ArrayList<Text> cache = new ArrayList<Text>();
			float D = 0;
			for(Text val:values)	
			{
				if(val.toString().split(",").length == 1)
					D = Float.parseFloat(val.toString());
				else if(val.toString().split(",").length >= 2)
					cache.add(new Text(val.getBytes()));
			}
			for(Text val:cache)
			{
				String[] arr = val.toString().split(",");
				String[] k = key.toString().split(",");	
				String v = arr[1];
				String[] A = v.split("\\.");
				v = A[0]+"."+A[1];	
				if(v.contains("-"))	v = "0.0";
				String eval = Float.toString(Float.parseFloat(v) * D);
				context.write(new Text(k[0]+","+arr[0]),new Text(eval));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String numUsers = "100";	
		if(args.length >=3)
		{
			numUsers = args[2];
		}
		conf.set("users",numUsers);
		Job job = Job.getInstance(conf, "movie rate");
		job.setJarByClass(MRContrib.class);
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
