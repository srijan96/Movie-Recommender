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

public class MRAdd {

	public static class MRMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String inKey = value.toString().split("\\s")[0];
			String R = value.toString().split("\\s")[1];
			context.write(new Text(inKey),new Text(R));
		}
	}

	public static class MRReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			float rating = 0;
			for(Text val:values)
			{
				rating = rating + Float.parseFloat(val.toString());
			}
			context.write(key,new Text(Float.toString(rating)));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "movie rate");
		job.setJarByClass(MRAdd.class);
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
