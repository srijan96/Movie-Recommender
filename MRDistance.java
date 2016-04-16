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

public class MRDistance {

	public static class MRMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			int numUsers = Integer.parseInt(conf.get("users"));
			String[] in = value.toString().split("\\s");
			if(in.length >= 3)
			{
				int cur = Integer.parseInt(in[0]);
				for(int i=1;i<=numUsers;i++)
				{
					String s1 = "";
					String s2 = "";
					String s3 = "";
					if(i<cur)
					{
						s1 = Integer.toString(i);
						s2 = in[0];
						s3 = "A";
					}
					else
					{
						s2 = Integer.toString(i);
						s1 = in[0];
						s3 = "B";
					}
					if(i != cur)	context.write(new Text(s1+","+s2),new Text(s3+","+in[1]+","+in[2]));
				}
			}
		}
	}

	public static class MRReducer extends Reducer<Text,Text,Text,Text> {		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			int numItems = Integer.parseInt(conf.get("items"));
			float distA[] = new float[numItems];
			float distB[] = new float[numItems];
			for(int i=0;i<numItems;i++){	distA[i] = 0;	distB[i] = 0;	}
			for(Text val:values)
			{
				String[] in = val.toString().split(",");
				int index = Integer.parseInt(in[1]);
				float rating = Float.parseFloat(in[2]);
				if(in[0].equals("A"))	distA[index] = rating;
				else if(in[0].equals("B"))	distB[index] = rating;
			}	
			float mA = 0,mB = 0,CP = 0;
			for(int i=0;i<numItems;i++)
			{
				mA += distA[i]*distA[i];
				mB += distB[i]*distB[i];
				CP += distA[i]*distB[i];								
			}
			mA = (float)Math.sqrt(mA);
			mB = (float)Math.sqrt(mB);
			CP = CP /(mA*mB);
			context.write(key,new Text(Float.toString(CP)));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String numUsers = "100";
		String numItems = "100";		
		if(args.length >=4)
		{
			numUsers = args[2];
			numItems = args[3];
		}
		conf.set("users",numUsers);
		conf.set("items",numItems);		
		Job job = Job.getInstance(conf, "movie rate");
		job.setJarByClass(MRDistance.class);
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
