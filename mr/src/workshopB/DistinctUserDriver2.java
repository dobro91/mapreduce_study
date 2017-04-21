package workshopB;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class DistinctUserDriver2 {

	
	private static class SODistinctMapper extends Mapper<Object, Text, Text, NullWritable>{
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
			
			if (parsed == null) {
				return;
			}
	
			
			String userId = parsed.get("UserId");
			if(userId == null) {
				return;
			}

			context.write(new Text(userId), NullWritable.get());
		}
		
		
	}
	
	private static class SODistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
	
			context.write(key, NullWritable.get());
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: TopTenDriver <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Distinct User List");
		job.setJarByClass(DistinctUserDriver2.class);
		job.setMapperClass(SODistinctMapper.class);
		job.setReducerClass(SODistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
