package workshop_ex;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class WifiLocCounter {



	public static class LocMapper extends Mapper<Object, Text, Text, IntWritable>{


		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] header = {"Loc","Loc_Detail","Province,City",
					"Facility","Provider","WiFi_SSID",
					"Install_date","Street_Addr","Loc_Addr",
					"Manage_Org","tel","Lat","Lon","CreationDate"};
			Map<String, String> parsed = MRUtils.transformCSVToMap(header, value.toString());
			if(parsed == null)
				return;
			String Loc = parsed.get("Loc");
			if(Loc == null)
				return;
			word.set(Loc);
			context.write(word, one);
		}

	}


	public static class LocReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Binning <posts> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "Binning");
		job.setJarByClass(WifiLocCounter.class);
		job.setMapperClass(LocMapper.class);
		job.setReducerClass(LocReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
