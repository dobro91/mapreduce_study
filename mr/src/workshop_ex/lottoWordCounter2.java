package workshop_ex;

import java.io.IOException;
import java.util.ArrayList;
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

public class lottoWordCounter2 {



	public static class LottoMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> i = new ArrayList<String>();
			String[] header = {"Year", "Round", "Date", "1","2","3","4","5","6","Bonus"};
			Map<String, String> parsed = MRUtils.transformCSVToMap(header, value.toString());
			if(parsed == null)
				return;
			String first = parsed.get("1");
			String second = parsed.get("2");
			String third = parsed.get("3");
			String forth = parsed.get("4");
			String fifth = parsed.get("5");
			String sixth = parsed.get("6");
			String bonus = parsed.get("Bonus");
			
			if(first == null || second == null || third == null ||
					forth == null || fifth == null || sixth == null || bonus == null)
				return;

			i.add(String.valueOf(first));
			i.add(String.valueOf(second));
			i.add(String.valueOf(third));
			i.add(String.valueOf(forth));
			i.add(String.valueOf(fifth));
			i.add(String.valueOf(sixth));
			i.add(String.valueOf(bonus));
			
			for(String k : i){
				word.set(k);
				context.write(word, one);
			}

		}
		
	}
	
	public static class LottoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
		job.setJarByClass(lottoWordCounter2.class);
		job.setMapperClass(LottoMapper.class);
		job.setReducerClass(LottoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
