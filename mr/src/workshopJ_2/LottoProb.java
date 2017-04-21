package workshopJ_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class LottoProb {

	
	public static class LottoWordMapper extends Mapper<Object, Text, Text, IntWritable>
	{
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
				context.getCounter("total", "word").increment(1);
				word.set(k);
				context.write(word, one);
			}

		}
		
	}
	
	public static class LottoWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int sum = 0;
			
			for(IntWritable val : values)
				sum += val.get();
			
			context.write(key, new IntWritable(sum));
		}

	}
	
	public static class LottoProbMapper extends Mapper<Text, IntWritable, Text, Text>{

		@Override
		protected void map(Text key, IntWritable value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double prob = value.get() / Float.parseFloat(context.getConfiguration().get("sum"));
			context.write(key, new Text(value.toString() + "\t" + prob));
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueUserCount <in> <out>");
			System.exit(2);
		}

		Path tmpout = new Path(otherArgs[1] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[1]);
		Job job = new Job(conf, "Lotto word count");
		job.setJarByClass(LottoProb.class);
		job.setMapperClass(LottoWordMapper.class);
		job.setReducerClass(LottoWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);		
		boolean exitCode = job.waitForCompletion(true);
		Counter counter = job.getCounters().findCounter("total", "word");
		if(exitCode)
		{
			job = new Job(conf, "Lotto Probability");
			job.getConfiguration().set("sum", String.valueOf(counter.getValue()));
			job.setJarByClass(LottoProb.class);
			job.setMapperClass(LottoProbMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, tmpout);
			FileOutputFormat.setOutputPath(job, finalout);
			exitCode = job.waitForCompletion(true);
		}
		
		System.exit(exitCode ? 0:1);
	}
}
