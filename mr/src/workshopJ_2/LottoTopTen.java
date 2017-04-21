package workshopJ_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class LottoTopTen {

	
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
				word.set(k);
				context.write(word, one);
			}

		}
	}
	
	public static class LottoWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
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
	
	public static class LottoTopMapper extends Mapper<Text, IntWritable, NullWritable, Text>{
		TreeMap<Integer, Text> TopTenRecord = new TreeMap<>();
		
		@Override
		protected void map(Text key, IntWritable value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			
			TopTenRecord.put(value.get(), new Text(key));
			
			if(TopTenRecord.size() > 10)
				TopTenRecord.remove(TopTenRecord.firstKey());
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			for(Entry<Integer, Text> t : TopTenRecord.entrySet())
				context.write(NullWritable.get(), new Text(t.getKey().toString() + "\t" + t.getValue().toString()));
		}
	}
	
	public static class LottoTopReducer extends Reducer<NullWritable, Text, Text, IntWritable>{
		TreeMap<Integer, Text> TopTenRecord = new TreeMap<>();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value : values){
				String[] str = value.toString().split("\t");
				TopTenRecord.put(Integer.parseInt(str[0]), new Text(str[1]));
			}
			
			if(TopTenRecord.size() > 10)
				TopTenRecord.remove(TopTenRecord.firstKey());
			
			for(Entry<Integer, Text> t : TopTenRecord.descendingMap().entrySet())
				context.write(new Text(t.getValue()), new IntWritable(t.getKey()));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UniqueUserCount <in> <out>");
			System.exit(2);
		}
		
		Path tmpout = new Path(otherArgs[1] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[1]);
		
		Job job = new Job(conf, "Lotto Word Counter");
		job.setMapperClass(LottoWordMapper.class);
		job.setCombinerClass(LottoWordReducer.class);
		job.setReducerClass(LottoWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);

		boolean exitCode = job.waitForCompletion(true);

		if(exitCode){
			job = new Job(conf, "Lotto Word Top Ten");
			job.setMapperClass(LottoTopMapper.class);
			job.setReducerClass(LottoTopReducer.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, tmpout);
			FileOutputFormat.setOutputPath(job, finalout);
			exitCode = job.waitForCompletion(true);
		}
		System.exit(exitCode ? 0 : 1);
	}
}
