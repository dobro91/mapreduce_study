package workshopJ_1;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang.StringEscapeUtils;
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

public class WordCountChain {




	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String txt = value.toString();
			
			if(txt == null)
				return;
			
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			txt = txt.replaceAll("'", "");
			txt = txt.replaceAll("[^a-zA-Z]", " ");
			
			StringTokenizer itr = new StringTokenizer(txt);
			while(itr.hasMoreTokens())
				context.write(new Text(itr.nextToken()), new IntWritable(1));
			
		}
		
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

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


	public static class TopTenMapper extends Mapper<Text, IntWritable, NullWritable, Text>{
		

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

	public static class TopTenReducer extends Reducer<NullWritable, Text, Text, IntWritable>
	{
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
		Job job = new Job(conf, "Word Count Chain");

		job.setJarByClass(WordCountChain.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);
		boolean exitCode = job.waitForCompletion(true);

		if(exitCode){
			job = new Job(conf, "Word Count Chain");
			job.setJarByClass(WordCountChain.class);
			job.setMapperClass(TopTenMapper.class);
			job.setReducerClass(TopTenReducer.class);
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
