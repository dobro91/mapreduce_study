package workshopJ_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountWithoutStopword {

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

	public static class WordSubTxtMapper extends Mapper<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void map(Text key, IntWritable value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, value);
		}

	}

	public static class WordSubMapper extends Mapper<Object, Text, Text, IntWritable>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String txt = value.toString();

			if(txt == null)
				return;

			txt = txt.toLowerCase();
			txt = txt.replaceAll("'|'", "");
			StringTokenizer itr = new StringTokenizer(txt);
			while(itr.hasMoreTokens())
				context.write(new Text(itr.nextToken()), new IntWritable(-1));

		}

	}

//	public static class WordSubReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
//
//		private ArrayList<Text> stopWords = new ArrayList<Text>();
//		private HashMap<Text, Integer> wordList = new HashMap<>();
//		@Override
//		public void reduce(Text key, Iterable<IntWritable> values, Context context)
//				throws IOException, InterruptedException {
//			stopWords.clear();
//			wordList.clear();
//			for(IntWritable value : values)
//			{
//				if(value.get() == -1){
//					stopWords.add(key);
//				}else{
//					wordList.put(key, value.get());
//				}
//			}
//
//			for(Text w : stopWords)
//				wordList.remove(w);
//			
//			for(Entry<Text, Integer> e : wordList.entrySet())
//				context.write(e.getKey(), new IntWritable(e.getValue()));
//		}
//	}
	
	
	public static class WordSubReducer extends Reducer<Text, IntWritable, Text, IntWritable>{


		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int flag = 0;
			
			for(IntWritable a : values){
				if(a.get()==-1) return; // here return;
				flag = a.get();
			}
			
			context.write(key, new IntWritable(flag));
		}
	}
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: WordCountWithoutStopword <in> <stopword> <out>");
			System.exit(2);
		}

		Path tmpout = new Path(otherArgs[2] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[2]);
		Job job = new Job(conf, "Martin Word Count include StopWord");
		job.setJarByClass(WordCountWithoutStopword.class);
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
			job = new Job(conf, "Martin Word Count Without StopWord");
			job.setJarByClass(WordCountWithoutStopword.class);

			MultipleInputs.addInputPath(job, tmpout,
					SequenceFileInputFormat.class, WordSubTxtMapper.class);

			MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, WordSubMapper.class);

			job.setReducerClass(WordSubReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileOutputFormat.setOutputPath(job, finalout);
			exitCode = job.waitForCompletion(true);
		}
		System.exit(exitCode ? 0:1);
	}
}
