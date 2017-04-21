package workshopJ_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;


public class UsersByState {

	public static class userJoinMapper extends Mapper<Object, Text, Text, Text>
	{

		private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
				"CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
				"IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
				"MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
				"OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
				"VT", "VA", "WA", "WV", "WI", "WY" };

		private HashSet<String> states = new HashSet<String>(
				Arrays.asList(statesArray));


		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());

			String userId = parsed.get("Id");
			String location = parsed.get("Location");

			if(userId == null)
				return;


			if (location != null && !location.isEmpty()) {
				boolean unknown = true;
				String[] tokens = location.toUpperCase().split("\\s");
				for (String state : tokens) {
					if (states.contains(state)) {
						context.write(new Text(userId), new Text("A" + state));
						unknown = false;
						break;
					}
				}
				if (unknown) {
					context.write(new Text(userId), new Text("A" + "unknown"));
				}
			} else {
				context.write(new Text(userId), new Text("A" + "NullOrEmpty"));
			}			
		}

	}

	public static class commentJoinMapper extends Mapper<Object, Text, Text, Text>
	{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
			String userId = parsed.get("UserId");
			if (userId == null)
				return;
			
			context.write(new Text(userId), new Text("B" + value.toString()));
			
		}

	}

	public static class joinReducer extends Reducer<Text, Text, Text, Text>
	{

		ArrayList<String> A = new ArrayList<String>();
		ArrayList<String> B = new ArrayList<String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			A.clear();
			B.clear();
			
			for(Text val : values)
				if(val.charAt(0) == 'A')
					A.add(val.toString().substring(1));
				else
					B.add(val.toString().substring(1));
			
			if(!A.isEmpty() && !B.isEmpty())
				for(String state : A)
					for(String comment : B)
						context.write(new Text(state), new Text(comment));
		}

	}
	
	
	public static class WordCountByStateMapper extends Mapper<Text, Text, Text, IntWritable>
	{

		@Override
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
			
			String txt = parsed.get("Text");
			if(txt == null)
				return;
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			txt = txt.replaceAll("'", "");
			txt = txt.replaceAll("[^a-zA-Z]", " ");
			
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				context.write(new Text(key + " " + itr.nextToken()), new IntWritable(1));
			}			
		}
		
	}
	
	public static class WordCountByStateReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		private MultipleOutputs<Text, IntWritable> mos = null;

		@Override
		protected void setup(Context context) {
			// Create a new MultipleOutputs using the context object
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int sum = 0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			
			String[] token = key.toString().split(" ");
			mos.write("state", new Text(token[1]), new IntWritable(sum), token[0]);
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Close multiple outputs!
			mos.close();
		}
		
	}	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3)
		{
			System.out.println("please input <usr> <cmt> <out>");
			System.exit(2);
		}
		Path tmpout = new Path(otherArgs[2] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[2]);
		
		Job job = new Job(conf, "getting State");
		job.setJarByClass(UsersByState.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, userJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, commentJoinMapper.class);
		job.setReducerClass(joinReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);
		boolean exitCode = job.waitForCompletion(true);
		
		if(exitCode){
			job = new Job(conf, "Word Count by States");
			job.setJarByClass(UsersByState.class);
			job.setMapperClass(WordCountByStateMapper.class);
			job.setReducerClass(WordCountByStateReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, tmpout);
			FileOutputFormat.setOutputPath(job, finalout);
			MultipleOutputs.addNamedOutput(job, "state", TextOutputFormat.class,
					Text.class, IntWritable.class);
			//MultipleOutputs.setCountersEnabled(job, true);
			exitCode = job.waitForCompletion(true);
		}
		System.exit(exitCode ? 0 : 1);


	}
}
