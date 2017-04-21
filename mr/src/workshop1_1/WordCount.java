package workshop1_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class SOWordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> { // input : key(Object) , value(Text) -> output : key(Text), value(IntWrit.

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map

			// Grab the "Text" field, since that is what we are counting over
			String txt = value.toString();
			
			// .get will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}

			// Unescape the HTML because the SO data is escaped.
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

			// Remove some annoying punctuation
			txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
			txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a
													// space

			// Tokenize the string, then send the tokens away
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); //configuration initialize
		String[] otherArgs = new GenericOptionsParser(conf, args) //argument passing
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CommentWordCount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Comment Word Count"); //job design.
		job.setJarByClass(WordCount.class); //main class define
		job.setMapperClass(SOWordCountMapper.class); //SOWordCountMapper class.
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class); //IntSum
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

