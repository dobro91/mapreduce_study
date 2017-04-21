package workshop1_3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCounter {

	public static class CountNumUsersByStateMapper extends
	Mapper<Object, Text, Text, IntWritable> {

		public static final String WORD_COUNTER_GROUP = "words";

		private String[] stopWord = new String[] { "a", "is", "am", "have", "are", "was", "were", "has", "had", "an", "i", "he", "she", "it", "they", "there" };
		private final static IntWritable one = new IntWritable(1);
		private Text wordKey = new Text();
		private HashSet<String> stopwords = new HashSet<String>(
				Arrays.asList(stopWord));

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String txt = value.toString();
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
			txt = txt.replaceAll("[^a-zA-Z]", " ");
			String[] tokens = txt.toLowerCase().split("\\s");
			for (String word : tokens) {
				if(!word.isEmpty()){
					if (stopwords.contains(word)) {
						context.getCounter(WORD_COUNTER_GROUP, word)
						.increment(1);
					}else{
						wordKey.set(word);
						context.write(wordKey, one);
					}
				}
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
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: CountNumUsersByState <users> <out>");
			System.exit(2);
		}

		Path input = new Path(otherArgs[0]);
		Path outputDir = new Path(otherArgs[1]);
		
		FileSystem.get(conf).delete(outputDir, true);//false means if already exists dir, don't remove that. true no conditionally.

		Job job = new Job(conf, "Count Num Users By State");
		job.setJarByClass(WordCounter.class);
		job.setMapperClass(CountNumUsersByStateMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(
					CountNumUsersByStateMapper.WORD_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}

		System.exit(code);
	}
}
