package workshop1_3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCounter2 {

	public static class CountNumUsersByStateMapper extends
	Mapper<Object, Text, NullWritable, NullWritable> {

		public static final String WORD_COUNTER_GROUP = "words";

		private String[] stopWord = new String[] { "a", "is", "am", "have" };

		private HashSet<String> stopwords = new HashSet<String>(
				Arrays.asList(stopWord));

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String txt = value.toString();
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
			txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a
			String[] tokens = txt.toLowerCase().split("\\s");
			for (String word : tokens) {
				if (stopwords.contains(word)) {
					context.getCounter(WORD_COUNTER_GROUP, word)
					.increment(1);
				}
			}

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

		Job job = new Job(conf, "Count Num Users By State");
		job.setJarByClass(WordCounter.class);
		job.setMapperClass(CountNumUsersByStateMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
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

		FileSystem.get(conf).delete(outputDir, true);

		System.exit(code);
	}
}
