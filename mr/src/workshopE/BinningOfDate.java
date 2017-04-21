package workshopE;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class BinningOfDate {

	public static class BinningMapper extends
	Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");
		@Override
		protected void setup(Context context) {
			// Create a new MultipleOutputs using the context object
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRUtils.transformXmlToMap(value
					.toString());

			String rawtags = parsed.get("CreationDate");
			if (rawtags == null) {
				return;
			}

			// Tags are delimited by ><. i.e. <tag1><tag2><tag3>
			
			// For each tag
			// Remove any > or < from the token
			try {
				Date date = frmt.parse(rawtags);
				// If this tag is one of the following, write to the named bin
				mos.write("bins", value, NullWritable.get(), String.valueOf(date.getYear() + 1900));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

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
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Binning <posts> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "Binning");
		job.setJarByClass(BinningOfDate.class);
		job.setMapperClass(BinningMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Configure the MultipleOutputs by adding an output called "bins"
		// With the proper output format and mapper key/value pairs
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, NullWritable.class);

		// Enable the counters for the job
		// If there is a significant number of different named outputs, this
		// should be disabled
		MultipleOutputs.setCountersEnabled(job, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}
}
