package workshopK_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;


public class MinMaxAndWordCount {

	public static final String MULTIPLE_OUTPUTS_MINMAX = "minmax";
	public static final String MULTIPLE_OUTPUTS_WORDCOUNT = "wordcount";

	public static class MWmapper extends Mapper<Object, Text, Text, MWTuple>{
		private Text outUserId = new Text();
		private MWTuple outTuple = new MWTuple();

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Mmap(key, value, context);
			Wmap(key, value, context);
		}

		protected void Mmap(Object key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
			String strDate = parsed.get("CreationDate");
			String userId = parsed.get("UserId");
			if (strDate == null || userId == null) {
				return;
			}

			try {
				Date creationDate = frmt.parse(strDate);

				outTuple.setMin(creationDate);
				outTuple.setMax(creationDate);

				outTuple.setCount(1);

				outUserId.set("A" + userId);

				context.write(outUserId, outTuple);
			} catch (ParseException e) {

			}
		}

		protected void Wmap(Object key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MRUtils.transformXmlToMap(value
					.toString());
			String txt = parsed.get("Text");
			if (txt == null) {
				return;
			}
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
			txt = txt.replaceAll("'", "");
			txt = txt.replaceAll("[^a-zA-Z]", " ");
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				outTuple.setCount(1);
				context.write(new Text(itr.nextToken()), outTuple);
			}
		}
	}

	public static class MWreducer extends Reducer<Text, MWTuple, Text, Text>{

		private MWTuple result = new MWTuple();
		private MultipleOutputs<Text, Text> mos = null;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<MWTuple> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			if (key.charAt(0) == 'A') {
				MReduce(new Text(key.toString().substring(1)), values, context);
			} else {
				WReduce(key, values, context);
			}
		}

		protected void MReduce(Text key, Iterable<MWTuple> values,
				Context context) throws IOException, InterruptedException{
			
			result.setMin(null);
			result.setMax(null);
			result.setCount(0);
			int sum = 0;
			
			for (MWTuple val : values) {

				if (result.getMin() == null
						|| val.getMin().compareTo(result.getMin()) < 0) {
					result.setMin(val.getMin());
				}


				if (result.getMax() == null
						|| val.getMax().compareTo(result.getMax()) > 0) {
					result.setMax(val.getMax());
				}

				sum += val.getCount();
			}

			result.setCount(sum);
			mos.write(MULTIPLE_OUTPUTS_MINMAX, key, new Text(result.toString()),
					MULTIPLE_OUTPUTS_MINMAX + "/part");
		}

		protected void WReduce(Text key, Iterable<MWTuple> values,
				Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (MWTuple val : values) {
				sum += val.getCount();
			}

			mos.write(MULTIPLE_OUTPUTS_WORDCOUNT, key, new Text(String.valueOf(sum)),
					MULTIPLE_OUTPUTS_WORDCOUNT + "_tmp/part");

		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}
	
	public static class WordSubTxtMapper extends Mapper<Text, Text, Text, IntWritable>{

		@Override
		protected void map(Text key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));
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
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: MergedJob <in> <stopword> <out>");
			System.exit(1);
		}
		Path tmpout = new Path(otherArgs[2] + "/" + MULTIPLE_OUTPUTS_WORDCOUNT + "_tmp");
		Path finalout = new Path(otherArgs[2] + "/" + MULTIPLE_OUTPUTS_WORDCOUNT);
		Job job = new Job(conf, "Min Max And Word Counter");
		job.setJarByClass(MinMaxAndWordCount.class);
		job.setMapperClass(MWmapper.class);
		job.setReducerClass(MWreducer.class);

		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_MINMAX,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_WORDCOUNT,
				SequenceFileOutputFormat.class, Text.class, Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MWTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean exitCode = job.waitForCompletion(true);
		
		if(exitCode){
			job = new Job(conf, "Martin Word Count Without StopWord");
			job.setJarByClass(MinMaxAndWordCount.class);

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

	public static class MWTuple implements Writable{

		private Date min = new Date();
		private Date max = new Date();
		private long count = 0;



		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		public MWTuple() {
			// TODO Auto-generated constructor stub
		}

		public MWTuple(String tag, Date date){
			setMax(date);
			setMin(date);
		}		

		public void setMin(Date min) {
			this.min = min;
		}

		public Date getMax() {
			return max;
		}

		public Date getMin() {
			return min;
		}

		public void setMax(Date max) {
			this.max = max;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			min = new Date(in.readLong());
			max = new Date(in.readLong());
			count = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(min.getTime());
			out.writeLong(max.getTime());
			out.writeLong(count);
		}

		@Override
		public String toString() {
			return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
		}
	}
}
