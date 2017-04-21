package workshop_ex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class lottoWordCounter {



	public static class LottoMapper extends Mapper<Object, Text, Word, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> i = new ArrayList<String>();
			Word words = new Word();
			String[] header = {	"Year", "Round", "Date", "1","2","3","4","5","6","Bonus"};
			Map<String, String> parsed = MRUtils.transformCSVToMap(header, value.toString());
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
			String[] flags = {"1","2","3","4","5","6","Bonus"};
			int m = 0;
			for(String k : i){
				words.setFlag(new Text(flags[m % 7]));
				words.setTag(new Text(k));
				System.out.println(words.toString());
				context.write(words, one);
				m++;
			}

		}

	}

	public static class LottoReducer extends Reducer<Word, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		private MultipleOutputs<Text, IntWritable> mos = null;
		private Text w = new Text();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}


		@Override
		protected void reduce(Word key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			result.set(sum);
			w.set(key.getTag());
			mos.write("bins", w, result, key.getFlag().toString());
		}
		@Override
		protected void cleanup(
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Binning <posts> <outdir>");
			System.exit(1);
		}

		Job job = new Job(conf, "Binning");
		
		
		TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(lottoWordCounter.class);
		
		job.setMapperClass(LottoMapper.class);
		job.setReducerClass(LottoReducer.class);
			job.setMapOutputKeyClass(Word.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, IntWritable.class);



		MultipleOutputs.setCountersEnabled(job, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


	public static class Word implements Writable{



		private Text flag, tag;
		public Text getFlag() {
			return flag;
		}
		
		

		public Word() {
			super();
			this.flag = new Text();
			this.tag = new Text();
		}



		public Text getTag() {
			return tag;
		}

		public void setTag(Text tag) {
			this.tag = tag;
		}

		public void setFlag(Text flag) {
			this.flag = flag;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			flag.readFields(in);
			tag.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			flag.write(out);
			tag.write(out);

		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return this.flag.toString() + this.tag.toString();
		}
	}
}
