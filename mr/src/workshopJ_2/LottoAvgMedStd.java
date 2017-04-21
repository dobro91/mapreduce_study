package workshopJ_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;

public class LottoAvgMedStd {

	public static class IntervalMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		// 1. Round will parsed!
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ArrayList<String> i = new ArrayList<String>();
			String[] header = {"Year", "Round", "Date", "1","2","3","4","5","6","Bonus"};
			Map<String, String> parsed = MRUtils.transformCSVToMap(header, value.toString());
			if(parsed == null)
				return;
			String Round = parsed.get("Round");
			String first = parsed.get("1");
			String second = parsed.get("2");
			String third = parsed.get("3");
			String forth = parsed.get("4");
			String fifth = parsed.get("5");
			String sixth = parsed.get("6");
			String bonus = parsed.get("Bonus");

			if(first == null || second == null || third == null || Round == null ||
					forth == null || fifth == null || sixth == null || bonus == null)
				return;

			i.add(String.valueOf(first));
			i.add(String.valueOf(second));
			i.add(String.valueOf(third));
			i.add(String.valueOf(forth));
			i.add(String.valueOf(fifth));
			i.add(String.valueOf(sixth));
			i.add(String.valueOf(bonus));

			for(String word : i)
				context.write(new IntWritable(Integer.parseInt(word)),
						new IntWritable(Integer.parseInt(Round)));

		}
	}

	public static class IntervalReducer extends Reducer<IntWritable, IntWritable, IntWritable, Tuple>{


		ArrayList<Integer> data = new ArrayList<Integer>();

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			data.clear();
			for(IntWritable value : values){
				data.add(value.get());
			}

			Collections.sort(data);

			int before = 0;

			for(int interval : data)
			{
				Tuple t = new Tuple();
				System.out.println(interval);
				t.setRound(interval);
				t.setMed((float)interval - before);
				t.setAvg((float)interval - before);
				context.write(key, t);
				before = interval;
			}

		}


	}

	public static class LottoStatsMapper extends Mapper<IntWritable, Tuple, IntWritable, Tuple>
	{

		@Override
		protected void map(IntWritable key, Tuple value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub


			context.write(key, value);
		}

	}

	public static class LottoStatsReducer extends Reducer<IntWritable, Tuple, IntWritable, Tuple>{


		private ArrayList<Float> MedianList = new ArrayList<Float>();
		Tuple result = new Tuple();

		@Override
		protected void reduce(IntWritable key, Iterable<Tuple> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			MedianList.clear();

			int round = 0, sum = 0, count = 0;
			float mean = 0, sumOfSquares = 0.0f;
			for(Tuple value : values)
			{

				MedianList.add(value.getMed());
				if(value.getRound() > round)
					round = value.getRound();

				sum += value.getAvg();
				count ++;
			}

			Collections.sort(MedianList);
			result.setRound(round);
			result.setCount(count);
			mean = sum / (float)MedianList.size();
			result.setAvg(mean);
			if(count % 2 == 0)
				result.setMed((MedianList.get(MedianList.size() / 2 - 1 ) + MedianList.get((int) MedianList.size() / 2)) / 2.0f);
			else
				result.setMed(MedianList.get((int) MedianList.size() / 2));

			for(Float f : MedianList)
				sumOfSquares += (f - mean) * (f - mean);
			
			result.setStd((float) Math.sqrt(sumOfSquares /(MedianList.size() - 1)));

			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		Path tmpout = new Path(otherArgs[1] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[1]);
		Job job = new Job(conf,
				"Lotto get Interval");

		job.setJarByClass(LottoAvgMedStd.class);
		job.setMapperClass(IntervalMapper.class);
		job.setReducerClass(IntervalReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Tuple.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);		
		boolean exitCode = job.waitForCompletion(true);
		if(exitCode)
		{
			job = new Job(conf, "Lotto Stats");
			job.setJarByClass(LottoAvgMedStd.class);
			job.setMapperClass(LottoStatsMapper.class);
			job.setReducerClass(LottoStatsReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Tuple.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, tmpout);
			FileOutputFormat.setOutputPath(job, finalout);
			exitCode = job.waitForCompletion(true);
		}

		System.exit(exitCode ? 0:1);
	}



	public static class Tuple implements Writable{
		float count;
		float avg;
		float std;
		float med;
		int round;
		
		public Tuple() {
			this.count = 0;
			this.avg = 0f;
			this.std = 0f;
			this.med = 0f;
			this.round = 0;
		}

		public float getCount() {
			return count;
		}

		public int getRound() {
			return round;
		}

		public void setRound(int round) {
			this.round = round;
		}

		public void setCount(float count) {
			this.count = count;
		}

		public float getAvg() {
			return avg;
		}

		public void setAvg(float avg) {
			this.avg = avg;
		}

		public float getStd() {
			return std;
		}

		public void setStd(float std) {
			this.std = std;
		}

		public float getMed() {
			return med;
		}

		public void setMed(float med) {
			this.med = med;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			count = in.readFloat();
			avg = in.readFloat();
			std = in.readFloat();
			med = in.readFloat();
			round = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeFloat(count);
			out.writeFloat(avg);
			out.writeFloat(std);
			out.writeFloat(med);
			out.writeInt(round);
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return avg + "\t" + std + "\t" + med + "\t" + round;
		}
	}
}
