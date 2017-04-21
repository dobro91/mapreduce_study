package workshop1_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MRUtils.MRUtils;
//sales - avg saf - workhour - salary
public class AverageDriver_csv {
	
	public static class SOAverageMapper extends
	Mapper<Object, Text, Text, CountAverageTuple> {

		private Text outHour = new Text();
		private CountAverageTuple outCountAverage = new CountAverageTuple();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			String[] header = 
				{"satisfaction_level","last_evaluation","number_project",
					"average_montly_hours","time_spend_company","Work_accident","left","promotion_last_5years","sales","salary"};
			Map<String, String> parsed = MRUtils.transformCSVToMap(header, value
					.toString());

			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strSatify = parsed.get("satisfaction_level");
			String strWorkHour = parsed.get("average_montly_hours");
			String salary = parsed.get("salary");
			// Grab the comment to find the length
			String text = parsed.get("sales");

			// .get will return null if the key is not there
			if (strSatify == null || text == null || strWorkHour == null || salary == null) {
				// skip this record
				return;
			}

			// get the hour this comment was posted in

			// get the comment length
			if (salary.equals("low")){
				outCountAverage.setSalary(0);
			}else if(salary.equals("medium")){
				outCountAverage.setSalary(50);
				
			}else if(salary.equals("high")){
				outCountAverage.setSalary(100);
			}else{
				System.out.println("Warning!");
			}
			outCountAverage.setAverageSatfiy(Float.parseFloat(strSatify));
			outCountAverage.setWorkHour(Float.parseFloat(strWorkHour));
			outCountAverage.setCount(1);
			outHour.set(text);
			// write out the user ID with min max dates and count
			context.write(outHour, outCountAverage);
			return;
		}
	}

	public static class SOAverageReducer
	extends
	Reducer<Text, CountAverageTuple, Text, CountAverageTuple> {
		private CountAverageTuple result = new CountAverageTuple();

		@Override
		public void reduce(Text key, Iterable<CountAverageTuple> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0, sum2 = 0, sum3 = 0, sum4 = 0;
			float count = 0;

			// Iterate through all input values for this key
			for(CountAverageTuple value : values){
				sum += (value.getAverage() * value.getCount());
				sum2 += (value.getAverageSatfiy() * value.getCount());
				sum3 += (value.getSalary() * value.getCount());
				sum4 += (value.getWorkHour() * value.getCount());
				count += value.getCount();
			}
			result.setCount(count);
			result.setAverage(sum / count);
			result.setAverageSatfiy(sum2 / count);
			result.setSalary(sum3 / count);
			result.setWorkHour(sum4 / count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageDriver <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Average Comment Length");
		job.setJarByClass(AverageDriver_csv.class);
		job.setMapperClass(SOAverageMapper.class);
		job.setCombinerClass(SOAverageReducer.class);
		job.setReducerClass(SOAverageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountAverageTuple.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class CountAverageTuple implements Writable {
		private float count = 0f;
		private float averageSatfiy = 0f;
		private float workHour = 0f;
		private float salary = 0f;

		public float getCount() {
			return count;
		}

		public float getAverageSatfiy() {
			return averageSatfiy;
		}

		public void setAverageSatfiy(float averageSatfiy) {
			this.averageSatfiy = averageSatfiy;
		}

		public float getWorkHour() {
			return workHour;
		}

		public void setWorkHour(float workHour) {
			this.workHour = workHour;
		}

		public float getSalary() {
			return salary;
		}

		public void setSalary(float salary) {
			this.salary = salary;
		}

		public void setCount(float count) {
			this.count = count;
		}

		public float getAverage() {
			return averageSatfiy;
		}

		public void setAverage(float average) {
			this.averageSatfiy = average;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readFloat();
			averageSatfiy = in.readFloat();
			salary = in.readFloat();
			workHour = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(count);
			out.writeFloat(averageSatfiy);
			out.writeFloat(salary);
			out.writeFloat(workHour);
		}

		@Override
		public String toString() {
			return count + "|" + averageSatfiy + "|" + salary + "|" + workHour;
		}
	}
}
