package AssociationRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Apriori2 {


	public static class combinationMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String txt = value.toString();
			String[] splits = txt.split(";");
			int maxItr = 1000;
			int k;
			for(k = 1; k <= maxItr; ++k){
				ArrayList<String> combination = combinations(
						k, splits);
				for(String each : combination){
//					System.out.println(each);
					context.write(new Text("L" + k), new Text(each));
				}
				if(combination.size() == 1)
					return;
			}
		}
	}


	public static class combinationReducer extends Reducer<Text, Text, Text, IntWritable>{

		HashMap<String, Integer> support = new HashMap<String, Integer>();
		
		private MultipleOutputs<Text, IntWritable> mos = null;

		@Override
		protected void setup(Context context) {
			// Create a new MultipleOutputs using the context object
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			support.clear();
			for(Text value : values)
				if(support.containsKey(value.toString())){
					support.put(value.toString(), support.get(value.toString()) + 1);
//					System.out.println("!");
				}
				else{
					support.put(value.toString(), 1);
					System.out.println(value.toString());
				}

			for(Entry<String, Integer> e : support.entrySet())
				if(e.getValue() >= 2201 * 0.005)
					mos.write("bins", e.getKey(), e.getValue(), key.toString());
//					context.write(new Text(key.toString() + "\t" + e.getKey()), new IntWritable(e.getValue()));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Close multiple outputs!
			mos.close();
		}

	}
	public static ArrayList<String> combinations(final int n, final String[] list){
		final int[] selected = new int[n];
		ArrayList<String> combinationSet = new ArrayList<String>();
		getCombination( n, list, 0, selected, combinationSet);
		return combinationSet;
	}

	public static void getCombination( final int n, final String[] list, 
			final int from, final int[] selected, ArrayList<String> combinationSet) {
		String tmp = null;
		if ( n == 0 ) { // all selected, just print them
			for ( int i = 0; i < selected.length; ++i ) {
				if(tmp == null) tmp = list[selected[i]];
				else tmp += "/" + list[selected[i]];
			}
			combinationSet.add(tmp);
			return;
		}
		// select one and use recursion for others
		for ( int i = from; i < list.length; ++i ) {
			selected[ selected.length - n ] = i;
			getCombination( n - 1, list, i + 1, selected, combinationSet);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if(otherArgs.length != 2){
			System.err.println("Usage: Apriori <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Apriori");
		job.setJarByClass(Apriori2.class);
		job.setMapperClass(combinationMapper.class);
		job.setReducerClass(combinationReducer.class);
//		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, IntWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
