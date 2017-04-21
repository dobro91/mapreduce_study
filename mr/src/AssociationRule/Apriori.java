package AssociationRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Apriori {


	public static class wordcountMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String txt = value.toString();
			if(txt == null)
				return;
			StringTokenizer itr = new StringTokenizer(txt);
			String idx = itr.nextToken();

			while(itr.hasMoreTokens())
				context.write(new Text(itr.nextToken()), new Text(idx));
		}

	}


	public static class wordcountReducer extends Reducer<Text, Text, Text, Text>{


		HashMap<String, String> pair = new HashMap<String, String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			int sum = 0;
			ArrayList<String> list = new ArrayList<String>();

			for(Text value : values){
				sum = sum + 1;
				list.add(value.toString());
			}
			System.out.println(sum);
			if(sum >= 2)
				for(String value : list)
					if(pair.containsKey(value)) pair.put(value, pair.get(value) + "\t" + key.toString());
					else pair.put(value, key.toString());
		}
		@Override
		protected void cleanup(
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Entry<String, String> e : pair.entrySet()){
				System.out.println(e.getKey() +"\t"+ e.getValue());
				context.write(new Text(e.getKey()), new Text(e.getValue()));
			}
		}

	}


	public static class combinationMapper extends Mapper<Text, Text, Text, Text>{

		@Override
		protected void map(Text key, Text value,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String txt = value.toString();
			String[] splits = txt.split("\t");
			ArrayList<String> combination = combinations(
					3, splits);
			for(String each : combination){
				context.write(key, new Text(each));
			}
		}
	}


	public static ArrayList<String> combinations(final int n, final String[] list){
		final int[] selected = new int[n];
		ArrayList<String> combinationSet = new ArrayList<String>();
		ArrayList<String> sortedCombination = new ArrayList<String>();
		getCombination( n, list, 0, selected, combinationSet);
		for(String s : combinationSet)
		{
			char[] sortedAlpha = s.toCharArray();
			Arrays.sort(sortedAlpha);
			char before = 0;
			String tmp = "";
			for(char c : sortedAlpha)
				if(c == before) ;
				else if(c != before){
					before = c;
					tmp = tmp + c;
				}
			sortedCombination.add(tmp);
		}
		return sortedCombination;
	}

	public static void getCombination( final int n, final String[] list, 
			final int from, final int[] selected, ArrayList<String> combinationSet) {
		String tmp = null;
		if ( n == 0 ) { // all selected, just print them
			for ( int i = 0; i < selected.length; ++i ) {
				if(tmp == null) tmp = list[selected[i]];
				else tmp += list[selected[i]];
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
		Path Start = new Path(otherArgs[0]);
		Path tmpout = new Path(otherArgs[1] + "_tmp");
		boolean exitCode = false;
		for(int i = 0; i < 4; i++){
			FileSystem.get(new Configuration()).delete(tmpout, true);
			Path finalout = new Path(otherArgs[1]);
			Job job = new Job(conf, "Apriori");
			job.setJarByClass(Apriori.class);
			job.setMapperClass(wordcountMapper.class);
			job.setReducerClass(wordcountReducer.class);
			//		job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, Start);
			//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, tmpout);
			exitCode = job.waitForCompletion(true);
//			Counter counter = job.getCounters().findCounter("", "comb");
			if(exitCode)
			{
				
				job = new Job(conf, "combination");
				FileSystem.get(new Configuration()).delete(finalout, true);
				job.setJarByClass(Apriori.class);
				job.setMapperClass(combinationMapper.class);
				job.setNumReduceTasks(0);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				FileInputFormat.addInputPath(job, tmpout);
				FileOutputFormat.setOutputPath(job, finalout);
				Start = finalout;
				exitCode = job.waitForCompletion(true);
			}
		}
		System.exit(exitCode ? 0:1);
	}
}
