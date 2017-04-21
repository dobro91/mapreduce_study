package TextMining;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TF_IDF {

	public static class wordcountMapper extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();
		public  Path filesplit;

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			filesplit = ((FileSplit) context.getInputSplit()).getPath();
			System.out.println(filesplit);
			//			String fileName =  getFilename((FileSplit) context.getInputSplit());
			String txt = value.toString();
			if(txt == null)
				return;
			txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

			txt = txt.replaceAll("'", "");
			txt = txt.replaceAll("[^a-zA-Z]", " ");

			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new Text("AAA"));
			}
		}


	}

	public static class wordcountReducer extends Reducer<Text, Text, Text, DoubleWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			HashSet<Text> df = new HashSet<Text>();
			int TF = 0;
			for(Text value : values){
				TF++;
				df.add(value);
			}
			int N = context.getConfiguration().getInt("documentList", 0);
			if(N == 0){
				System.err.println("error!!!");
				System.exit(2);
			}
			double weight = TF * (Math.log(N / df.size()));

			context.write(key, new DoubleWritable(weight));

		}


	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration(); //configuration initialize
		String[] otherArgs = new GenericOptionsParser(conf, args) //argument passing
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Versus H,C <in> <out>");
			System.exit(2);
		}

		File dir = new File(args[0]);
		File[] fileList = dir.listFiles();
		Path[] filePaths = new Path[fileList.length];
		int i = 0;
		for(File each : fileList){
			filePaths[i++] = new Path(each.getCanonicalPath().toString());
			System.out.println(each.getCanonicalPath().toString());
		}
		Job job = new Job(conf, "Versus H,C"); //job design.
		job.setJarByClass(TF_IDF.class); //main class define
		for(Path p : filePaths)
			MultipleInputs.addInputPath(job, p, TextInputFormat.class, wordcountMapper.class);
		job.setMapperClass(wordcountMapper.class); //SOWordCountMapper class.
		job.setCombinerClass(wordcountReducer.class);
		job.setReducerClass(wordcountReducer.class); //IntSum
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.getConfiguration().setInt("documentList", fileList.length);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
