package midexam;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import MRUtils.MRUtils;

public class AdvancedJoin {

	public static class RepMapper extends Mapper<Object, Text, Text, NullWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
			String reputation = parsed.get("Reputation");
			String userId = parsed.get("Id");
			if(reputation == null || userId == null)
				return;
			if(Integer.parseInt(reputation) >= Integer.parseInt(context.getConfiguration().get("repNum"))){
				context.getCounter("bloom", "countWord").increment(1);
				context.write(new Text(userId), NullWritable.get());
			}
		}
	}


	public static class BloomMapper extends Mapper<Object, Text, Text, NullWritable>
	{
		private BloomFilter filter = new BloomFilter();

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());

			if (files != null && files.length == 1) {
				System.out.println("Reading Bloom filter from: "
						+ files[0].getPath());

				DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));

				filter.readFields(strm);
				strm.close();
			} else {
				throw new IOException(
						"Bloom filter file not set in the DistributedCache.");
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("Id");

			if (userId == null) {
				return;
			}

			if(filter.membershipTest(new Key(userId.getBytes()))){
				context.write(value, NullWritable.get());
			}

		}
	}

	public static class UserMapper extends Mapper<Text, NullWritable, Text, Text>{

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Text key, NullWritable value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRUtils.transformXmlToMap(key.toString());

			String userId = parsed.get("Id");
			String rep = parsed.get("Reputation");
			if (userId == null || rep == null) {
				return;
			}

			outkey.set(userId);

			outvalue.set("U" + key.toString());
			if(Integer.parseInt(rep) < 1500)
				return;
			if(Integer.parseInt(rep) >= 1500)
				context.write(outkey, outvalue);
		}
	}

	public static class CommentMapper extends Mapper<Object, Text, Text, Text>{
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRUtils.transformXmlToMap(value
					.toString());

			String userId = parsed.get("UserId");
			if (userId == null) {
				return;
			}

			outkey.set(userId);

			outvalue.set("C" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable>{
		private ArrayList<String> comments = new ArrayList<String>();
		private DocumentBuilderFactory dbf = DocumentBuilderFactory
				.newInstance();
		private String user = null;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			user = null;
			comments.clear();

			for (Text t : values) {
				if (t.charAt(0) == 'U') {
					user = t.toString().substring(1, t.toString().length())
							.trim();
				} else {

					comments.add(t.toString()
							.substring(1, t.toString().length()).trim());
				}
			}

			if (user != null && !comments.isEmpty()) {
				String postWithCommentChildren = nestElements(user, comments);

				context.write(new Text(postWithCommentChildren),
						NullWritable.get());
			}
		}

		private String nestElements(String user, List<String> comments) {
			try {
				DocumentBuilder bldr = dbf.newDocumentBuilder();
				Document doc = bldr.newDocument();

				Element userEl = getXmlElementFromString(user);
				Element toAddUserEl = doc.createElement("User");

				copyAttributesToElement(userEl.getAttributes(), toAddUserEl);

				for (String commentXml : comments) {
					Element commentEl = getXmlElementFromString(commentXml);
					Element toAddCommentEl = doc.createElement("Comment");

					copyAttributesToElement(commentEl.getAttributes(),
							toAddCommentEl);

					toAddUserEl.appendChild(toAddCommentEl);
				}

				doc.appendChild(toAddUserEl);

				return transformDocumentToString(doc);

			} catch (Exception e) {
				return null;
			}
		}

		private Element getXmlElementFromString(String xml) {
			try {

				DocumentBuilder bldr = dbf.newDocumentBuilder();

				return bldr.parse(new InputSource(new StringReader(xml)))
						.getDocumentElement();
			} catch (Exception e) {
				return null;
			}
		}

		private void copyAttributesToElement(NamedNodeMap attributes,
				Element element) {

			for (int i = 0; i < attributes.getLength(); ++i) {
				Attr toCopy = (Attr) attributes.item(i);
				element.setAttribute(toCopy.getName(), toCopy.getValue());
			}
		}

		private String transformDocumentToString(Document doc) {
			try {
				TransformerFactory tf = TransformerFactory.newInstance();
				Transformer transformer = tf.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
						"yes");
				StringWriter writer = new StringWriter();
				transformer.transform(new DOMSource(doc), new StreamResult(
						writer));

				return writer.getBuffer().toString().replaceAll("\n|\r", "");
			} catch (Exception e) {
				return null;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		if(otherArgs.length != 5){
			System.err.println("Usage: AdvancedJoin <users.xml> <comments.xml> <reputation count> <false ratio> <out>");
			System.exit(2);
		}

		Path tmpout = new Path(otherArgs[4] + "_tmp");
		FileSystem.get(new Configuration()).delete(tmpout, true);
		Path finalout = new Path(otherArgs[4]);

		Job job = new Job(conf, "get Reputation");
		job.getConfiguration().set("repNum", otherArgs[2]);
		job.setJarByClass(AdvancedJoin.class);
		job.setMapperClass(RepMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, tmpout);
		boolean exitCode = job.waitForCompletion(true);
		Counter counter = job.getCounters().findCounter("bloom", "countWord");

		if(exitCode){
			FileSystem fs = FileSystem.get(new Configuration());

			int numMembers = (int) counter.getValue();
			float falseRatio = Float.parseFloat(otherArgs[3]);
			Path outfile = new Path("./out");
			int vectorSize = getOptimalBloomFilterSize(numMembers, falseRatio);
			int nbHash = getOptimalK(numMembers, vectorSize);
			BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
			System.out.println("Training Bloom filter of size " + vectorSize
					+ " with " + nbHash + " hash functions, " + numMembers
					+ " approximate number of records, and " + falseRatio
					+ " false positive rate");

			String line = null;
			int numRecords = 0;
			Path inputfile = new Path(tmpout + "/part-m-00000");
			for (FileStatus status : fs.listStatus(inputfile)) {
				BufferedReader rdr;
				if (status.getPath().getName().endsWith(".gz")) {
					rdr = new BufferedReader(new InputStreamReader(
							new GZIPInputStream(fs.open(status.getPath()))));
				} else {
					rdr = new BufferedReader(new InputStreamReader(fs.open(status
							.getPath())));
				}

				System.out.println("Reading " + status.getPath());
				while ((line = rdr.readLine()) != null) {
					filter.add(new Key(line.getBytes()));
					++numRecords;
				}

				rdr.close();
			}

			System.out.println("Trained Bloom filter with " + numRecords
					+ " entries.");

			System.out.println("Serializing Bloom filter to HDFS at " + outfile);
			FSDataOutputStream strm = fs.create(outfile);
			filter.write(strm);

			strm.flush();
			strm.close();

			System.out.println("Done training Bloom filter.");

			////
			FileSystem.get(new Configuration()).delete(tmpout, true);
			job = new Job(conf, "Bloom Filtering");
			job.setJarByClass(AdvancedJoin.class);
			job.setMapperClass(BloomMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

			FileOutputFormat.setOutputPath(job, tmpout);

			DistributedCache.addCacheFile(FileSystem.get(conf).makeQualified(outfile).toUri(), job.getConfiguration());
			exitCode = job.waitForCompletion(true);


			if(exitCode){

				job = new Job(conf, "Advanced Join operation");
				job.setJarByClass(AdvancedJoin.class);
				MultipleInputs.addInputPath(job, tmpout, SequenceFileInputFormat.class, UserMapper.class);
				MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, CommentMapper.class);
				job.setReducerClass(JoinReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);

				FileOutputFormat.setOutputPath(job, finalout);
				exitCode = job.waitForCompletion(true);
			}
		}
		System.out.println("Total excution time is " + (System.currentTimeMillis() - startTime));
		System.exit(exitCode ? 0 : 1);
	}

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float)Math.log(falsePosRate)/Math.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize/numMembers * Math.log(2));
	}

}
