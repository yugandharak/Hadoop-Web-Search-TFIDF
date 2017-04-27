
/* Name - Yugandhara Kulkarni
 * Email - ykulkarn@uncc.edu
 * This program performs search operations on a set of input files.*/

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);

	public static String inputQuery = "";

	public static void main(String[] args) throws Exception {

		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the word to search : ");
		inputQuery = scan.nextLine();

		//to run toolrunner for the class.
		int res = ToolRunner.run(new Search(), args);
		scan.close();
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		//to create job
		Job job = Job.getInstance(getConf(), " search ");

		//to set jar
		job.setJarByClass(this.getClass());

		//to add path for input file
		FileInputFormat.addInputPaths(job, args[0]);

		//to add path for output file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//to set mapper
		job.setJarByClass(this.getClass());

		//to set reducer
		job.setMapperClass(Map.class);

		//to set data type of key for mapper
		job.setReducerClass(Reduce.class);

		//to set data type of output key
		job.setOutputKeyClass(Text.class);

		//to set data type of output value
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion( true)  ? 0 : 1;

	}



	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		//Map function which will generate intermediate key value pairs
		public void map(LongWritable offset, Text lineText, Context context)throws IOException, InterruptedException {

			String inputLine = lineText.toString();

			//to retrieve word from the pattern of input file line
			String word = inputLine.substring(0, inputLine.indexOf("#"));

			//to retrieve file name from the pattern
			String fileName = inputLine.substring(inputLine.lastIndexOf("#") + 1, inputLine.lastIndexOf("\t"));

			//to retrieve term frequency from the pattern
			String TFIDFValue = inputLine.substring(inputLine.lastIndexOf("\t") + 1);

			//to break input string into tokens creating an instance of StringTokenizer
			StringTokenizer tokens = new StringTokenizer(inputQuery);

			//to match the words of the input query with the text of target file
			while (tokens.hasMoreTokens()) {
				if (tokens.nextToken().equalsIgnoreCase(word)) {
					context.write(new Text(fileName), new Text(TFIDFValue));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
		//Reducer that takes intermediate key value pairs from map as an input to display search results.
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)throws IOException, InterruptedException {
			double sum = 0.0;
			for (Text t : counts) {
				sum = sum + Double.parseDouble(t.toString());
			}
			context.write(new Text(word), new DoubleWritable(sum));
		}

	}
}