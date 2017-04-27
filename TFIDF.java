
/* Name - Yugandhara Kulkarni
 * Email - ykulkarn@uncc.edu
 * This program calculates the number of occurrences of each word in input file in the form of Term frequency & Inverse Document Frequency.*/

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static final String COUNT_OF_FILES = "count";
	private static final String delimeter=new String("#####");    
	private static final String comma=new String(",");    
	private static final String equal=new String("=");    

	public static void main(String[] args) throws Exception {

		//to run toolrunner for the class TermFrequency as we need to use output of TermFrequency as an input to this class.
		// This step enables chaining
		ToolRunner.run(new TermFrequency(), args);
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		//to calculate total number of input files
		int count = new File(args[0]).listFiles().length;
		Configuration conf = new Configuration();

		//to set the count of input files
		conf.setInt(COUNT_OF_FILES,count);

		//to create job
		Job job_obj = Job.getInstance(conf, " Job");

		//to set jar
		job_obj.setJarByClass(this.getClass());

		//to add path for input file
		FileInputFormat.addInputPaths(job_obj, args[1]);

		//to add path for output file
		FileOutputFormat.setOutputPath(job_obj, new Path(args[2]));

		//to set mapper
		job_obj.setMapperClass(Map.class);

		//to set reducer
		job_obj.setReducerClass(Reduce.class);

		//to set data type of key for mapper
		job_obj.setMapOutputKeyClass(Text.class);

		//to set data type of value for mapper
		job_obj.setMapOutputValueClass(Text.class);

		//to set data type of key for reducer
		job_obj.setOutputKeyClass(Text.class);

		//to set data type of value for reducer
		job_obj.setOutputValueClass(DoubleWritable.class);
		return job_obj.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		//Map function which will generate intermediate key value pairs
		public void map(LongWritable offset, Text lineText, Context context)throws IOException, InterruptedException {
			String eachLine = lineText.toString();

			//to separate word and file name from given input file
			String[] splitAtDelimiter = eachLine.split(delimeter);

			//to separate file name and term frequency digit available string
			String[] splitAtTab = splitAtDelimiter[1].split("\t");
			context.write(new Text(splitAtDelimiter[0]), new Text(splitAtTab[0] + equal + splitAtTab[1]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

		//Reducer that takes intermediate key value pairs from map as an input to calculate term frequency and inverse document frequency.
		@Override
		public void reduce( Text word,  Iterable<Text> file,  Context context)throws IOException,  InterruptedException {
			String listOfFiles="";
			int count=0;
			Configuration conf=context.getConfiguration();

			//to create a string of list of files containing current word
			for(Text list:file){
				if(listOfFiles.equals(""))
					listOfFiles=listOfFiles+list.toString();
				else
					listOfFiles=listOfFiles+comma+list.toString();
				count++;
			}
			
			//total number of input files
			int numberOfFiles=conf.getInt(COUNT_OF_FILES,0);

			double TFIDF=0;
			double WF = 0;
			double IDF=0;

			//separate methods of calculation for singular and multiple files as words with single file do not have comma seperated values and hence cannot be split over comma
			if(count>1){

				//to get the array of array of file names separated by comma
				String[] arrayOfFileNamesAndFrequency=listOfFiles.toString().split(comma);
				for(int j=0;j<arrayOfFileNamesAndFrequency.length;j++){

					//to separate file names and term frequency using split over equals sign
					String[] arrayOfFileNames=arrayOfFileNamesAndFrequency[j].split(equal);

					//to calculate Inverse Document Frequency
					IDF = Math.log10(1+(numberOfFiles/arrayOfFileNamesAndFrequency.length));

					//to get the value of term frequency
					WF = Double.parseDouble(arrayOfFileNames[1]);

					//to calculate TDIDF using formula TFIDF = IDF * WF
					TFIDF=IDF*WF;
					context.write(new Text(word+delimeter+arrayOfFileNames[0]),new DoubleWritable(TFIDF));
				}
			}
			else{

				//to get the file name by splitting over equal sign which separates file name and term frequency
				String[] arrayOfFileNameAndFrequency=listOfFiles.toString().split(equal);

				//to calculate Inverse Document Frequency
				IDF = Math.log10(1+(numberOfFiles));

				//to get the value of term frequency which is the second element of arrayOfFileNameAndFrequency array.
				WF = Double.parseDouble(arrayOfFileNameAndFrequency[1]);

				//to calulate TFIDF using formula TFIDF = IDF * WF
				TFIDF=IDF*WF;
				context.write(new Text(word+delimeter+arrayOfFileNameAndFrequency[0]),new DoubleWritable(TFIDF));
			}
		}




	}
}