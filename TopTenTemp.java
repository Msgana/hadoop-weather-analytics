import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/* Project1 
 * This program filters out the top ten raninfalls for the 
 * years 1960 and up
 * 
 * Date@ Feb-09,2021
 * Professor Yasin N. Silva
 * Mesegena Ysieni
 * Misgana Gebremariam
 * Mesfin Haile
 * */



public class TopTenTemp extends Configured implements Tool {


	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
 	{
		private static int COUNT 		= 10;
		private static int STATION_ID	= 0;
		private static int ZIPCODE		= 1;
		private static int LAT			= 2;
		private static int LONG			= 3;
		private static int TEMP			= 4;
		private static int PERCIP		= 5;
		private static int HUMID		= 6;
		private static int YEAR			= 7;
		private static int MONTH		= 8;

		private TreeMap <Double,String> treeMap;


		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			treeMap = new TreeMap <Double,String>();

		}//End for setup


		public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException
 		{
 			//StationID | Zipcode |  Lat      |  Lon       |  Temp   | Percip | Humid | Year | Month

 			String[] element = new String[9];
 			StringTokenizer st = new StringTokenizer(inputValue.toString());


 			for(int i = 0; i < element.length; i++)
 			{
	 			if(st.hasMoreTokens())
	 			{
	 				element[i] = st.nextToken();
	 				if(i == 0 && element[i].equals("StationID"))
	 				{
	 					return;
	 				}

	 				//Skip if the year is less than 1960
	 				if(i == 7 && Integer.parseInt(element[i]) < 1959)
						return;

	 			}
				else
					return;
 			}


 			//TreeMap <Double,String> treeMap = new TreeMap <Double,String>();
 			//System.out.println("Zipcode "+"Year "+"Month "+"Rainfall");
 			treeMap.put(Double.parseDouble(element[PERCIP]), (element[ZIPCODE] + "\t" + element[YEAR]) + "\t" 
 													+ element[MONTH]);

 			if(treeMap.size() > 10)
 			{
 				treeMap.remove(treeMap.firstKey());
 			}

 		}//End of Map


 		//Get entry set of the tree map using the entrySet method

 		//Set<Map.Entry<Double,String>> entries = treeMap.entrySet();

 		//Convert entry set to Array using the toArray method
 		//Map.Entry<Double, String> []entryArray = entries.toArray(new Map.Entry[entries.size()]);

		@Override
 		public void cleanup(Context context) throws IOException, InterruptedException
 		{
 			for(Map.Entry<Double, String> entry : treeMap.entrySet())
 	 		{
 	 			String mapKey = String.valueOf(entry.getKey());
 	 			String mapValue = entry.getValue();

 	 			context.write( new Text(mapKey), new Text(mapValue));
 	 		}
 		}//End for CleanUp

	}//end MyMapper






	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{

		private TreeMap <Double,String> treeReduce;

		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			treeReduce = new TreeMap <Double,String>();
			context.write(new Text("Zipcode\t" + "Year\t"+ "Month\t"+"Rainfall\t"+"\n"),null);
		}


		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//Iterator<Text> valItr = values.iterator();
			//TreeMap <Double,String> treeReduce = new TreeMap <Double,String>();

			for(Text value:values)
			{
				treeReduce.put(Double.parseDouble(key.toString()),value.toString());

				if(treeReduce.size() > 10)
	 			{
	 				treeReduce.remove(treeReduce.firstKey());
	 			}
			}

		}//end reduce



		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{

			for(Map.Entry<Double, String> entry : treeReduce.descendingMap().entrySet())
			//for(String t: treeReduce.descendingMap().values())
			{

			 		String reduceKey = String.valueOf(entry.getKey());
			 		String reduceValue = entry.getValue();

			 		//System.out.println(reduceValue);

			 		context.write( new Text(reduceValue), new Text(reduceKey));

			 }


		}




	}//end MyReducer






	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new TopTenTemp(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.out.println("Usage: bin/hadoop jar InstructionalSolutions.jar SortByStationID <input directory> <ouput directory>");
			System.out.println("args length incorrect, length: " + args.length);
			return -1;
		}
		int numReduces;

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		numReduces 	= 1;

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if(!fs.exists(inputPath))
		{
			System.out.println("Usage: bin/hadoop jar InstructionalSolutions.jar SortByStationID <input directory> <ouput directory>");
			System.out.println("Error: Input Directory Does Not Exist");
			System.out.println("Invalid input Path: " + inputPath.toString());
			return -1;
		}

		if(fs.exists(outputPath))
		{
			System.out.println("Usage: bin/hadoop jar InstructionalSolutions.jar SortByStationID <input directory> <ouput directory>");
			System.out.println("Error: Output Directory Already Exists");
			System.out.println("Please delete or specifiy different output directory");
			return -1;
		}

		Job job = new Job(conf, "MapReduceShell Test");

		job.setNumReduceTasks(numReduces);
		job.setJarByClass(TopTenTemp.class);

		//sets mapper class
		job.setMapperClass(MyMapper.class);

		//sets map output key/value types
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

		//Set Reducer class
	    job.setReducerClass(MyReducer.class);

	    // specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//sets Input format
	    job.setInputFormatClass(TextInputFormat.class);

	    // specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);


		job.waitForCompletion(true);
		return 0;
	}

}

