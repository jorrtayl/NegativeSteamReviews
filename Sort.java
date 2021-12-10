import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Sort {

    public static class Map1 extends Mapper<Object, Text, IntWritable, Text>
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
            {
                int number = 999; 
                String word = "empty";

                if(stringTokenizer.hasMoreTokens())
                {
                    String str0 = stringTokenizer.nextToken();
                    word = str0.trim();
                }

                if(stringTokenizer.hasMoreElements())
                {
                    String str1 = stringTokenizer.nextToken();
                    number = Integer.parseInt(str1.trim());
                }

                context.write(new IntWritable(number), new Text(word));
            }
        }
    }

    public static class Reduce1 extends Reducer<IntWritable, Text, IntWritable, Text>
    {
        public void reduce(IntWritable key, Iterator<Text> values, Context context) throws IOException, InterruptedException
        {
            while((values.hasNext()))
            {
                context.write(key, values.next());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

        Job job = Job.getInstance(new Configuration(), "sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}