import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogProcessor {

    public static class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static final Pattern logPattern = Pattern.compile(
            "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\d{3}) (\\S+)"
        );

        private Text outKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = logPattern.matcher(line);
            if (matcher.find()) {
                String host = matcher.group(1);
                String identity = matcher.group(2);
                String user = matcher.group(3);
                String time = matcher.group(4);
                String request = matcher.group(5);
                String status = matcher.group(6);
                String size = matcher.group(7);
                if (size.equals("-")) size = "0";

                String outputLine = host + "\t" + identity + "\t" + user + "\t" + time + "\t" + request + "\t" + status + "\t" + size;
                outKey.set(outputLine);
                context.write(outKey, NullWritable.get());
            }
        }
    }

    public static class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogProcessor");
        job.setJarByClass(LogProcessor.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
