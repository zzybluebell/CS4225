import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {
    public static class MapOne extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> stopwords;
        private String localFiles;
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void setup(Context context) throws IOException {
            stopwords = new TreeSet<>();
            Configuration conf = context.getConfiguration();
            localFiles = conf.getStrings("stopwords")[0];
            FileSystem fs = FileSystem.get(URI.create(localFiles), conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(localFiles));
            String line;
            BufferedReader br = new BufferedReader( new InputStreamReader(hdfsInStream, "utf-8"));
            while ((line = br.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line);
                while (itr.hasMoreTokens()) {
                    stopwords.add(itr.nextToken());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (!stopwords.contains(word)) {
                    context.write(new Text(fileName + "\t" + word), one);
                }
            }
        }
    }

    public static class ReduceOne extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class MapTwo extends Mapper<LongWritable, Text, IntWritable, Text> {
        Map<String,Integer> compSet = new HashMap();
        String curKeyFile = null;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String keyFile = data[0];
            String keyword = data[1];
            int num = Integer.parseInt(data[2]);
            if (compSet.isEmpty()) {
                curKeyFile = keyFile;
            }
            if (keyFile.equals(curKeyFile)) {
                compSet.put(keyword, num);
            } else {
                if (compSet.containsKey(keyword)) {
                    int sum = compSet.get(keyword);
                    if (sum <= num) {
                        context.write(new IntWritable(sum), new Text(keyword));
                    } else {
                        context.write(new IntWritable(num), new Text(keyword));
                    }
                }
            }
        }
    }

    public static class ReduceTwo extends Reducer<IntWritable, Text, IntWritable, Text>  {

        private static List<Map<Integer, String>> list = new ArrayList();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (Text text : values) {
                Map<Integer, String> map = new HashMap();
                map.put(key.get(), text.toString());
//                int i = 0 ;
//                context.write(new IntWritable(key.get()), new Text(text.toString()));
                list.add(map);
                int size = list.size();
                if(size > 20){
                    list.remove(size - 1);
                }
            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //reorder
            Queue<Map<Integer, String>> q = new LinkedList(list);

            Collections.sort(list, new Comparator<Map<Integer, String>>() {
                @Override
                public int compare(Map<Integer, String> o1, Map<Integer, String> o2) {
                    Map.Entry<Integer, String> entry1 = o1.entrySet().iterator().next();
                    Map.Entry<Integer, String> entry2 = o2.entrySet().iterator().next();
                    if ((int)entry1.getKey() == (int) entry2.getKey()) {
                        return -1 * entry1.getValue().compareTo(entry2.getValue());
                    }
                    return 0;
                }
            });
            for (Map<Integer, String> map : list) {

                System.out.println(Arrays.asList(map.entrySet().toArray()));
                for (Map.Entry<Integer, String> entry : map.entrySet()) {
                    context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
                }
            }
        }
    }

    public static class Sort extends WritableComparator {

        protected Sort(){
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -1* a.compareTo(b);
        }
    }

    public static void main(String[] args) throws Exception {
        String out = args[3];
//        String out = "output";
        String tmpout = "A0209261L";
        if (out.endsWith("/")) {
            int targetIndex =  out.length() - 2;
            tmpout = out.substring(0, targetIndex) + "tmp/";
        }
        Configuration confA = new Configuration(true);
        confA.setStrings("stopwords", args[2]);
//        confA.setStrings("stopwords", "data/stopwords.txt");
        // jobA
        Job jobA = Job.getInstance(confA, "Top K Common Words");
        jobA.setJarByClass(TopkCommonWords.class);
        jobA.setMapperClass(MapOne.class);
        jobA.setReducerClass(ReduceOne.class);
        jobA.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(jobA, new Path(args[0]));
        FileInputFormat.addInputPath(jobA, new Path(args[1]));
//        FileInputFormat.addInputPath(jobA, new Path("data/task1-input1.txt"));
//        FileInputFormat.addInputPath(jobA, new Path("data/task1-input2.txt"));
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(jobA, new Path(tmpout));

        if(jobA.waitForCompletion(true)) {

            //jobB
            Configuration confB = new Configuration(true);
            confB.set("topKout", out);
            Job jobB = Job.getInstance(confB, "sort");
            jobB.setJarByClass(TopkCommonWords.class);
            jobB.setMapperClass(MapTwo.class);
            jobB.setReducerClass(ReduceTwo.class);
            jobB.setInputFormatClass(TextInputFormat.class);
            jobB.setSortComparatorClass(Sort.class);
            FileInputFormat.addInputPath(jobB, new Path(tmpout));
            jobB.setOutputKeyClass(IntWritable.class);
            jobB.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(jobB, new Path(out));
            if (jobB.waitForCompletion(true) ) {
                FileSystem fs = FileSystem.get(URI.create(tmpout), confB);
                fs.delete(new Path(tmpout), true);
            }
        }

    }
}