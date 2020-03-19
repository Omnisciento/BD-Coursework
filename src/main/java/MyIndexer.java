import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.PorterStemmer;

/*

values.txt will contain number of documents,length of document d. Average length can be computed in query processor.

* */
public class MyIndexer extends Configured implements Tool {
    static Set<String> stopWords = new HashSet<>();
    private static FileSystem fs;

    static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        public enum COUNTER {
            NUMBER_OF_ARTICLES,
            NUMBER_OF_TERMS
        };
        @Override
        //Extract first line of each document which will be the article title.
        //Remove article articlesnumbers as it is a design decision.
        //number of  is the number of map input records
        //Do job get counters.
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if(!tokenizer.hasMoreTokens()){
                return;
            }
            String title = tokenizer.nextToken();
            context.getCounter(COUNTER.NUMBER_OF_ARTICLES).increment(1);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if(stopWords.contains(token)){
                    continue;
                }
                //Run stemmer.
                PorterStemmer stemmer = new PorterStemmer();
                stemmer.add(token.toCharArray(), token.length());
                stemmer.stem();
                token = stemmer.toString();

                context.getCounter(COUNTER.NUMBER_OF_TERMS).increment(1);

                this.word.set(title + ":" + token);
                Text one = new Text();
                one.set("1");
                try{
                    context.write(this.word, one);
                    context.write(new Text(token), new Text(title));
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws
                IOException, InterruptedException{
            super.cleanup(context);
        }
    }

    public static class Reduce extends Reducer<WritableComparable, Writable,
            WritableComparable, Writable> {

        private MultipleOutputs<Text, Text> mos;

        @Override
        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }
    /**
     * Term frequency outputs a Composite key consisting of the Article and the Term and a frequency value
     * Article:Term -> Frequency.
     *
     * Document Frequency of Terms outputs a key which is a term and a frequency value which
     * tells us how many documents contains the Term.
     * Term -> Document Frequency.
     *
     *
     * */
        public void reduce(WritableComparable key, Iterable<Writable> values, Context
                context) throws IOException, InterruptedException {
            //Article : Term -> Frequency
            if(key.toString().contains(":")){
                String[] keys = key.toString().split(":");
                if(keys.length != 2){
                    return;
                }
                int sum = 0;
                for(Writable freq : values){
                    sum++;
                }
                mos.write("TermFrequency", key, new Text("" + sum));
            } else{
                Set<String> articles = new HashSet<>();
                String term = key.toString();
                for(Writable article : values){
                    articles.add(article.toString());
                }try{
                    mos.write("DocumentFrequencyOfTerms", key, new Text("" + articles.size()));

                } catch(Exception e){
                    System.out.println(e.getMessage());
                }
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            mos.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration myconf = getConf();

        // The following two lines instruct Hadoop/MapReduce to run in local
        // mode. In this mode, mappers/reducers are spawned as thread on the
        // local machine, and all URLs are mapped to files in the local disk.
        // Remove these lines when executing your code against the cluster.
    //    myconf.set("mapreduce.framework.name", "local");
    //    myconf.set("fs.defaultFS", "file:///");

        myconf.set("textinputformat.record.delimiter", "\n[[");
        myconf.set("mapred.textoutputformat.separator", "\t");
        myconf.set("mapreduce.map.memory.mb", "2096");
        myconf.set("mapreduce.reduce.memory.mb", "2096");
        myconf.set("mapreduce.map.java.opts", "1000");
        myconf.set("mapreduce.reduce.java.opts", "1000");

        File file = new File("stopword-list.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        while ((st = br.readLine()) != null){
            stopWords.add(st);
        }


        Job job = Job.getInstance(myconf, "MyIndexer");
        job.setJarByClass(MyIndexer.class);

        job.setMapperClass(Map.class);
     //   job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

       job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "TermFrequency", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "DocumentFrequencyOfTerms",
                TextOutputFormat.class,
                Text.class, Text.class);

        MultipleOutputs.setCountersEnabled(job, true);

        int jobComplete = (job.waitForCompletion(true) ? 0 : 1);
        Counters counter = job.getCounters();
        Counter numOfMappers = counter.findCounter(Map.COUNTER.NUMBER_OF_ARTICLES);
        Counter numOfTerms = counter.findCounter(Map.COUNTER.NUMBER_OF_TERMS);

        System.out.println("The Number of Terms is: " +   numOfTerms.getValue());
        System.out.println("The Number of Documents is: " +   numOfMappers.getValue());

        return jobComplete;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));
    }
}