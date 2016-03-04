/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.URLPartitioner.SelectorEntryPartitioner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.ToolUtil;

public class GeneratorJob extends NutchTool implements Tool {
  public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  public static final String GENERATOR_MIN_SCORE = "generate.min.score";
  public static final String GENERATOR_FILTER = "generate.filter";
  public static final String GENERATOR_NORMALISE = "generate.normalise";
  public static final String GENERATOR_MAX_COUNT = "generate.max.count";
  public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
  public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
  public static final String GENERATOR_COUNT_VALUE_HOST = "host";
  public static final String GENERATOR_COUNT_VALUE_IP = "ip";
  public static final String GENERATOR_TOP_N = "generate.topN";
  public static final String GENERATOR_CUR_TIME = "generate.curTime";
  public static final String GENERATOR_DELAY = "crawl.gen.delay";
  public static final String GENERATOR_RANDOM_SEED = "generate.partition.seed";
  public static final String BATCH_ID = "generate.batch.id";
  public static final String GENERATE_COUNT = "generate.count";

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
      //FETCH_TIME(2, "fetchTime")
    FIELDS.add(WebPage.Field.FETCH_TIME);
      //SCORE(16, "score")
    FIELDS.add(WebPage.Field.SCORE);
      //STATUS(1, "status")
    FIELDS.add(WebPage.Field.STATUS);
      //MARKERS(21, "markers")
    FIELDS.add(WebPage.Field.MARKERS);
  }

  public static final Logger LOG = LoggerFactory.getLogger(GeneratorJob.class);

    /**
     * SelectorEntry做为Map阶段输出的Key,那么必须要实现WritableComparable接口
     * */
  public static class SelectorEntry implements WritableComparable<SelectorEntry> {

    String url;
    float score;

    public SelectorEntry() {
    }

    public SelectorEntry(String url, float score) {
      this.url = url;
      this.score = score;
    }
    /**
     * readFields 反序列化函数
    * */
    public void readFields(DataInput in) throws IOException {
      url = Text.readString(in);
      score = in.readFloat();
    }
      /**
       * write 序列化函数
       * */

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      out.writeFloat(score);
    }
      /**
       * 比较和排序规则的实现
       * 首先按网页分值排序，分值高的靠前；然后再按照url的字典序排序。
       */
    public int compareTo(SelectorEntry se) {
      if (se.score > score)
        return 1;
      else if (se.score == score)
        return url.compareTo(se.url);
      return -1;
    }
      /**
       * 依靠hashCode和equals判断两个对象是否相同，用于去重的目的。
       */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + url.hashCode();
      result = prime * result + Float.floatToIntBits(score);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      SelectorEntry other = (SelectorEntry) obj;
      if (!url.equals(other.url))
        return false;
      if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
        return false;
      return true;
    }




    /**
     * Sets url with score on this writable. Allows for writable reusing.
     * 
     * @param url
     * @param score
     */
    public void set(String url, float score) {
      this.url = url;
      this.score = score;
    }
  }

  public static class SelectorEntryComparator extends WritableComparator {
    public SelectorEntryComparator() {
      super(SelectorEntry.class, true);
    }
  }

  static {

    WritableComparator.define(SelectorEntry.class, new SelectorEntryComparator());

  }

  public GeneratorJob() {

  }

  public GeneratorJob(Configuration conf) {
    setConf(conf);
  }

  public Collection<WebPage.Field> getFields(Job job) {

      Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
      fields.addAll(FetchScheduleFactory.getFetchSchedule(job.getConfiguration()).getFields());
      for (Iterator iter = fields.iterator(); iter.hasNext();) {
          String str = (String)iter.next();
          System.out.println(str);
      }

      return fields;
  }





  public Map<String, Object> run(Map<String, Object> args) throws Exception {

      String batchId = (String) args.get(Nutch.ARG_BATCH);

      if (batchId != null) {
          getConf().set(GeneratorJob.BATCH_ID, batchId);
      }

      // map to inverted subset due for fetch, sort by score
      Long topN = (Long) args.get(Nutch.ARG_TOPN);

      Long curTime = (Long) args.get(Nutch.ARG_CURTIME);
      if (curTime == null) {
          curTime = System.currentTimeMillis();
      }

      Boolean filter = (Boolean) args.get(Nutch.ARG_FILTER);
      Boolean norm = (Boolean) args.get(Nutch.ARG_NORMALIZE);

      // 将topN,curTime,filter,norm等变量赋值给Configration.
      // map to inverted subset due for fetch, sort by score
      getConf().setLong(GENERATOR_CUR_TIME, curTime);

      if (topN != null)
          getConf().setLong(GENERATOR_TOP_N, topN);
      if (filter != null)
          getConf().setBoolean(GENERATOR_FILTER, filter);

      //Nutch.GENERATE_TIME_KEY?好像是generateJob的开始时间
      getConf().setLong(Nutch.GENERATE_TIME_KEY, System.currentTimeMillis());

      if (norm != null)
          getConf().setBoolean(GENERATOR_NORMALISE, norm);

      //得到GENERATOR_COUNT_MODE,如果没有得到,则mode为默认值GENERATOR_COUNT_VALUE_HOST(即host)
      String mode = getConf().get(GENERATOR_COUNT_MODE, GENERATOR_COUNT_VALUE_HOST);
      System.out.println("mode:"+"\t"+mode);//输出为host

      if (GENERATOR_COUNT_VALUE_HOST.equalsIgnoreCase(mode)) {
          getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
      } else if (GENERATOR_COUNT_VALUE_DOMAIN.equalsIgnoreCase(mode)) {
          getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_DOMAIN);
      } else {
          LOG.warn("Unknown generator.max.count mode '" + mode + "', using mode=" + GENERATOR_COUNT_VALUE_HOST);
          getConf().set(GENERATOR_COUNT_MODE, GENERATOR_COUNT_VALUE_HOST);
          getConf().set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
      }

      numJobs = 1;
      currentJobNum = 0;


      currentJob = new NutchJob(getConf(), "generate: " + getConf().get(BATCH_ID));
      //WebPage.Field是Enum类型,这里的fields是做什么用的?
      /**
       * 经测试得,fields里含有下面这个元素.
         markers
         status
         fetchInterval
         fetchTime
         retriesSinceFetch
          score
       * */
      Collection<WebPage.Field> fields = getFields(currentJob);
      /**
       * Map阶段输出的<K,V> = <SelectorEntry.class,WebPage.class>
       * Map阶段的Mapper为:GeneratorMapper.class->GoraMapper<String, WebPage, SelectorEntry, WebPage>
       * Map阶段的Partitioner为:SelectorEntryPartitioner.class
       * 最后参数为:boolean reuseObjects?
       * */
      StorageUtils.initMapperJob(currentJob, fields, SelectorEntry.class, WebPage.class, GeneratorMapper.class, SelectorEntryPartitioner.class, true);
      /**
       * GeneratorReducer.class -> GoraReducer<SelectorEntry, WebPage, String, WebPage>
       * */
      StorageUtils.initReducerJob(currentJob, GeneratorReducer.class);
      currentJob.waitForCompletion(true);
      /**
       * 其中的results是NutchTool中的成员变量:
       * protected HashMap <String, Object> results = new HashMap<String, Object>();
       * */
      ToolUtil.recordJobStatus(null, currentJob, results);
      results.put(BATCH_ID, getConf().get(BATCH_ID));
      long generateCount = currentJob.getCounters().findCounter("Generator", "GENERATE_MARK").getValue();
      results.put(GENERATE_COUNT, generateCount);
      return results;
  }




  /**
   * Mark URLs ready for fetching.
   * 
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * */
  public String generate(long topN, long curTime, boolean filter, boolean norm) throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("GeneratorJob: starting at " + sdf.format(start));
    LOG.info("GeneratorJob: Selecting best-scoring urls due for fetch.");
    LOG.info("GeneratorJob: starting");
    LOG.info("GeneratorJob: filtering: " + filter);
    LOG.info("GeneratorJob: normalizing: " + norm);
    if (topN != Long.MAX_VALUE) {
      LOG.info("GeneratorJob: topN: " + topN);
    }
    Map<String, Object> results = run(ToolUtil.toArgMap(Nutch.ARG_TOPN, topN, Nutch.ARG_CURTIME, curTime, Nutch.ARG_FILTER, filter, Nutch.ARG_NORMALIZE, norm));

    String batchId = getConf().get(BATCH_ID);
      System.out.println("batchId"+batchId);

    long finish = System.currentTimeMillis();
    long generateCount = (Long) results.get(GENERATE_COUNT);
      System.out.println("generateCount"+"\n"+generateCount);
    LOG.info("GeneratorJob: finished at " + sdf.format(finish) + ", time elapsed: " + TimingUtil.elapsedTime(start, finish));
    LOG.info("GeneratorJob: generated batch id: " + batchId + " containing " + generateCount + " URLs");
    if (generateCount == 0) {
      return null;
    }
    return batchId;
  }

  public int run(String[] args) throws Exception {
    if (args.length <= 0) {
      System.out.println("Usage: GeneratorJob [-topN N] [-crawlId id] [-noFilter] [-noNorm] [-adddays numDays]");
      System.out.println("    -topN <N>      - number of top URLs to be selected, default is Long.MAX_VALUE ");
      System.out.println("    -crawlId <id>  - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\");");
      System.out.println("    -noFilter      - do not activate the filter plugin to filter the url, default is true ");
      System.out.println("    -noNorm        - do not activate the normalizer plugin to normalize the url, default is true ");
      System.out.println("    -adddays       - Adds numDays to the current time to facilitate crawling urls already");
      System.out.println("                     fetched sooner then db.fetch.interval.default. Default value is 0.");
      System.out.println("    -batchId       - the batch id ");
      System.out.println("----------------------");
      System.out.println("Please set the params.");
      return -1;
    }

    long curTime = System.currentTimeMillis(), topN = Long.MAX_VALUE;
      //topN 默认为2^63-1
      System.out.println(topN);
      //默认不过滤,不规则化
    boolean filter = true, norm = true;

    // generate batchId
    int randomSeed = Math.abs(new Random().nextInt());
    String batchId = (curTime / 1000) + "-" + randomSeed;
    getConf().set(BATCH_ID, batchId);

    for (int i = 0; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[++i]);
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      } else if ("-noNorm".equals(args[i])) {
        norm = false;
      } else if ("-crawlId".equals(args[i])) {
        getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[++i]);
        curTime += numDays * 1000L * 60 * 60 * 24;
      } else if ("-batchId".equals(args[i]))
        getConf().set(BATCH_ID, args[++i]);
      else {
        System.err.println("Unrecognized arg " + args[i]);
        return -1;
      }
    }

    try {
      return (generate(topN, curTime, filter, norm) != null) ? 0 : 1;
    } catch (Exception e) {
      LOG.error("GeneratorJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String args[]) throws Exception {
      for (int i=0;i<args.length;i++){
          System.out.println(args[i]);
      }
    int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorJob(), args);
    System.exit(res);
  }

}
