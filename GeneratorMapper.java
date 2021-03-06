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

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class GeneratorMapper extends GoraMapper<String, WebPage, SelectorEntry, WebPage> {

    private URLFilters filters;             /**插件*/
    private URLNormalizers normalizers;     /**插件*/
    private boolean filter;                 /**是否过滤*/
    private boolean normalise;              /**是否规范化*/
    private FetchSchedule schedule;         /**还是插件*/
    private ScoringFilters scoringFilters;  /**仍然插件*/
    private long curTime;                   /**Generate的时间*/
    private SelectorEntry entry = new SelectorEntry();
    private int maxDistance;                /**该Url到种子Url最短路径的最大距离*/


    @Override
  public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {

    String url = TableUtil.unreverseUrl(reversedUrl);

    /**
     * 判断WebPage是否已经被generate过,如何已经generate过,Mark.GENERATE_MARK.checkMark(page)的返回值为1450609636-931585743,
     * 如果没有generate过,该方法返回值为null
     * */
    if (Mark.GENERATE_MARK.checkMark(page) != null) {
      GeneratorJob.LOG.debug("Skipping {}; already generated", url);
      return;
    }

    // filter on distance
    if (maxDistance > -1) {                /**-1 if unlimited.???*/
      CharSequence distanceUtf8 = page.getMarkers().get(DbUpdaterJob.DISTANCE);
      if (distanceUtf8 != null) {
        int distance = Integer.parseInt(distanceUtf8.toString());
        if (distance > maxDistance) {      /**距离种子页面太远（i.e.太不相关了）*/
          return;
        }
      }
    }

    // If filtering is on don't generate URLs that don't pass URLFilters
    try {
      if (normalise) {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      }
      if (filter && filters.filter(url) == null)
        return;
    } catch (URLFilterException e) {
      GeneratorJob.LOG.warn("Couldn't filter url: {} ({})", url, e.getMessage());
      return;
    } catch (MalformedURLException e) {
      GeneratorJob.LOG.warn("Couldn't filter url: {} ({})", url, e.getMessage());
      return;
    }
    // check fetch schedule
        /**默认调用org.apache.nutch.crawl.DefaultFetchSchedule*/
    if (!schedule.shouldFetch(url, page, curTime)) {//对于不应该抓取的页面
      if (GeneratorJob.LOG.isDebugEnabled()) {
        GeneratorJob.LOG.debug("-shouldFetch rejected '" + url + "', fetchTime=" + page.getFetchTime() + ", curTime=" + curTime);
      }
      return;
    }
    float score = page.getScore();
    try {
        /**
         * 该方法为排序并且选择分值Top N的页面得出一个排序的分值???
         */
      score = scoringFilters.generatorSortValue(url, page, score);
    } catch (ScoringFilterException e) {
      // ignore
    }
    entry.set(url, score);
    context.write(entry, page);
  }

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
    normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
    if (filter) {
      filters = new URLFilters(conf);
    }
    if (normalise) {
      normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
    }
    maxDistance = conf.getInt("generate.max.distance", -1);
    curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    scoringFilters = new ScoringFilters(conf);
  }
}
