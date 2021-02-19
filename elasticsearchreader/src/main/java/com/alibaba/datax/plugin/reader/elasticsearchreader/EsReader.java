package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.exception.ExceptionTracker;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import io.searchbox.params.SearchType;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author liufei mail:1583409404@qq.com
 * @date 2021-02-19 10:32
 */
@SuppressWarnings(value = {"unchecked"})
public class EsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public void prepare() {
            /*
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            ESClient esClient = new ESClient();
            esClient.createClient(Key.getEndpoints(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));

            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            log.info("index:[{}], type:[{}]", indexName, typeName);
            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (!isIndicesExists) {
                    throw new IOException(String.format("index[%s] not exist", indexName));
                }
            } catch (Exception ex) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_INDEX_NOT_EXISTS, ex.toString());
            }
            esClient.closeRestHighLevelClient();
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();
            List<Object> search = conf.getList(Key.SEARCH_KEY, Object.class);
            for (Object query : search) {
                Configuration clone = conf.clone();
                clone.set(Key.SEARCH_KEY, query);
                configurations.add(clone);
            }
            return configurations;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader job destroy=================");
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;
        ESClient esClient = null;
        private String index;
        private SearchType searchType;
        private String query;
        private String[] includes;
        private String[] excludes;
        private int size;
        private boolean containsId;
        private long timeout;

        @Override
        public void prepare() {
            esClient.createClient(Key.getEndpoints(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            this.esClient = new ESClient();
            this.index = Key.getIndexName(conf);
            this.searchType = Key.getSearchType(conf);
            this.query = Key.getQuery(conf);
            this.includes = Key.getIncludes(conf);
            this.excludes = Key.getExcludes(conf);
            this.size = Key.getSize(conf);
            this.containsId = Key.getContainsId(conf);
            this.timeout = Key.getTimeout(conf);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            PerfTrace.getInstance().addTaskDetails(super.getTaskId(), index);
            //search
            PerfRecord queryPerfRecord = new PerfRecord(super.getTaskGroupId(), super.getTaskId(), PerfRecord.PHASE.SQL_QUERY);
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = jsonToSearchSourceBuilder(query);
            sourceBuilder.trackTotalHits(true);
            sourceBuilder.fetchSource(includes, excludes);
            sourceBuilder.size(size);
            sourceBuilder.timeout(new TimeValue(timeout, TimeUnit.MILLISECONDS));
            // 使用searchAfter需要指定排序规则
            searchRequest.searchType(searchType.toString());
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse;

            queryPerfRecord.start();
            Object[] sortValues = null;
            try {
                searchResponse = esClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
                TotalHits totalHits = searchResponse.getHits().getTotalHits();
                int total = (int) totalHits.value;
                log.info("search total：{}, size: {} ", total, sourceBuilder.size());
                if (total == 0) {
                    return;
                }
                SearchHit[] searchHits = searchResponse.getHits().getHits();

                queryPerfRecord.start();
                this.transportRecords(recordSender, searchHits);
                queryPerfRecord.end();

                if (total <= sourceBuilder.size()) {
                    return;
                }

                SearchHit last = searchHits[searchHits.length - 1];
                sortValues = last.getSortValues();
                log.info("searchAfter is：{} ", Arrays.toString(sortValues));
            } catch (Exception e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            }
            queryPerfRecord.end();

            try {
                while (true) {
                    // 使用searchAfter循环
                    queryPerfRecord.start();
                    log.info("searchAfter is：{} ", Arrays.toString(sortValues));
                    sourceBuilder.searchAfter(sortValues);
                    searchResponse = esClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
                    SearchHit[] searchHits = searchResponse.getHits().getHits();
                    queryPerfRecord.end();

                    queryPerfRecord.start();
                    this.transportRecords(recordSender, searchHits);
                    queryPerfRecord.end();

                    if (searchHits.length == 0) {
                        break;
                    }
                    // 重新复值searchAfter
                    SearchHit last = searchHits[searchHits.length - 1];
                    sortValues = last.getSortValues();
                }
            } catch (IOException e) {
                throw DataXException.asDataXException(ESReaderErrorCode.ES_SEARCH_ERROR, e);
            }
        }

        private SearchSourceBuilder jsonToSearchSourceBuilder(String query) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            if (StringUtils.isNotBlank(query)) {
                log.info("search condition is : {} ", query);
                SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
                try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), null, query)) {
                    searchSourceBuilder.parseXContent(parser);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return searchSourceBuilder;
        }


        private boolean transportRecords(RecordSender recordSender, SearchHit[] searchHits) {
            if (searchHits == null && searchHits.length == 0) {
                return false;
            }
            List<String> sources = Lists.newArrayList();
            for (SearchHit hit : searchHits) {
                sources.add(hit.getSourceAsString());
            }
            Map<String, Object> recordMap = new LinkedHashMap<>();
            for (SearchHit hit : searchHits) {
                if (containsId) {
                    recordMap.put("_id", hit.getId());
                }
                Map<String, Object> parent = JSON.parseObject(hit.getSourceAsString(), LinkedHashMap.class);
                recordMap.putAll(parent);
                this.transportOneRecord(recordSender, recordMap);
                recordMap.clear();
            }
            return sources.size() > 0;
        }


        private void transportOneRecord(RecordSender recordSender, Map<String, Object> recordMap) {
            if (recordMap.entrySet().stream().anyMatch(x -> x.getValue() != null)) {
                Record record = buildRecord(recordSender, recordMap);
                recordSender.sendToWriter(record);
            }
        }

        private Record buildRecord(RecordSender recordSender, Map<String, Object> source) {
            Record record = recordSender.createRecord();
            boolean hasDirty = false;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                try {
                    Object o = source.get(entry.getKey());
                    record.addColumn(getColumn(o));
                } catch (Exception e) {
                    hasDirty = true;
                    sb.append(ExceptionTracker.trace(e));
                }
            }
            if (hasDirty) {
                getTaskPluginCollector().collectDirtyRecord(record, sb.toString());
            }
            return record;
        }

        private Column getColumn(Object value) {
            Column col;
            if (value == null) {
                col = new StringColumn();
            } else if (value instanceof String) {
                col = new StringColumn((String) value);
            } else if (value instanceof Integer) {
                col = new LongColumn(((Integer) value).longValue());
            } else if (value instanceof Long) {
                col = new LongColumn((Long) value);
            } else if (value instanceof Byte) {
                col = new LongColumn(((Byte) value).longValue());
            } else if (value instanceof Short) {
                col = new LongColumn(((Short) value).longValue());
            } else if (value instanceof Double) {
                col = new DoubleColumn(BigDecimal.valueOf((Double) value));
            } else if (value instanceof Float) {
                col = new DoubleColumn(BigDecimal.valueOf(((Float) value).doubleValue()));
            } else if (value instanceof Date) {
                col = new DateColumn((Date) value);
            } else if (value instanceof Boolean) {
                col = new BoolColumn((Boolean) value);
            } else if (value instanceof byte[]) {
                col = new BytesColumn((byte[]) value);
            } else if (value instanceof List) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Map) {
                col = new StringColumn(JSON.toJSONString(value));
            } else if (value instanceof Array) {
                col = new StringColumn(JSON.toJSONString(value));
            } else {
                throw DataXException.asDataXException(ESReaderErrorCode.UNKNOWN_DATA_TYPE, "type:" + value.getClass().getName());
            }
            return col;
        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void destroy() {
            log.info("============elasticsearch reader taskGroup[{}] taskId[{}] destroy=================", super.getTaskGroupId(), super.getTaskId());
            esClient.closeRestHighLevelClient();
        }
    }
}
