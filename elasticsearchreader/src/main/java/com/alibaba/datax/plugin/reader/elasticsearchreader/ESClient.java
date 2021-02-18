package com.alibaba.datax.plugin.reader.elasticsearchreader;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author liufei
 * @date 2020-04-14 10:32
 */
public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private RestHighLevelClient client = null;

    public RestHighLevelClient createClient(String host, int port){
        client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(host, port, "http")
        ));
        log.info("======= RestHighLevelClient 初始化成功 =======");
        return client;
    }

    public RestHighLevelClient getClient() {
        return client;
    }


    public boolean indicesExists(String indexName) throws Exception {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 关闭RestHighLevelClient客户端
     */
    public void closeRestHighLevelClient() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.error("close RestHighLevelClient is error: {}", e.getMessage());
            }
        }
    }
}
