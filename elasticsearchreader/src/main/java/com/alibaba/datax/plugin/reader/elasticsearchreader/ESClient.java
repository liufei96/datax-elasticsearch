package com.alibaba.datax.plugin.reader.elasticsearchreader;


import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

    public RestHighLevelClient createClient(String endpoints, String username, String password){
        // 账号密码认证
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }

        String[] endpointSplit = endpoints.split(",");
        HttpHost[] hosts = new HttpHost[endpointSplit.length];
        for (int i = 0; i < endpointSplit.length; i++) {
            String[] ips = endpointSplit[0].split(":");
            String ip = ips[0];
            int port = Integer.parseInt(ips[1]);
            hosts[i] = new HttpHost(ip, port, HttpHost.DEFAULT_SCHEME_NAME);
        }
        client =  new RestHighLevelClient(RestClient.builder(hosts));
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
