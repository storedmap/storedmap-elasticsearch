/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vsetec.storedmap.elasticsearch;

import com.vsetec.storedmap.Driver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class ElasticsearchDriver implements Driver<RestHighLevelClient> {

    @Override
    public RestHighLevelClient openConnection(Properties properties) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(
                properties.getProperty("storedmap.elasticsearch.host", "localhost"),
                Integer.parseInt(properties.getProperty("storedmap.elasticsearch.port", "9200")),
                "http"))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder
                                .setConnectionRequestTimeout(0) // to avoid enigmatic TimeoutException
                                .setConnectTimeout(10000)
                                .setSocketTimeout(180000);
                    }
                })
                .setMaxRetryTimeoutMillis(180000); // TODO: parametrize - move to the config file

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    @Override
    public void closeConnection(RestHighLevelClient client) {
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException("Couldnt't close Elasticsearch client", e);
        }
    }

    @Override
    public int getMaximumIndexNameLength() {
        return 200;
    }

    @Override
    public int getMaximumKeyLength() {
        return 200;
    }

    @Override
    public int getMaximumTagLength() {
        return 200;
    }

    @Override
    public int getMaximumSorterLength() {
        return 200;
    }

    private void _waitForClusterReady(RestClient client) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("wait_for_status", "yellow");
        Response response = client.performRequest("GET", "_cluster/health", params);
        response.getEntity();
    }

    private Map _get(String key, String indexName, RestHighLevelClient client) {
        GetRequest req = Requests.getRequest(indexName).type("doc").id(key);
        GetResponse response;

        while (true) {
            try {
                try {

                    response = client.get(req, RequestOptions.DEFAULT);

                } catch (ElasticsearchStatusException ee) {
                    if (ee.status().getStatus() == RestStatus.NOT_FOUND.getStatus()) {
                        return null;
                    } else {
                        String msg = ee.getMessage();
                        if (msg.contains("no_shard_available")) {
                            //_LOG.warn("Elasticsearch warning: " + ee.getMessage() + ", retrying");
                            _waitForClusterReady(client.getLowLevelClient());
                            continue;
                        }
                        throw new RuntimeException(ee);
                    }
                }
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!response.isExists()) {
            return null;
        }

        BytesReference bytes = response.getSourceAsBytesRef();

        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(bytes, true, Requests.INDEX_CONTENT_TYPE);

        Map ret = tuple.v2();

        return ret;
    }

    @Override
    public byte[] get(String key, String indexName, RestHighLevelClient client) {
        Map map = _get(key, indexName + "_main", client);
        if (map == null) {
            return null;
        }
        String b64 = (String) map.get("value");
        if (b64 == null) {
            return null;
        }
        return Base64.decodeBase64(b64);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, String[] anyOfTags) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int tryLock(String key, String indexName, RestHighLevelClient connection, int milliseconds) {
        long currentTime = System.currentTimeMillis(); // TODO: make universal cluster time
        int millisStillToWait;
        indexName = indexName + "_lock";

        Map map = _get(key, indexName, connection);
        if (map != null) {
            Long lockedUntil = (Long) map.get("lockedUntil");
            if (lockedUntil != null) {
                millisStillToWait = (int) (lockedUntil - currentTime);
            } else {
                millisStillToWait = 0;
            }
        } else {
            millisStillToWait = 0;
        }

        // write lock time if we are not waiting anymore
        if (millisStillToWait <= 0) {
            map = new HashMap(2);
            map.put("lockedUntil", currentTime + milliseconds);
            IndexRequest put = Requests.indexRequest(indexName).type("doc").id(key).source(map);
            try {
                connection.index(put, RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return millisStillToWait;
    }

    @Override
    public void unlock(String key, String indexName, RestHighLevelClient connection) {
        DeleteRequest req = Requests.deleteRequest(indexName + "_lock").type("doc").id(key);
        try {
            connection.delete(req, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String key, String indexName, RestHighLevelClient connection, byte[] value, Runnable callbackOnIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void put(String key, String indexName, RestHighLevelClient connection, Map<String, Object> map, Locale[] locales, byte[] sorter, String[] tags, Runnable callbackOnAdditionalIndex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void remove(String key, String indexName, RestHighLevelClient connection, Runnable callback) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}