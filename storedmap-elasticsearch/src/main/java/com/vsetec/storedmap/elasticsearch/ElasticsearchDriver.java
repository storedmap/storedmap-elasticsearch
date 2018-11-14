/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vsetec.storedmap.elasticsearch;

import com.vsetec.storedmap.Driver;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class ElasticsearchDriver implements Driver<RestHighLevelClient> {

    private final HashMap<RestHighLevelClient, BulkProcessor> _bulkers = new HashMap<>(4);
    private final Base32 _b32 = new Base32(true);

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

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                List<Object> payloads = request.payloads();
                System.out.println("afterbulk with " + payloads.toString());
                for (Object r : request.payloads()) {
                    Runnable callback = (Runnable) r;
                    callback.run();
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                // TODO: do something useful with errors 
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer
                = (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor bulker = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(500)
                .setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
                .setConcurrentRequests(1)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                .build();

        synchronized (_bulkers) {
            _bulkers.put(client, bulker);
        }

        return client;
    }

    @Override
    public void closeConnection(RestHighLevelClient client) {
        synchronized (_bulkers) {
            BulkProcessor bulker = _bulkers.remove(client);
            try {
                bulker.awaitClose(3, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException("Unexpected interruption", e);
            }
        }

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
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        return new Ids(connection, indexName + "_main", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String[] anyOfTags) {
        QueryBuilder query = QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        QueryBuilder query = QueryBuilders.rangeQuery("sorter")
                .from(minSorter == null ? null : _b32.encodeAsString(minSorter)).includeLower(true)
                .to(maxSorter == null ? null : _b32.encodeAsString(maxSorter)).includeUpper(true);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        source.sort("sorter", ascending ? SortOrder.ASC : SortOrder.DESC);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery) {
        QueryBuilder query = QueryBuilders.wrapperQuery(textQuery);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        QueryBuilder query1 = QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        QueryBuilder query2 = QueryBuilders.rangeQuery("sorter")
                .from(minSorter == null ? null : _b32.encodeAsString(minSorter)).includeLower(true)
                .to(maxSorter == null ? null : _b32.encodeAsString(maxSorter)).includeUpper(true);
        QueryBuilder query = QueryBuilders.boolQuery().must(query1).must(query2);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        source.sort("sorter", ascending ? SortOrder.ASC : SortOrder.DESC);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, String[] anyOfTags) {
        QueryBuilder query1 = QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        QueryBuilder query2 = QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query = QueryBuilders.boolQuery().must(query1).must(query2);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        QueryBuilder query1 = QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        QueryBuilder query2 = QueryBuilders.rangeQuery("sorter")
                .from(minSorter == null ? null : _b32.encodeAsString(minSorter)).includeLower(true)
                .to(maxSorter == null ? null : _b32.encodeAsString(maxSorter)).includeUpper(true);
        QueryBuilder query3 = QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query = QueryBuilders.boolQuery().must(query1).must(query2).must(query3);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        source.sort("sorter", ascending ? SortOrder.ASC : SortOrder.DESC);
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        QueryBuilder query2 = QueryBuilders.rangeQuery("sorter")
                .from(minSorter == null ? null : _b32.encodeAsString(minSorter)).includeLower(true)
                .to(maxSorter == null ? null : _b32.encodeAsString(maxSorter)).includeUpper(true);
        QueryBuilder query3 = QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query = QueryBuilders.boolQuery().must(query2).must(query3);
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        source.sort("sorter", ascending ? SortOrder.ASC : SortOrder.DESC);
        return new Ids(connection, indexName + "_indx", source, true);
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
                System.out.println("locked " + indexName + " key " + key);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return millisStillToWait;
    }

    @Override
    public void unlock(String key, String indexName, RestHighLevelClient connection) {
        System.out.println("UNlockING " + indexName + "_lock key " + key);
        DeleteRequest req = Requests.deleteRequest(indexName + "_lock").type("doc").id(key);
        try {
            connection.delete(req, RequestOptions.DEFAULT);
            System.out.println("UNlocked " + indexName + "_lock key " + key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String key, String indexName, RestHighLevelClient connection, byte[] value, Runnable callbackOnIndex) {
        String data = Base64.encodeBase64String(value);
        IndexRequest req = Requests.indexRequest(indexName + "_main").type("doc").id(key).source("value", data);
        _bulkers.get(connection).add(req, callbackOnIndex);
    }

    @Override
    public void put(String key, String indexName, RestHighLevelClient connection, Map<String, Object> map, Locale[] locales, byte[] sorter, String[] tags, Runnable callbackOnAdditionalIndex) {
        Map<String, Object> data = new HashMap<>(map);
        data.put("sorter", _b32.encodeAsString(sorter));
        data.put("tags", tags);
        IndexRequest req = Requests.indexRequest(indexName + "_indx").type("doc").id(key).source(data);
        _bulkers.get(connection).add(req, callbackOnAdditionalIndex);
    }

    @Override
    public void remove(String key, String indexName, RestHighLevelClient connection, Runnable callback) {
        DeleteRequest req1 = Requests.deleteRequest(indexName + "_main").type("doc").id(key);
        DeleteRequest req2 = Requests.deleteRequest(indexName + "_indx").type("doc").id(key);
        _bulkers.get(connection).add(req1, new Runnable() {
            @Override
            public void run() {
            }
        });
        _bulkers.get(connection).add(req2, callback);
    }

}
