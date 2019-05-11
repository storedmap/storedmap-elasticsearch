/*
 * Copyright 2018 Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.storedmap.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.storedmap.Driver;

/**
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class ElasticsearchDriver implements Driver<RestHighLevelClient> {

    private final HashMap<RestHighLevelClient, BulkProcessor> _bulkers = new HashMap<>(4);
    private final Base32 _b32 = new Base32(true);
    private final HashMap<RestHighLevelClient, ExecutorService> _unlockers = new HashMap<>(4);
    private final int _maxSorterLength;

    {
        // TODO: review this hacky way to know the longest sorter length
        byte[] longestChars = new byte[200];
        for (int i = 0; i < longestChars.length; i++) {
            longestChars[i] = 'a';
        }

        _maxSorterLength = _b32.decode(longestChars).length;
    }

    @Override
    public RestHighLevelClient openConnection(Properties properties) {

        RestClientBuilder builder = RestClient.builder(new HttpHost(
                properties.getProperty("elasticsearch.host", "localhost"),
                Integer.parseInt(properties.getProperty("elasticsearch.port", "9200")),
                "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                        return hacb
                                .setMaxConnTotal(750)
                                .setMaxConnPerRoute(750);
                    }
                })
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
                for (Object r : request.payloads()) {
                    Runnable[] callbacks = (Runnable[]) r;
                    if (callbacks[0] != null) {
                        _unlockers.get(client).submit(callbacks[0]);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    for (BulkItemResponse bir : response.getItems()) {
                        if (bir.isFailed()) {
                            if (!bir.getOpType().equals(DocWriteRequest.OpType.DELETE)) {
                                System.out.println("ITEM Failure::: " + bir.getFailureMessage());
                            }
                        }
                    }
                }

                for (Object r : request.payloads()) {
                    Runnable[] callbacks = (Runnable[]) r;
                    if (callbacks[1] != null) {
                        _unlockers.get(client).submit(callbacks[1]);
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                // TODO: do something useful with errors 
                List<Object> payloads = request.payloads();
                System.out.println("Failure " + failure.toString() + " afterbulk with " + payloads.toString());
                for (Object r : request.payloads()) {
                    Runnable[] callbacks = (Runnable[]) r;
                    if (callbacks[1] != null) {
                        _unlockers.get(client).submit(callbacks[1]);
                    }
                }
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer
                = (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        BulkProcessor bulker = BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
                .setConcurrentRequests(500)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), Integer.MAX_VALUE))
                .build();

        synchronized (_bulkers) {
            _bulkers.put(client, bulker);
            //_unlockers.put(client, Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "ElasticsearchCallback")));

            ExecutorService es = new ThreadPoolExecutor(20, Integer.MAX_VALUE, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(100), new ThreadFactory() {
                private int _num = 0;

                @Override
                public Thread newThread(Runnable r) {
                    _num++;
                    return new Thread(r, "StoredMapESCallback-" + _num);
                }
            });

            _unlockers.put(client, es);
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

            ExecutorService unlocker = _unlockers.remove(client);
            try {
                unlocker.shutdown();
                unlocker.awaitTermination(3, TimeUnit.MINUTES);
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
    public int getMaximumIndexNameLength(RestHighLevelClient client) {
        return 200;
    }

    @Override
    public int getMaximumKeyLength(RestHighLevelClient client) {
        return 200;
    }

    @Override
    public int getMaximumTagLength(RestHighLevelClient client) {
        return 200;
    }

    @Override
    public int getMaximumSorterLength(RestHighLevelClient client) {
        return _maxSorterLength;
    }

    private void _waitForClusterReady(RestClient client) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("wait_for_status", "yellow");
        Response response = client.performRequest("GET", "_cluster/health", params);
        System.out.println("Waiting for yellow");
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

//        BytesReference bytes = response.getSourceAsBytesRef();
//
//        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(bytes, true, Requests.INDEX_CONTENT_TYPE);
//
//        Map ret = tuple.v2();
        Map ret = response.getSourceAsMap();

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
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String secondaryKey, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, Boolean ascending, String textQuery) {
        QueryBuilder query1 = anyOfTags == null ? null : QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        RangeQueryBuilder query2;
        if (minSorter != null || maxSorter != null) {
            query2 = QueryBuilders.rangeQuery("sorter.keyword");
            if (minSorter != null) {
                query2 = query2.gte(_b32.encodeAsString(minSorter));
            }
            if (maxSorter != null) {
                query2 = query2.lt(_b32.encodeAsString(maxSorter));
            }
        } else {
            query2 = null;
        }
        QueryBuilder query3 = textQuery == null ? null : QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query4 = secondaryKey == null ? null : QueryBuilders.termQuery("sec.keyword", secondaryKey);
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        if (query1 != null) {
            query = query.must(query1);
        }
        if (query2 != null) {
            query = query.must(query2);
        }
        if (query3 != null) {
            query = query.must(query3);
        }
        if (query4 != null) {
            query = query.must(query4);
        }

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);
        if (ascending != null) {
            source.sort("sorter.keyword", ascending ? SortOrder.ASC : SortOrder.DESC);
        }
        return new Ids(connection, indexName + "_indx", source, true);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, int from, int size) {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query).from(from).size(size);
        return new Ids(connection, indexName + "_main", source, false);
    }

    @Override
    public Iterable<String> get(String indexName, RestHighLevelClient connection, String secondaryKey, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, Boolean ascending, String textQuery, int from, int size) {
        QueryBuilder query1 = anyOfTags == null ? null : QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        RangeQueryBuilder query2;
        if (minSorter != null || maxSorter != null) {
            query2 = QueryBuilders.rangeQuery("sorter.keyword");
            if (minSorter != null) {
                query2 = query2.gte(_b32.encodeAsString(minSorter));
            }
            if (maxSorter != null) {
                query2 = query2.lt(_b32.encodeAsString(maxSorter));
            }
        } else {
            query2 = null;
        }
        QueryBuilder query3 = textQuery == null ? null : QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query4 = secondaryKey == null ? null : QueryBuilders.termQuery("sec.keyword", secondaryKey);
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        if (query1 != null) {
            query = query.must(query1);
        }
        if (query2 != null) {
            query = query.must(query2);
        }
        if (query3 != null) {
            query = query.must(query3);
        }
        if (query4 != null) {
            query = query.must(query4);
        }
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query).from(from).size(size);
        if (ascending != null) {
            source.sort("sorter.keyword", ascending ? SortOrder.ASC : SortOrder.DESC);
        }
        return new Ids(connection, indexName + "_indx", source, false);
    }

    @Override
    public Lock tryLock(String key, String indexName, RestHighLevelClient connection, int milliseconds, String sessionId) {
        long currentTime = System.currentTimeMillis(); // TODO: make universal cluster time
        final int millisStillToWait;
        final String sessionInDb;
        indexName = indexName + "_lock";

        Map map = _get(key, indexName, connection);
        if (map != null) {
            Long lockedUntil = (Long) map.get("lockedUntil");
            sessionInDb = (String) map.get("sessionId");
            //System.out.println(indexName + ", "+ key +" locked until = " + lockedUntil);
            if (lockedUntil != null) {
                millisStillToWait = (int) (lockedUntil - currentTime);
            } else {
                millisStillToWait = 0;
            }
        } else {
            millisStillToWait = 0;
            sessionInDb = sessionId;
        }

        // write lock time if we are not waiting anymore
        if (millisStillToWait <= 0) {
            map = new HashMap(2);
            map.put("lockedUntil", currentTime + milliseconds);
            map.put("sessionId", sessionInDb);
            IndexRequest put = Requests.indexRequest(indexName).type("doc").id(key).source(map);
            try {

                boolean success = false;
                do {
                    try {
                        connection.index(put, RequestOptions.DEFAULT);
                        success = true;
                    } catch (ElasticsearchStatusException ee) {
                        if (ee.status().getStatus() == RestStatus.TOO_MANY_REQUESTS.getStatus()) {
                            System.out.println("Elasticsearch error: " + ee.getMessage() + ", retrying after wait");
                            synchronized (ee) {
                                try {
                                    ee.wait(10);
                                } catch (InterruptedException eee) {
                                    throw new RuntimeException("Unexpected interruption", eee);
                                }
                            }
                            success = false;
                        }
                    }
                } while (!success);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return new Lock() {
            @Override
            public int getWaitTime() {
                return millisStillToWait;
            }

            @Override
            public String getLockerSession() {
                return sessionInDb;
            }
        };
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
    public void put(String key, String indexName, RestHighLevelClient connection, byte[] value, Runnable callbackBeforeIndex, Runnable callbackAfterIndex) {
        String data = Base64.encodeBase64String(value);
        IndexRequest req = Requests.indexRequest(indexName + "_main").type("doc").id(key).source("value", data);
        _bulkers.get(connection).add(req, new Runnable[]{callbackBeforeIndex, callbackAfterIndex});
    }

    @Override
    public void put(String key, String indexName, RestHighLevelClient connection, Map<String, Object> map, Locale[] locales, String secondaryKey, byte[] sorter, String[] tags, Runnable callbackOnAdditionalIndex) {
        Map<String, Object> data = new HashMap<>(map);
        data.put("sorter", _b32.encodeAsString(sorter));
        data.put("tags", tags);
        data.put("sec", secondaryKey);
        IndexRequest req = Requests.indexRequest(indexName + "_indx").type("doc").id(key).source(data);
        _bulkers.get(connection).add(req, new Runnable[]{null, callbackOnAdditionalIndex});
    }

    @Override
    public void remove(String key, String indexName, RestHighLevelClient connection, Runnable callback) {
        DeleteRequest req1 = Requests.deleteRequest(indexName + "_main").type("doc").id(key);
        DeleteRequest req2 = Requests.deleteRequest(indexName + "_indx").type("doc").id(key);
        _bulkers.get(connection).add(req1, new Runnable[]{null, null});
        _bulkers.get(connection).add(req2, new Runnable[]{null, callback});
    }

    @Override
    public void removeAll(String indexName, RestHighLevelClient client) {
        DeleteIndexRequest request1 = new DeleteIndexRequest(indexName + "_indx");
        DeleteIndexRequest request2 = new DeleteIndexRequest(indexName + "_main");

        try {
            client.indices().delete(request1, RequestOptions.DEFAULT);
            client.indices().delete(request2, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterable<String> getIndices(RestHighLevelClient connection) {
        try {
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = connection.cluster().health(request, RequestOptions.DEFAULT);
            Set<String> indices = response.getIndices().keySet();
            Set<String> tables = new HashSet<>();
            for (String indexCandidate : indices) {
                int strPos = -1;
                if ((strPos = indexCandidate.indexOf("_main")) > 0) {
                    indexCandidate = indexCandidate.substring(0, strPos);
                } else if ((strPos = indexCandidate.indexOf("_lock")) > 0) {
                    indexCandidate = indexCandidate.substring(0, strPos);
                } else if ((strPos = indexCandidate.indexOf("_indx")) > 0) {
                    indexCandidate = indexCandidate.substring(0, strPos);
                }
                if (strPos > 0) {
                    tables.add(indexCandidate);
                }
            }
            return tables;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count(String indexName, RestHighLevelClient connection) {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);

        source = source.size(0);

        SearchRequest sr = new SearchRequest(indexName + "_indx");
        sr.source(source);
        try {
            SearchResponse response = connection.search(sr, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            return hits.getTotalHits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count(String indexName, RestHighLevelClient connection, String secondaryKey, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, String textQuery) {
        QueryBuilder query1 = anyOfTags == null ? null : QueryBuilders.termsQuery("tags.keyword", anyOfTags);
        RangeQueryBuilder query2;
        if (minSorter != null || maxSorter != null) {
            query2 = QueryBuilders.rangeQuery("sorter.keyword");
            if (minSorter != null) {
                query2 = query2.gte(_b32.encodeAsString(minSorter));
            }
            if (maxSorter != null) {
                query2 = query2.lt(_b32.encodeAsString(maxSorter));
            }
        } else {
            query2 = null;
        }
        QueryBuilder query3 = textQuery == null ? null : QueryBuilders.wrapperQuery(textQuery);
        QueryBuilder query4 = secondaryKey == null ? null : QueryBuilders.termQuery("sec.keyword", secondaryKey);
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        if (query1 != null) {
            query = query.must(query1);
        }
        if (query2 != null) {
            query = query.must(query2);
        }
        if (query3 != null) {
            query = query.must(query3);
        }
        if (query4 != null) {
            query = query.must(query4);
        }
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(query);

        source = source.size(0);

        SearchRequest sr = new SearchRequest(indexName + "_indx");
        sr.source(source);
        try {
            SearchResponse response = connection.search(sr, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            return hits.getTotalHits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
