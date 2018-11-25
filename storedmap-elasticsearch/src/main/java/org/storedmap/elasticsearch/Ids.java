/*
 * Copyright 2018 Fyodor Kravchenko <fedd@vsetec.com>.
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Ids implements Iterable<String> {

    private final static int SCROLLIFMORE = 100;

    final private Iterator<String> _i;

    Ids(RestHighLevelClient client, String index, SearchSourceBuilder sourceBuilder, boolean infinite) {
        try {
            SearchRequest sr = new SearchRequest(index);
            if (infinite) {
                sourceBuilder.size(SCROLLIFMORE + 1);
            }
            sr.source(sourceBuilder);

            final SearchHit[] hits;
            SearchResponse response;

            try {
                response = client.search(sr, RequestOptions.DEFAULT);

            } catch (ElasticsearchStatusException ee) {
                if (ee.status().getStatus() == RestStatus.NOT_FOUND.getStatus()) {
                    response = null;
                } else {
                    throw new RuntimeException(ee);
                }
            }
            if (response != null) {
                hits = response.getHits().getHits();
            } else {
                hits = new SearchHit[0];
            }

            if (hits.length <= SCROLLIFMORE || !infinite) { // if too few results or we are not infinite

                _i = new Iterator<String>() {

                    private int _pos = 0;

                    @Override
                    public boolean hasNext() {
                        return _pos < hits.length;
                    }

                    @Override
                    public String next() {
                        if (_pos < hits.length) {
                            String ret = hits[_pos].getId();
                            _pos++;
                            return ret;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                };
            } else { // if too many results and we required infinite scroll

                sr = new SearchRequest(index);
                sourceBuilder.size(SCROLLIFMORE);
                sr.source(sourceBuilder);

                sr.scroll(TimeValue.timeValueMinutes(1));
                response = client.search(sr, RequestOptions.DEFAULT);
                final String scrollId = response.getScrollId();
                final SearchHit[] scrollHits = response.getHits().getHits();

                _i = new Iterator<String>() {

                    private String _scrollId = scrollId;
                    private int _pos = 0;
                    private SearchHit[] _hits = scrollHits;
                    private boolean _endReached = false;

                    private void _scroll() {
                        if (_pos >= _hits.length && !_endReached) {
                            try {
                                SearchScrollRequest scrollRequest = new SearchScrollRequest(_scrollId);
                                scrollRequest.scroll(TimeValue.timeValueSeconds(30));
                                SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                                _scrollId = searchScrollResponse.getScrollId();
                                SearchHits hits = searchScrollResponse.getHits();
                                if (hits != null) {
                                    _hits = hits.getHits();
                                } else {
                                    _hits = new SearchHit[0];
                                }
                                _pos = 0;
                                _endReached = _hits.length == 0;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        _scroll();
                        return _pos < _hits.length;
                    }

                    @Override
                    public String next() {
                        _scroll();
                        if (_pos < hits.length) {
                            String ret = _hits[_pos].getId();
                            _pos++;
                            return ret;
                        } else {
                            throw new NoSuchElementException();
                        }
                    }
                };
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Iterator<String> iterator() {
        return _i;
    }

}
