package cache.dataimportes.util;

import cache.dataimportes.holders.SearchResult;
import cache.dataimportes.holders.Strand;
import cache.dataimportes.model.Bartelpolyareads;
import cache.dataimportes.model.Indices;
import cache.dataimportes.model.Polyareads;
import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.model.Tssreads;
import static cache.dataimportes.util.DataAccess.BARTELPOLYAREADSMAPPER;
import static cache.dataimportes.util.DataAccess.INDICESMAPPER;
import static cache.dataimportes.util.DataAccess.POLYAREADSMAPPER;
import static cache.dataimportes.util.DataAccess.READSMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTMAPPER;
import static cache.dataimportes.util.DataAccess.TSSREADSMAPPER;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataAccessES {

    public static final TransportClient CLIENT = createClient();

    public static final SearchRequestBuilder TRANSCRIPTSEARCHBUILDER = createSearchBuilder();
    public static final SearchRequestBuilder SYNONYMSEARCHBUILDER = createSynonymBuilder();

    static TransportClient createClient() {
        TransportClient client = null;
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "cache")
                    .put("client.transport.sniff", true).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException ex) {
            Logger.getLogger(DataAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
        return client;
    }

    static SearchRequestBuilder createSearchBuilder() {
        SearchRequestBuilder srb;
        srb = CLIENT.prepareSearch("index_transcripts_101")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes("record")
                .storedFields("transcriptid", "name")
                .setRouting("1")
                .setExplain(false);
        return srb;
    }

    static SearchRequestBuilder createSynonymBuilder() {
        SearchRequestBuilder srb;
        srb = CLIENT.prepareSearch("index_synonyms")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes("record")
                .storedFields("transcriptid", "synonym")
                .setRouting("1")
                .setExplain(false);
        return srb;
    }

    public static void deleteTranscript(Long transcriptId) {
        BulkByScrollResponse response
                = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_101")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_100")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_97")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_76")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_75")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_65")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_50")
                .get();

        response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_40")
                .get();
    }

    public static void indexTranscript(Long transcriptId, String name, String sequence) {
        try {
            System.out.println("line 142");
            Map<String, Object> json101 = new HashMap<>();
            json101.put("transcriptid", transcriptId);
            json101.put("suggest", name);
            json101.put("name", name);
            json101.put("sequence", sequence);

            Map<String, Object> json = new HashMap<>();
            json.put("transcriptid", transcriptId);
            json.put("name", name);
            json.put("sequence", sequence);

            IndexRequestBuilder irb = CLIENT.prepareIndex("index_transcripts_101", "record")
                    .setSource(json101)
                    .setRouting("1");
            IndexResponse response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_100", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_97", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_76", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_75", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_65", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_50", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();

            irb = CLIENT.prepareIndex("index_transcripts_40", "record")
                    .setSource(json)
                    .setRouting("1");
            response = irb.get();
        } catch (Exception ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void editTranscriptName(Long transcriptId, String newName, String sequence) {

        Script script = new Script("ctx._source.name = \"" + newName + "\"");

        BulkByScrollResponse response
                = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_101")
                .get();

        Map<String, Object> json101 = new HashMap<>();
        json101.put("transcriptid", transcriptId);
        json101.put("suggest", newName);
        json101.put("name", newName);
        json101.put("sequence", sequence);
        IndexRequestBuilder irb = CLIENT.prepareIndex("index_transcripts_101", "record")
                .setSource(json101)
                .setRouting("1");
        irb.get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_100")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_97")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_76")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_75")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_65")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_50")
                .script(script)
                .get();

        response = UpdateByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_transcripts_40")
                .script(script)
                .get();
    }

    public static void editTranscriptSynonyms(Long transcriptId, String newName) {

        try {
            deleteTranscriptSynonyms(transcriptId);

            if (newName != null && !newName.equals("") && !newName.equals("-")) {
                if (newName.contains(",")) {
                    String[] tokens = newName.split(",");
                    for (String tok : tokens) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("transcriptid", transcriptId.toString());
                        json.put("suggest", tok.trim());
                        json.put("synonym", tok.trim());

                        IndexRequestBuilder irb = CLIENT.prepareIndex("index_synonyms", "record")
                                .setSource(json).setRouting("1");
                        irb.get();
                    }
                } else {
                    Map<String, Object> json = new HashMap<>();
                    json.put("transcriptid", transcriptId.toString());
                    json.put("suggest", newName);
                    json.put("synonym", newName);

                    IndexRequestBuilder irb = CLIENT.prepareIndex("index_synonyms", "record")
                            .setSource(json).setRouting("1");
                    irb.get();
                }
            }
        } catch (Exception e) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static void deleteTranscriptSynonyms(Long transcriptId) {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(CLIENT)
                .filter(QueryBuilders.matchQuery("transcriptid", transcriptId.toString()))
                .source("index_synonyms")
                .get();
    }

    public static List<SearchResult> searchReadsExactMatch40ScrollingWithMaxCount(final Long experimentId, int maxCount, String query,
            final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getTertiaryindex();

        try {

            switch (strand) {
                case FORWARD:
                    SearchResponse scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.FORWARD);
                            if (searchResults.size() < maxCount) {
                                searchResults.add(sr);
                            } else {
                                break;
                            }
                        }
                        if (searchResults.size() == maxCount) {
                            break;
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                            if (searchResults.size() < maxCount) {
                                searchResults.add(sr);
                            } else {
                                break;
                            }
                        }
                        if (searchResults.size() == maxCount) {
                            break;
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case BOTH:
                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.FORWARD);
                            if (searchResults.size() < maxCount) {
                                searchResults.add(sr);
                            } else {
                                break;
                            }
                        }
                        if (searchResults.size() == maxCount) {
                            break;
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                            if (searchResults.size() < maxCount) {
                                searchResults.add(sr);
                            } else {
                                break;
                            }
                        }
                        if (searchResults.size() == maxCount) {
                            break;
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsExactMatch40WithMaxCount(final Long experimentId, int maxCount, String query,
            final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getTertiaryindex();

        try {
            SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("id")
                    .setFrom(0)
                    .setSize(maxCount)
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("eid", experimentId))
                            .must(QueryBuilders.matchQuery("sequence", query))).get();
                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), 40, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("eid", experimentId))
                            .must(QueryBuilders.matchQuery("sequence", revComp))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("eid", experimentId))
                            .must(QueryBuilders.matchQuery("sequence", query))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), 40, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("eid", experimentId))
                            .must(QueryBuilders.matchQuery("sequence", revComp))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsExactMatch40Scrolling(final Long experimentId, String query,
            final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getTertiaryindex();

        try {

            switch (strand) {
                case FORWARD:
                    SearchResponse scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case BOTH:
                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp)))
                            .setSize(100)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), 40, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

//    public static List<SearchResult> searchReadsExactMatchScrolling(final Long experimentId, String query,
//            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
//        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
//
//        Indices _index = INDICESMAPPER.get(1, experimentId);
//        String index = _index.getPrimaryindex();
//        String secondaryIndex = _index.getSecondaryindex();
//
//        try {
//
//            switch (strand) {
//                case FORWARD:
//                    SearchResponse scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
//                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
//                            .setScroll(new TimeValue(600000l))
//                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                            .setTypes("record")
//                            .setRouting(experimentId.toString())
//                            .storedFields("id")
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", query))).setSize(1000)
//                            .setExplain(false).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (query.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//
//                    break;
//                case REVERSECOMPLEMENT:
//                    DNASequence sequence = new DNASequence(query);
//                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
//                            = new ReversedSequenceView<>(
//                                    new ComplementSequenceView<>(sequence));
//                    String revComp = rc.getSequenceAsString();
//
//                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
//                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
//                            .setScroll(new TimeValue(600000l))
//                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                            .setTypes("record")
//                            .setRouting(experimentId.toString())
//                            .storedFields("id")
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", revComp))).setSize(1000)
//                            .setExplain(false).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (revComp.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//
//                    break;
//                case BOTH:
//                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
//                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
//                            .setScroll(new TimeValue(600000l))
//                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                            .setTypes("record")
//                            .setRouting(experimentId.toString())
//                            .storedFields("id")
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", query))).setSize(1000)
//                            .setExplain(false).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (query.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//
//                    sequence = new DNASequence(query);
//                    rc = new ReversedSequenceView<>(
//                            new ComplementSequenceView<>(sequence));
//                    revComp = rc.getSequenceAsString();
//
//                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
//                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
//                            .setScroll(new TimeValue(600000l))
//                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                            .setTypes("record")
//                            .setRouting(experimentId.toString())
//                            .storedFields("id")
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", revComp))).setSize(1000)
//                            .setExplain(false).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (revComp.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//                    break;
//            }
//
//        } catch (CompoundNotFoundException ex) {
//            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return searchResults;
//    }
    public static List<SearchResult> searchReadsExactMatchScrolling(final Long experimentId, String query,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getPrimaryindex();
//        String secondaryIndex = _index.getSecondaryindex();

        SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                .setScroll(new TimeValue(600000l))
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes("record")
                //                .setRouting(experimentId.toString())
                .storedFields("id")
                .setExplain(false);

//        SearchRequestBuilder srbs = CLIENT.prepareSearch(secondaryIndex)
//                .setScroll(new TimeValue(600000l))
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setTypes("record")
//                .setRouting(experimentId.toString())
//                .storedFields("id")
//                .setExplain(false);
        try {

            switch (strand) {
                case FORWARD:
                    SearchResponse scrollResp = srb
                            .setQuery(QueryBuilders.matchQuery("sequence", query)).setSize(1000).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            if (query.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

//                    scrollResp = srbs
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", query))).setSize(1000).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (query.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//
//                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = srb
                            .setQuery(QueryBuilders.matchQuery("sequence", revComp)).setSize(1000).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            if (revComp.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

//                    scrollResp = srbs
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", revComp))).setSize(1000).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (revComp.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
//
//                    break;
                case BOTH:
                    scrollResp = srb
                            .setQuery(QueryBuilders.matchQuery("sequence", query)).setSize(1000).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            if (query.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

//                    scrollResp = srbs
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", query))).setSize(1000).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (query.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    scrollResp = srb
                            .setQuery(QueryBuilders.matchQuery("sequence", revComp)).setSize(1000).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            if (revComp.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

//                    scrollResp = srbs
//                            .setQuery(QueryBuilders
//                                    .boolQuery()
//                                    .must(QueryBuilders.matchQuery("eid", experimentId))
//                                    .must(QueryBuilders.matchQuery("sequence", revComp))).setSize(1000).get();
//                    do {
//                        for (SearchHit hit : scrollResp.getHits().getHits()) {
//                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
//                            Reads r = READSMAPPER.get(experimentId, rid);
//                            if (revComp.contains(r.getSequence())) {
//                                SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
//                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
//                                searchResults.add(sr);
//                            }
//                        }
//                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000000l)).execute().actionGet();
//                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsExactMatch(final Long experimentId, String query, final int maxCount,
            final int searchLength, final boolean primaryIndex, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            Indices _index = INDICESMAPPER.get(1, experimentId);
            String index = _index.getPrimaryindex();
//            String secondaryIndex = _index.getSecondaryindex();

            SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("id")
                    //                    .setRouting(experimentId.toString())
                    .setFrom(0)
                    .setSize(maxCount)
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
//                    SearchResponse response = srb.setQuery(QueryBuilders
//                            .boolQuery()
//                            .must(QueryBuilders.matchQuery("eid", experimentId))
//                            .must(QueryBuilders.matchQuery("sequence", query))).get();
                    SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("sequence", query)).get();
                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    break;
                case BOTH:
                    count = 0;
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", query)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsAllMatchesScrolling(final Long experimentId, String query,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getPrimaryindex();
        String secondaryIndex = _index.getSecondaryindex();

        try {

            switch (strand) {
                case FORWARD:
                    SearchResponse scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(6000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(6000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    break;
                case BOTH:
                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(6000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO)))
                            .setSize(1000)
                            .setExplain(false).get();
                    System.out.println("Matches: " + scrollResp.getHits().getHits().length);
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index, secondaryIndex)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(6000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO)))
                            .setSize(1000)
                            .setExplain(false).get();
                    System.out.println("Matches: " + scrollResp.getHits().getHits().length);
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(6000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsAllMatchesFuzzyScrolling(final Long experimentId, String query,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);

        String index = _index.getPrimaryindex();
        String analyzer = "analyzer" + index.split("_")[3];
        AnalyzeRequest request = (new AnalyzeRequest(index)).analyzer(analyzer);
        List<UUID> matched = new ArrayList<>();
        try {

            switch (strand) {
                case FORWARD:
                    request.text(query);
                    List<AnalyzeResponse.AnalyzeToken> tokens = CLIENT.admin()
                            .indices().analyze(request).actionGet().getTokens();
                    System.out.println("Analyzed tokens size: " + tokens.size());

                    SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setSize(1000)
                            .setExplain(false);

                    Consumer<AnalyzeResponse.AnalyzeToken> action = (AnalyzeResponse.AnalyzeToken token) -> {
                        SearchResponse scrollResp
                                = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", token.getTerm().toLowerCase()).fuzziness(Fuzziness.TWO).prefixLength(2)).get();
                        do {
                            for (SearchHit hit : scrollResp.getHits().getHits()) {
                                UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                if (!matched.contains(rid)) {
                                    Reads r = READSMAPPER.get(experimentId, rid);
                                    SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                            r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                    searchResults.add(sr);
                                    matched.add(rid);
                                }
                            }
                            scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                        } while (scrollResp.getHits().getHits().length != 0);
                    };

                    Spliterator<AnalyzeResponse.AnalyzeToken> s = tokens.spliterator();
                    long targetBatchSize = s.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, s, action, targetBatchSize).invoke();

                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    request.text(revComp);
                    tokens = CLIENT.admin().indices().analyze(request).actionGet().getTokens();

                    srb = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setSize(1000)
                            .setExplain(false);

                    action = (AnalyzeResponse.AnalyzeToken token) -> {
                        SearchResponse scrollResp
                                = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", token.getTerm().toLowerCase()).fuzziness(Fuzziness.TWO).prefixLength(2)).get();
                        do {
                            for (SearchHit hit : scrollResp.getHits().getHits()) {
                                UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                if (!matched.contains(rid)) {
                                    Reads r = READSMAPPER.get(experimentId, rid);
                                    SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                            r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                    searchResults.add(sr);
                                    matched.add(rid);
                                }
                            }
                            scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                        } while (scrollResp.getHits().getHits().length != 0);
                    };

                    s = tokens.spliterator();
                    targetBatchSize = s.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, s, action, targetBatchSize).invoke();

                    break;
                case BOTH:
                    request.text(query);
                    tokens = CLIENT.admin().indices().analyze(request).actionGet().getTokens();
                    System.out.println("Analyzed query tokens size: " + tokens.size());

                    srb = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setSize(1000)
                            .setExplain(false);

                    action = (AnalyzeResponse.AnalyzeToken token) -> {
                        SearchResponse scrollResp
                                = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", token.getTerm().toLowerCase()).fuzziness(Fuzziness.TWO).prefixLength(2)).get();
                        do {
                            for (SearchHit hit : scrollResp.getHits().getHits()) {
                                UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                if (!matched.contains(rid)) {
                                    Reads r = READSMAPPER.get(experimentId, rid);
                                    SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                            r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                    searchResults.add(sr);
                                    matched.add(rid);
                                }
                            }
                            scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                        } while (scrollResp.getHits().getHits().length != 0);
                    };

                    s = tokens.spliterator();
                    targetBatchSize = s.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, s, action, targetBatchSize).invoke();

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    request.text(revComp);
                    tokens = CLIENT.admin().indices().analyze(request).actionGet().getTokens();
                    System.out.println("Analyzed revComp tokens size: " + tokens.size());

                    action = (AnalyzeResponse.AnalyzeToken token) -> {
                        SearchResponse scrollResp
                                = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", token.getTerm().toLowerCase()).fuzziness(Fuzziness.TWO).prefixLength(2)).get();
                        do {
                            for (SearchHit hit : scrollResp.getHits().getHits()) {
                                UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                if (!matched.contains(rid)) {
                                    Reads r = READSMAPPER.get(experimentId, rid);
                                    SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                            r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                    searchResults.add(sr);
                                    matched.add(rid);
                                }
                            }
                            scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                        } while (scrollResp.getHits().getHits().length != 0);
                    };

                    s = tokens.spliterator();
                    targetBatchSize = s.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, s, action, targetBatchSize).invoke();

                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReadsAllMatchesFuzzyScrolling2(final Long experimentId, String query,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());

        Indices _index = INDICESMAPPER.get(1, experimentId);
        String index = _index.getPrimaryindex();

        try {

            switch (strand) {
                case FORWARD:
                    SearchResponse scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO).fuzzyTranspositions(true)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            if (!searchResults.contains(sr)) {
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case REVERSECOMPLEMENT:
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO).fuzzyTranspositions(true)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            if (!searchResults.contains(sr)) {
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
                case BOTH:
                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO).fuzzyTranspositions(true)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            if (!searchResults.contains(sr)) {
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch(index)
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(600000l))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setRouting(experimentId.toString())
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO).fuzzyTranspositions(true)))
                            .setSize(1000)
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Reads r = READSMAPPER.get(experimentId, rid);
                            SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            if (!searchResults.contains(sr)) {
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000l)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                    break;
            }

        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReads1Mismatch(final Long experimentId, String query, final int maxCount,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            Indices _index = INDICESMAPPER.get(1, experimentId);
            String index = _index.getPrimaryindex();
            String secondaryIndex = _index.getSecondaryindex();

            SearchRequestBuilder srb = CLIENT.prepareSearch(index, secondaryIndex)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("id")
                    .setRouting(experimentId.toString())
                    .setFrom(0)
                    .setSize(maxCount)
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", query))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", revComp))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", query))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", revComp))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
            }
        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchReads2Mismatches(final Long experimentId, String query, final int maxCount,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            Indices _index = INDICESMAPPER.get(1, experimentId);
            String index = _index.getPrimaryindex();
            String secondaryIndex = _index.getSecondaryindex();

            SearchRequestBuilder srb = CLIENT.prepareSearch(index, secondaryIndex)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("id")
                    .setFrom(0)
                    .setSize(maxCount)
                    .setRouting(experimentId.toString())
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);
                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("eid", experimentId))
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders
                                            .boolQuery()
                                            .must(QueryBuilders.matchQuery("eid", experimentId))
                                            .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                        Reads r = READSMAPPER.get(experimentId, rid);

                        SearchResult sr = new SearchResult(experimentId, -1l, rid, r.getCount(), r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
            }
        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchPolyaReads(String transcript, int searchLength,
            SearchResult.SearchType searchType, Strand strand) {

        ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        });
        List<Callable<Object>> callables = new ArrayList<>();
        Set<SearchResult> searchResults = Collections.synchronizedSet(new HashSet<>());
        List<String> tokens = tokenize(transcript, searchLength);

        tokens.stream().forEach((_item) -> {
            Callable<Object> callable = () -> {
                try {
                    SearchResponse scrollResp = CLIENT.prepareSearch("index_polya_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", _item))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Polyareads r = POLYAREADSMAPPER.get(1, rid);
                            if (_item.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    DNASequence sequence = new DNASequence(_item);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch("index_polya_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", revComp))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Polyareads r = POLYAREADSMAPPER.get(1, rid);
                            if (revComp.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                } catch (CompoundNotFoundException e) {
                    Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, e);
                }
                return searchResults;
            };
            callables.add(callable);
        });

        try {
            ex.invokeAll(callables);
        } catch (InterruptedException ex1) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex1);
        }
        ex.shutdown();

        List<SearchResult> sr1 = new ArrayList<>();
        sr1.addAll(searchResults);
        return sr1;
    }

    public static List<SearchResult> searchBartelPolyaReads(String transcript, int searchLength,
            SearchResult.SearchType searchType, Strand strand) {

        ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        });
        List<Callable<Object>> callables = new ArrayList<>();
        Set<SearchResult> searchResults = Collections.synchronizedSet(new HashSet<>());
        List<String> tokens = tokenize(transcript, searchLength);

        tokens.stream().forEach((_item) -> {
            Callable<Object> callable = () -> {
                try {
                    SearchResponse scrollResp = CLIENT.prepareSearch("index_bartel_polya_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", _item))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Bartelpolyareads r = BARTELPOLYAREADSMAPPER.get(1, rid);
                            if (_item.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    DNASequence sequence = new DNASequence(_item);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch("index_bartel_polya_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", revComp))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Bartelpolyareads r = BARTELPOLYAREADSMAPPER.get(1, rid);
                            if (revComp.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                } catch (CompoundNotFoundException e) {
                    Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, e);
                }
                return searchResults;
            };
            callables.add(callable);
        });

        try {
            ex.invokeAll(callables);
        } catch (InterruptedException ex1) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex1);
        }
        ex.shutdown();

        List<SearchResult> sr1 = new ArrayList<>();
        sr1.addAll(searchResults);
        return sr1;
    }

    public static List<SearchResult> searchTssReads(String transcript, int searchLength,
            SearchResult.SearchType searchType, Strand strand) {

        ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        });
        List<Callable<Object>> callables = new ArrayList<>();
        Set<SearchResult> searchResults = Collections.synchronizedSet(new HashSet<>());
        List<String> tokens = tokenize(transcript, searchLength);

        tokens.stream().forEach((_item) -> {
            Callable<Object> callable = () -> {
                try {
                    SearchResponse scrollResp = CLIENT.prepareSearch("index_tss_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", _item))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Tssreads r = TSSREADSMAPPER.get(1, rid);
                            if (_item.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);

                    DNASequence sequence = new DNASequence(_item);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    scrollResp = CLIENT.prepareSearch("index_tss_reads_20")
                            .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                            .setScroll(new TimeValue(60000))
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setTypes("record")
                            .storedFields("id")
                            .setQuery(QueryBuilders.matchQuery("sequence", revComp))
                            .setSize(100)
                            .setRouting("1")
                            .setExplain(false).get();
                    do {
                        for (SearchHit hit : scrollResp.getHits().getHits()) {
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            Tssreads r = TSSREADSMAPPER.get(1, rid);
                            if (revComp.contains(r.getSequence())) {
                                SearchResult sr = new SearchResult(1l, -1l, rid, r.getCount(), r.getSequence(), r.getRemoveda().toString(),
                                        r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                                searchResults.add(sr);
                            }
                        }
                        scrollResp = CLIENT.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                    } while (scrollResp.getHits().getHits().length != 0);
                } catch (CompoundNotFoundException ex1) {
                    Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex1);
                }
                return searchResults;
            };
            callables.add(callable);
        });

        try {
            ex.invokeAll(callables);
        } catch (InterruptedException ex1) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex1);
        }
        ex.shutdown();

        List<SearchResult> sr1 = new ArrayList<>();
        sr1.addAll(searchResults);
        return sr1;
    }

    public static List<SearchResult> searchTranscriptsExactMatch(String query, final int maxCount,
            final Integer searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            String index = "index_transcripts_" + searchLength.toString();

            SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid", "name")
                    .setRouting("1")
                    .setFrom(0)
                    .setSize(maxCount)
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("sequence", query)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);
                        SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);
                        SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", query)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);
                        SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), name,
                                r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }

                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();
                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);
                        SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), name,
                                r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                        searchResults.add(sr);
                        count++;
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
            }

        } catch (NumberFormatException | CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchTranscripts1Mismatch(String query, final int maxCount,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            String index;
            if ((searchLength == 101) || (searchLength == 200)) {
                index = "index_transcripts_101";
            } else if (searchLength == 100) {
                index = "index_transcripts_100";
            } else if (searchLength == 97) {
                index = "index_transcripts_97";
            } else if (searchLength == 76) {
                index = "index_transcripts_76";
            } else if (searchLength == 75) {
                index = "index_transcripts_75";
            } else if (searchLength == 65) {
                index = "index_transcripts_65";
            } else if (searchLength == 50) {
                index = "index_transcripts_50";
            } else if (searchLength == 40) {
                index = "index_transcripts_40";
            } else {
                index = "index_transcripts_101";
            }

            SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid", "name")
                    .setRouting("1")
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))
                            .mustNot(QueryBuilders.matchQuery("sequence", query))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(query, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 1) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                            count++;
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))
                            .mustNot(QueryBuilders.matchQuery("sequence", revComp))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(revComp, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 1) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                            count++;
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.boolQuery()
                            .should(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders.matchQuery("sequence", revComp)))
                            .should(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))
                                    .mustNot(QueryBuilders.matchQuery("sequence", query)))
                    ).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(revComp, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 1) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                            count++;
                            count++;
                        } else {
                            fuzzy = new Fuzzy();
                            distance = fuzzy.containability(query, r.getSequence()) * r.getSequence().length();
                            if (Math.rint(distance) == 1) {
                                SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                                count++;
                                count++;
                            }
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
            }
        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static List<SearchResult> searchTranscripts2Mismatches(String query, final int maxCount,
            final int searchLength, final SearchResult.SearchType searchType, final Strand strand) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<SearchResult>());
        try {

            String index;
            if ((searchLength == 101) || (searchLength == 200)) {
                index = "index_transcripts_101";
            } else if (searchLength == 100) {
                index = "index_transcripts_100";
            } else if (searchLength == 97) {
                index = "index_transcripts_97";
            } else if (searchLength == 76) {
                index = "index_transcripts_76";
            } else if (searchLength == 75) {
                index = "index_transcripts_75";
            } else if (searchLength == 65) {
                index = "index_transcripts_65";
            } else if (searchLength == 50) {
                index = "index_transcripts_50";
            } else if (searchLength == 40) {
                index = "index_transcripts_40";
            } else {
                index = "index_transcripts_101";
            }

            SearchRequestBuilder srb = CLIENT.prepareSearch(index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid", "name")
                    .setRouting("1")
                    .setExplain(false);

            switch (strand) {
                case FORWARD:
                    int count = 0;
                    SearchResponse response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO))
                            .mustNot(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(query, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 2) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                            searchResults.add(sr);
                            count++;
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case REVERSECOMPLEMENT:
                    count = 0;
                    DNASequence sequence = new DNASequence(query);
                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders
                            .boolQuery()
                            .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO))
                            .mustNot(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE))).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(revComp, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 2) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                            count++;
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
                case BOTH:
                    count = 0;
                    sequence = new DNASequence(query);
                    rc = new ReversedSequenceView<>(
                            new ComplementSequenceView<>(sequence));
                    revComp = rc.getSequenceAsString();

                    response = srb.setQuery(QueryBuilders.boolQuery()
                            .should(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE)))
                            .should(QueryBuilders
                                    .boolQuery()
                                    .must(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.TWO))
                                    .mustNot(QueryBuilders.matchQuery("sequence", query).fuzziness(Fuzziness.ONE)))
                    ).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String name = hit.field("name").getValue().toString();
                        Transcript r = TRANSCRIPTMAPPER.get(1, tid, name);

                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(revComp, r.getSequence()) * r.getSequence().length();
                        if (Math.rint(distance) == 2) {
                            SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                    r.getSequence().length(), searchLength, searchType, Strand.REVERSECOMPLEMENT);
                            searchResults.add(sr);
                            count++;
                            count++;
                        } else {
                            fuzzy = new Fuzzy();
                            distance = fuzzy.containability(query, r.getSequence()) * r.getSequence().length();
                            if (Math.rint(distance) == 2) {
                                SearchResult sr = new SearchResult(-1, tid, null, 0, r.getSequence(), "",
                                        r.getSequence().length(), searchLength, searchType, Strand.FORWARD);
                                searchResults.add(sr);
                                count++;
                                count++;
                            }
                        }
                        if (count >= maxCount) {
                            break;
                        }
                    }
                    break;
            }
        } catch (CompoundNotFoundException ex) {
            Logger.getLogger(DataAccessES.class.getName()).log(Level.SEVERE, null, ex);
        }
        return searchResults;
    }

    public static ClusterHealthStatus getHealthStatus() {
        ClusterHealthResponse healths = CLIENT.admin().cluster().prepareHealth().get();
        return healths.getStatus();
    }

    public static List<String> tokenize(String transcript, Integer readLength) {
        List<String> tokens = new ArrayList<>();
        int length = transcript.length();
        for (int i = 0; i < length; i += 5000) {
            int start = (i == 0) ? 0 : (i - (readLength / 2) - 1);
            int end = ((i + 5000 + (readLength / 2) + 1) > length) ? length : (i + 5000 + (readLength / 2) + 1);
            String token = transcript.substring(start, end);
            tokens.add(token);
        }
        return tokens;
    }

    static class ParEach<T> extends CountedCompleter<Void> {

        final Spliterator<T> spliterator;
        final Consumer<T> action;
        final long targetBatchSize;

        ParEach(ParEach<T> parent, Spliterator<T> spliterator,
                Consumer<T> action, long targetBatchSize) {
            super(parent);
            this.spliterator = spliterator;
            this.action = action;
            this.targetBatchSize = targetBatchSize;
        }

        @Override
        public void compute() {
            Spliterator<T> sub;
            while ((sub = spliterator.trySplit()) != null
                    && spliterator.estimateSize() > targetBatchSize) {
                addToPendingCount(1);
                new ParEach<>(this, sub, action, targetBatchSize).fork();
            }
            spliterator.forEachRemaining(action);
            propagateCompletion();
        }
    }
}
