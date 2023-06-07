package com.example.community;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.example.community.dao.DiscussPostMapper;
import com.example.community.dao.elasticsearch.DiscussPostRepository;
import com.example.community.entity.DiscussPost;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.client.elc.QueryBuilders;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitSupport;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchPage;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.highlight.Highlight;
import org.springframework.data.elasticsearch.core.query.highlight.HighlightField;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = CommunityApplication.class)
public class ElasticsearchTests {

    @Autowired
    private DiscussPostMapper discussPostMapper;

    @Autowired
    private DiscussPostRepository discussPostRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void testInsert() {
        discussPostRepository.save(discussPostMapper.selectDiscussPostById(241));
        discussPostRepository.save(discussPostMapper.selectDiscussPostById(242));
        discussPostRepository.save(discussPostMapper.selectDiscussPostById(243));
    }

    @Test
    public void testInsertList() {
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(101, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(102, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(103, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(111, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(112, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(131, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(132, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(133, 0, 100));
        discussPostRepository.saveAll(discussPostMapper.selectDiscussPosts(134, 0, 100));
    }

    @Test
    public void testDelete() {
        discussPostRepository.deleteAll();
    }

/*    @Test
    public void testSearchByRepository() {
        NativeQuery searchQuery = new NativeQueryBuilder()
                .withQuery(QueryBuilders.)
    }*/

    @Test
    public void testSearchByTemplate() {
        HighlightField titleHighlightField = new HighlightField("title");
        HighlightField contentHighlightField = new HighlightField("content");

        Highlight highlight = new Highlight(List.of(titleHighlightField, contentHighlightField));

        NativeQuery searchQuery = new NativeQueryBuilder()
                .withQuery(Query.of(q -> q.multiMatch(mq -> mq.query("互联网寒冬").fields("title", "content"))))
                .withSort(Sort.by("type", "score", "createTime").descending())
                .withPageable(PageRequest.of(0, 10))
                .withHighlightQuery(
                        new HighlightQuery(highlight, DiscussPostRepository.class)
                ).build();

        SearchHits<DiscussPost> searchHits = elasticsearchTemplate.search(searchQuery, DiscussPost.class);
        SearchPage<DiscussPost> page = SearchHitSupport.searchPageFor(searchHits, searchQuery.getPageable());

        if(!page.isEmpty()) {
            for(SearchHit<DiscussPost> hit : page) {
                DiscussPost discussPost = hit.getContent();
                //获取高亮部分
                List<String> title = hit.getHighlightFields().get("title");
                if(title != null) {
                    discussPost.setTitle(title.get(0));
                }

                List<String> content = hit.getHighlightFields().get("content");
                if(content != null) {
                    discussPost.setContent(content.get(0));
                }

                System.out.println(hit.getContent());
            }
        }

/*        SearchPage<DiscussPost> searchPage = page;
        List<Map<String, Object>> discussPosts = new ArrayList<>();
        if(searchPage != null) {
            for(SearchHit<DiscussPost> discussPostSearchHit : searchPage) {
                Map<String, Object> map = new HashMap<>();
                //帖子
                DiscussPost post = discussPostSearchHit.getContent();
                System.out.println(post);
            }
        }*/

    }

}
