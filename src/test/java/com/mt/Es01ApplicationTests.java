package com.mt;

import com.mt.bean.Student;
import com.mt.dao.StudentDao;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import java.util.*;
import java.util.stream.Collectors;

@SpringBootTest
class Es01ApplicationTests {

    @Autowired
    private ElasticsearchRestTemplate restTemplate;

    //@Autowired
    //private ReactiveElasticsearchTemplate reactiveTemplate;

    @Test
     void createIndex(){
        IndexOperations indexOperations = restTemplate.indexOps(Student.class);
        indexOperations.delete();
        indexOperations.create();
        restTemplate.putMapping(Student.class);
    }


    @Test
    void addData() {

        List<String> tags = new ArrayList<>();
        tags.add("唱歌");
        tags.add("跳舞");
        tags.add("打篮球");

        Student student = new Student(1,"张三",20,new Date(),tags,"中华人民共和国");


        restTemplate.save(student);

    }


    @Autowired
    private StudentDao studentDao;


    @Test
    void query(){



        Optional<Student> byId = studentDao.findById(2);

        System.out.println(byId.get());

        List<Student> list = studentDao.queryByNameLikeAndHobbyLike("人民","人民");
        System.out.println("list===>"+list);


        List<Student> list2 = studentDao.queryByBirthdayBetween(new Date(2001),new Date());
        System.out.println("list2=====>"+list2);

    }


    @Test
     void update(){



        List<String> tags = new ArrayList<>();
        tags.add("唱歌");
        tags.add("跳舞");
        tags.add("打篮球");

        Student student = new Student(1,"张三",20,new Date(),tags,"中华人民共和国");


        List<String> tags2 = new ArrayList<>();
        tags2.add("演戏");
        tags2.add("唱歌");
        tags2.add("跳舞");

        Student student2 = new Student(2,"张小斐",18,new Date(),tags2,"人民国家");




        List<String> tags3 = new ArrayList<>();
        tags3.add("演戏");
        tags3.add("唱歌");
        tags3.add("跳舞");

        Student student3 = new Student(3,"王老",40,new Date(),tags3,"刷牙刷");




        List<String> tags4 = new ArrayList<>();
        tags4.add("演戏");
        tags4.add("唱歌");
        tags4.add("跳舞");

        Student student4 = new Student(4,"人民",40,new Date(),tags4,"牙刷不要太湖");


        List<Student> list = new ArrayList<>();
        list.add(student);
        list.add(student2);
        list.add(student3);
        list.add(student4);

        studentDao.saveAll(list);


    }


    /**
     * 查询所有
     * */
    @Test
    void queryAll(){

        MatchAllQueryBuilder matchAll = new MatchAllQueryBuilder();

        Sort sort = Sort.by(new Sort.Order(Sort.Direction.ASC,"age"));
         sort = sort.and(Sort.by(new Sort.Order(Sort.Direction.DESC, "userId")));

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(matchAll)
                //高亮
                .withHighlightBuilder(new HighlightBuilder().field("name"))
                //分页查询+ 排序
                .withPageable(PageRequest.of(0, 5,sort))
                .build();


        //设置分页 注意分页从 0 开始

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> studnetList = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        studnetList.forEach(System.out::println);
    }



    /**
     * term 查询
     *  查询的值不会被分词
     *
     GET /student/_search
     {
         "query": {
             "term": {
                "name": "张小斐"
             }
         }
     }

     * */
    @Test
    void termQuery(){

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "人民");


        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(termQueryBuilder)
                .build();

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);

    }


    /**
     * terms 查询 + 高亮显示
     * 一个字段匹配多个值  where name ='aa' or name ='bb'
     *
     GET /student/_search
     {
         "query": {
             "terms": {
                 "name": ["张小斐","张三"]
             }
         },
         "highlight": {
             "fields": {
                 "name": {
                    "pre_tags": "<a href='a'>",
                    "post_tags": "</a>"
                 }
             }
         }
     }
     * */
    @Test
     void termsQuery(){

        TermsQueryBuilder termsQuery = new TermsQueryBuilder("name","张小斐","张三");

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(termsQuery)
                //高亮
                .withHighlightBuilder(new HighlightBuilder().field("name").preTags("<a href='aa'>").postTags("</a>"))
                .build();
        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(o -> {
            Student content = o.getContent();
            //把高亮的值取出然后替换name
            content.setName(o.getHighlightField("name").get(0));
            return content;

        }).collect(Collectors.toList());

        collect.forEach(System.out::println);


    }


    /**
     * match 查询的值会被分词器 分词
     *

     GET /student/_search
     {
         "query": {
             "match": {
                "hobby": "国家"
             }
         }
     }

     * */
    @Test
     void matchQuery(){

        MatchQueryBuilder matchQuery = new MatchQueryBuilder("hobby","国家");
        matchQuery.operator(Operator.AND);

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(matchQuery)
                .build();
        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::print);

    }


    /**
     * mulit_match 查询多个字段 where name ='aa' or hobby = 'aa'
     * */

    @Test
     void mulitMatchQuery(){

        MultiMatchQueryBuilder multiMatch = new MultiMatchQueryBuilder("人民","name","hobby");

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(multiMatch)
                .build();

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);


    }


    /**
     * 通过 id查询
     * */
    @Test
     void idsQuery(){

        IdsQueryBuilder idQuery = new IdsQueryBuilder();
        idQuery.addIds("1","2","3");

        NativeSearchQuery query = new NativeSearchQuery(idQuery);

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);
    }


    /**
     * prefix 前缀查询 查询已 xx开头的
     * */

    @Test
     void prefixQuery(){


        PrefixQueryBuilder prefixQuery = new PrefixQueryBuilder("name","张");

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(prefixQuery)
                .build();

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);
    }


    /**
     * wildcard 通配符查询 ? 表示一个字符 * 表示多个字符
     *
     * */
    @Test
     void wildcardQuery(){

        WildcardQueryBuilder wildcard = new WildcardQueryBuilder("name","张");


        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(wildcard)
                .build();

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> collect = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);
    }

    /**
     * range 范围查询
     * 查询 age 在 10 - 20 岁之间的
     * */
    @Test
     void rangeQuery(){

        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("age");
        rangeQuery.gte("10");
        rangeQuery.lte("20");

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(rangeQuery)
                .build();
        SearchHits<Student> search = restTemplate.search(query, Student.class);

        List<Student> studentList = search.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());

        studentList.forEach(System.out::println);

    }


    /**
     * 正则匹配查询
     * */
    @Test
      void regexpQuery(){

        RegexpQueryBuilder regexpQuery = new RegexpQueryBuilder("name","正则");



        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(regexpQuery)
                .build();

        restTemplate.search(query,Student.class);
     }

     /**
      * elasticsearch 的 分页查询 最大支支持 1万条记录
      * 如果数据量过大只能使用 scroll
      from+size  ES查询数据的方式：
          1  先将用户指定的关键词进行分词处理
          2  将分词去词库中进行检索，得到多个文档的id
          3  去各个分片中拉去指定的数据   耗时
          4  根据数据的得分进行排序       耗时
          5  根据from的值，将查询到的数据舍弃一部分，
          6  返回查询结果

      Scroll+size    在ES中查询方式
          1  先将用户指定的关键词进行分词处理
          2  将分词去词库中进行检索，得到多个文档的id
          3  将文档的id存放在一个ES的上下文中，ES内存
          4  根据你指定给的size的个数去ES中检索指定个数的数据，拿完数据的文档id,会从上下文中移除
          5  如果需要下一页的数据，直接去ES的上下文中，找后续内容
          6  循环进行4.5操作
      * */

    @Test
     void scrollQuery(){

        MatchAllQueryBuilder matchAllQuery = new MatchAllQueryBuilder();

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(matchAllQuery)
                .withPageable(PageRequest.of(0,1))
                .build();

        SearchScrollHits<Student> student = restTemplate.searchScrollStart(1000 * 60 * 3, query, Student.class, IndexCoordinates.of("student"));

        System.out.println("第一页");
        List<Student> collect = student.get().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);


        String scrollId = student.getScrollId();

        student = restTemplate.searchScrollContinue(scrollId,1000*60*3,Student.class,IndexCoordinates.of("student"));

        System.out.println("第二页");
         collect = student.get().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);


         scrollId = student.getScrollId();
         student = restTemplate.searchScrollContinue(scrollId,1000*60*3,Student.class,IndexCoordinates.of("student"));

         System.out.println("第三页");
         collect = student.get().map(SearchHit::getContent).collect(Collectors.toList());

        collect.forEach(System.out::println);

         scrollId = student.getScrollId();



        student = restTemplate.searchScrollContinue(scrollId,1000*60*3,Student.class,IndexCoordinates.of("student"));
        scrollId = student.getScrollId();
        System.out.println("第四页");
        collect = student.get().map(SearchHit::getContent).collect(Collectors.toList());
        collect.forEach(System.out::println);

        //如果不用了要及时删除
        restTemplate.searchScrollClear(Collections.singletonList(scrollId));
    }

    /**
     * bool 查询 用于 多逻辑查询
      must(and)      : 必须匹配的内容
     must_not(!=)    : 必须不匹配的内容
     should( or )    : 可选项 匹配到了加分,不匹配就不影响
     *
     * */

    @Test
    void boolQuery(){

        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        MatchQueryBuilder matchQuery = new MatchQueryBuilder("tag","演戏");
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("age");
        rangeQuery.gte(20);

        //查询 tag 为 演戏的 并且 age> 20
        List<QueryBuilder> must = boolQuery.must();
        must.add(matchQuery);
        must.add(rangeQuery);


        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(boolQuery)
                .build();

        restTemplate.search(query,Student.class).getSearchHits().forEach( o -> System.out.println(o.getContent()));
    }



    @Test
    void test1(){

        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name","王老 张小斐");
        matchQuery.operator(Operator.OR);

        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(matchQuery)
                .build();

        SearchHits<Student> search = restTemplate.search(query, Student.class);

        search.getSearchHits().forEach( o ->System.out.println(o.getContent()));
    }
}
