package com.leyou;


import com.leyou.pojo.Goods;
import com.leyou.pojo.Item;
import com.leyou.repository.GoodsRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringDataEsTest {
    @Autowired
    private ElasticsearchTemplate esTemplate;
    //根据Goods中的注解的配置生成索引库
    @Test
    public void testAddIndex(){
        esTemplate.createIndex(Goods.class);
    }
    //根据Goods中的注解的配置生成mapping映射
    @Test
    public void testAddMapping(){
        esTemplate.putMapping(Goods.class);
    }
    @Autowired
    private GoodsRepository goodsRepository;
    @Test
    public void testAddDoc(){
        Goods goods = new Goods("1","小米9999手机","手机","小米",1199.0,"q3311");
        goodsRepository.save(goods);//有新增和修改的功能
        }
        @Test
    public void testAddBulkDoc(){
            List<Goods> list = new ArrayList<>();
            list.add(new Goods("1", "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("2", "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("3", "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("4", "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("5", "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("6", "IphoneX", "手机", "苹果", 7299.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("7", "Iphone11", "手机", "苹果", 6799.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("8", "三星S10", "手机", "三星", 5799.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("9", "OPPOFindX", "手机", "OPPO", 3799.00, "http://image.leyou.com/13123.jpg"));
            list.add(new Goods("10", "ViVONex", "手机", "ViVO", 4499.00, "http://image.leyou.com/13123.jpg"));
            goodsRepository.saveAll(list);

        }
        @Test
    public void testdeleteDoc(){
        //goodsRepository.deleteAll();
            goodsRepository.deleteById("1");
        }
    @Test
    public void testSearch(){

//      List<Goods> goodsList = goodsRepository.findByTitle("小米");
//      List<Goods> goodsList = goodsRepository.findByBrand("小米");
//        List<Goods> goodsList =  goodsRepository.findByPriceBetween(2000.0,5000.0);
//        List<Goods> goodsList = goodsRepository.findByBrandAndPriceBetween("小米",2000.0,5000.0);
        List<Goods> goodsList = goodsRepository.findByBrandOrPriceBetween("小米",2000.0,5000.0);
        goodsList.forEach(goods -> {
            System.out.println(goods);
        });
    }


    @Test
    public void testQuery(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
//        nativeSearchQueryBuilder.withQuery(QueryBuilders.termQuery("title","华为"));
        nativeSearchQueryBuilder.withQuery(QueryBuilders.matchAllQuery());

        nativeSearchQueryBuilder.withPageable( PageRequest.of(0,2));  //分页

//        聚合条件
        nativeSearchQueryBuilder.addAggregation( AggregationBuilders.terms("brandCount").field("brand"));

//        SearchQuery query
        AggregatedPage<Goods> aggregatedPage = esTemplate.queryForPage(nativeSearchQueryBuilder.build(), Goods.class);

//        获取聚合的结果
        Terms terms = aggregatedPage.getAggregations().get("brandCount");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println( bucket.getKeyAsString()+":"+bucket.getDocCount());
        });
//        List<Goods> goodsList = aggregatedPage.getContent(); //获取数据的集合
//        goodsList.forEach(goods -> {
//            System.out.println(goods);
//        });-

    }

}
