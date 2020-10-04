package com.roncoo.eshop.dataaggr.rabbitmq;


import com.alibaba.fastjson.JSONObject;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


@Component
@RabbitListener(queues = "refresh-aggr-data-change-queue")
public class RefreshAggrDataChangeQueueReceiver {



    @Autowired
    private JedisPool jedisPool;

    @RabbitHandler
    public void process(String message){
        //对这个message进行解析
        System.out.println("从aggr-data-change-queue队列接受一条消息：" + message);
        JSONObject messageJsonObject = JSONObject.parseObject(message);

        String dimType = messageJsonObject.getString("dim_type");

        if("brand".equals(dimType)){
            processBrandDimChange(messageJsonObject);
        }else if("category".equals(dimType)){
            processCategoryDimChange(messageJsonObject);
        }else if("product".equals(dimType)){
            processProductDimChange(messageJsonObject);
        }else if("product_intro".equals(dimType)){
            processProductIntroDimChange(messageJsonObject);
        }


    }


    private void processBrandDimChange(JSONObject messageJsonObject){
        Long id = messageJsonObject.getLong("id");
        Jedis jedis = jedisPool.getResource();
        String dataJSON = jedis.get("brand:" + id+":");

        if(dataJSON != null && !"".equals(dataJSON)){
            jedis.set("dim_brand:" + id +":",dataJSON);
        }else {
            jedis.del("dim_brand:" + id+":");
        }
    }

    private void processCategoryDimChange(JSONObject messageJsonObject){
        Long id = messageJsonObject.getLong("id");
        Jedis jedis = jedisPool.getResource();
        String dataJSON = jedis.get("category:" + id+":");

        if(dataJSON != null && !"".equals(dataJSON)){
            jedis.set("dim_category:" + id +":",dataJSON);
        }else {
            jedis.del("dim_category:" + id+":");
        }
    }


    private void processProductDimChange(JSONObject messageJsonObject){
        Long id = messageJsonObject.getLong("id");
        Jedis jedis = jedisPool.getResource();
        String productDataJSON = jedis.get("product:" + id+":");

        if(productDataJSON != null && !"".equals(productDataJSON)){
            JSONObject productDataJSONObject = JSONObject.parseObject(productDataJSON);

            String productPropertyDataJSON = jedis.get("product_property:" + id+":");
            if(productPropertyDataJSON != null && !"".equals(productPropertyDataJSON)){
                productDataJSONObject.put("product_property",productPropertyDataJSON);
            }

            String productSpecificationDataJSON = jedis.get("product_specification:" + id+":");
            if(productSpecificationDataJSON != null && !"".equals(productSpecificationDataJSON)){
                productDataJSONObject.put("product_specification",productSpecificationDataJSON);
            }


            jedis.set("dim_product:" + id +":",productDataJSONObject.toJSONString());
        }else {
            jedis.del("dim_product:" + id+":");
        }
    }


    private void processProductIntroDimChange(JSONObject messageJsonObject){
        Long id = messageJsonObject.getLong("id");
        Jedis jedis = jedisPool.getResource();
        String dataJSON = jedis.get("product_intro:" + id+":");

        if(dataJSON != null && !"".equals(dataJSON)){
            jedis.set("dim_product_intro:" + id +":",dataJSON);
        }else {
            jedis.del("dim_product_intro:" + id+":");
        }
    }


}
