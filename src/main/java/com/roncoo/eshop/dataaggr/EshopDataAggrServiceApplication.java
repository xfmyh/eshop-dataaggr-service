package com.roncoo.eshop.dataaggr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@SpringBootApplication
@EnableEurekaClient
@EnableFeignClients
public class EshopDataAggrServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(EshopDataAggrServiceApplication.class,args);
    }

    @Bean
    public JedisPool jedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxWaitMillis(1000 * 10);
        jedisPoolConfig.setTestOnBorrow(true);

        return new JedisPool(jedisPoolConfig,"172.17.0.2",1111);
    }

}
