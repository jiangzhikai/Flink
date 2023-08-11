package org.example.function;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.pojo.Event;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author WEIYUNHUI
 * @date 2023/8/6 15:31
 *
 * 基于自定义数据源 ，模拟生成点击数据
 *
 * 使用SourceFunction的方式:
 *    1. 实现SourceFunction接口
 *    2. 重写方法
 *          run：     用于生成数据
 *          cancel:  退出Source
 *
 */
public class ClickSource implements SourceFunction<Event> {

    static boolean  isRunning = true ;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //每秒生成1条数据
        while(isRunning){
            // 生成的数据
            String [] users = {"Zhang3", "Li4" , "Tom" , "Jerry" , "Alice" , "Peiqi"} ;
            String [] urls = {"/home" , "/pay" , "/detail" , "/cart" , "/order" , "/login"} ;

            Event event = new Event(
                    users[RandomUtils.nextInt(0, users.length)],
                    urls[RandomUtils.nextInt(0, urls.length)],
                    System.currentTimeMillis()
            );

            //发射数据
            ctx.collect(event);

            //休眠1秒
            //Thread.sleep(1000);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}