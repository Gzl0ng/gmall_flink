package com.gzl0ng.app;

import com.gzl0ng.utils.ThreadPoolUtil;
import lombok.SneakyThrows;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 郭正龙
 * @date 2022-04-13
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        ThreadPoolExecutor threadpool = ThreadPoolUtil.getThreadpool();
        for (int i = 0; i < 10; i++) {
            threadpool.submit(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + ":gzl0ng");
                    Thread.sleep(2000);
                }
            });
        }
    }
}
