package com.gzl0ng.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author 郭正龙
 * @date 2022-04-13
 */

//懒汉式
public class ThreadPoolUtil {

     private static ThreadPoolExecutor threadPoolExecutor = null;


     private ThreadPoolUtil(){

     }

     public static ThreadPoolExecutor getThreadpool(){
         if (threadPoolExecutor == null){
             synchronized (ThreadPoolExecutor.class){
                 if (threadPoolExecutor == null){
                     threadPoolExecutor = new ThreadPoolExecutor(2,10,1L, TimeUnit.MINUTES,new LinkedBlockingDeque<>());
                 }
             }
         }
         return threadPoolExecutor;
    }
}
