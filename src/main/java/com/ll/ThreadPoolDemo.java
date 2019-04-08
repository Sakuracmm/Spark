package com.ll;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author liuliang
 * @Date   2019/4/3 0003 19:22
 */
public class ThreadPoolDemo {

    public static void main(String[] args) {

        //创建一个单线程的线程池
//        ExecutorService pool = Executors.newSingleThreadExecutor();

        //固定大小的线程池，一次可以有nThread个进程并发执行
//        ExecutorService pool = Executors.newFixedThreadPool(5);

        //可缓冲的线程池(可以有多个线程，最大数量视机器实际硬件决定)
        ExecutorService pool = Executors.newCachedThreadPool();

        for(int i = 1; i<= 20; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //打印当前线程的名字
                    System.out.println(Thread.currentThread().getName() + " is over.");
                }
            });
//            System.out.println("all tasks are submitted");
//            pool.shutdownNow();
        }

    }

}
