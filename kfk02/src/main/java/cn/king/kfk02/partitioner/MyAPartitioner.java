package cn.king.kfk02.partitioner;

import kafka.producer.Partitioner;


/**
 * @author: wjl@king.cn
 * @time: 2020/12/6 10:33
 * @version: 1.0.0
 * @description: 自定义分区器.
 * 需求：将所有数据存储到topic的第0号分区上.
 * 定义一个类实现Partitioner接口，重写里面的方法即可 -- 过时API.
 */
public class MyAPartitioner implements Partitioner {

    public MyAPartitioner() {
        super();
    }

    @Override
    public int partition(Object key, int numPartitions) {
        // 控制分区
        return 0;
    }

}
