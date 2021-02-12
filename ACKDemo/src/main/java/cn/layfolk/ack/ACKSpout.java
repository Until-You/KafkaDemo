package cn.layfolk.ack;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

public class ACKSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
    }

    //往下游发送一个Hello world+" "+UUID
    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString();
        String message="Hello world "+uuid;
        //emit传递的数据是一个List,而Values extends ArrayList<Object>
        //outputCollector.emit(new Values(message));
        //为了保证每条数据被处理过一次,这里为每一个Event添加一个messageId作为跟踪的标志.
        outputCollector.emit(new Values(message),uuid);

        //如果不是生产环境,建议做一个延迟
        try {
            Thread.sleep(5*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功:"+msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败:"+msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hello_uuid"));
    }
}
