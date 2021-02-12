package cn.layfolk.ack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ABolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //1.模拟处理了消息
        //tuple(从ACKSpout发送出来的)---><"hello_uuid","Hello world "+uuid>
        //String message = tuple.getStringByField("hello_uuid");
        String message = tuple.getString(0);
        System.out.println("ABolt 处理了消息:"+message);
        //2.往下游发送数据时,必须加上上游接收到的Tuple
        outputCollector.emit(tuple,new Values(message));
        //3.通知ACKBolt组件,本组件处理的结果
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hello_uuid"));
    }

}
