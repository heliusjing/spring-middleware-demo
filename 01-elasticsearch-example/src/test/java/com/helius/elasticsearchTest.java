package com.helius;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

/**
 * @Author Helius
 * @Create 2020-05-03-9:17 下午
 */
public class elasticsearchTest {
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 9300;
    public static final String CLUSTERNAME="elasticsearch";

    public static TransportClient getConnection() throws UnknownHostException {
        Settings settings = Settings.builder().put("client.transport.sniff", true)
                .put("cluster.name", CLUSTERNAME)
                .build();
        TransportClient esClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), PORT));
        return esClient;
    }

    public void add() {
        try {
            XContentBuilder content = XContentFactory.jsonBuilder().startObject()
                    .field("name", "LYC")
                    .field("age", 24)
                    .field("job", "coder")
                    .endObject();
            String index = "data";
            String type = "person";
            String id = "1";
            TransportClient esClient = this.getConnection();
            IndexResponse indexResponse = esClient.prepareIndex(index, type, id).setSource(content).get();
            esClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据
     */
    public void get(String index, String type, String id) throws Exception {
        TransportClient client = this.getConnection();
        GetResponse response = client.prepareGet(index, type, id).get();
        System.out.println("response = " + response);
        System.out.println(response.getSourceAsString());
        System.out.println(response.getType());
        System.out.println(response.getVersion());
        System.out.println(response.getIndex());
        System.out.println(response.getId());
        client.close();

    }

    /**
     * 添加map数据
     */
    public void addMap() throws Exception {
        HashMap<String, Object> map = new HashMap<>();
        map.put("userName","LYC");
        map.put("sendDate",new Date());
        map.put("msg","hello world");
        TransportClient esClient = this.getConnection();
        IndexResponse response = esClient.prepareIndex("momo", "msg", "1")
                .setSource(map).get();
        System.out.println("map索引名称:"+response.getIndex()+"\n map类型:"+response.getType()+"\n map文档ID:"+response.getId()+"\n当前实例状态:"+response.status());
    }

    public static void main(String[] args) throws Exception {
        elasticsearchTest test = new elasticsearchTest();
//        test.get("momo","msg","1");
        test.add();
    }
}

