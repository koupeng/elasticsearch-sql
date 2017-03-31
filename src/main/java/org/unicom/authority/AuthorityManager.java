package org.unicom.authority;


import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugin.nlpcn.ElasticHitsExecutor;
import org.elasticsearch.plugin.nlpcn.ElasticJoinExecutor;
import org.elasticsearch.plugin.nlpcn.GetIndexRequestRestListener;
import org.elasticsearch.plugin.nlpcn.MultiRequestExecutorFactory;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.nlpcn.es4sql.query.join.JoinRequestBuilder;
import org.nlpcn.es4sql.query.multi.MultiQueryRequestBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * 将Elasticsearch-sql引入到LogCenter中
 * 此模块进行权限认证
 * Created by koupeng on 2017/3/30 16:37.
 */
public class AuthorityManager {
    /**
     * 认证
     * @param queryAction
     * @param channel
     * @param userid 用户ID
     * @return
     */
    public static boolean authenticate(QueryAction queryAction,RestChannel channel,String userid){
        //不是从LogCenter访问的，直接给所有权限
        if(userid==null || "".equals(userid)){
            return true;
        }
        try{
            //step0:获取索引
            String []indices=getIndices(queryAction,channel);
            //step1:根据userid获取SystemMap
            HashMap<String,String> systemMap = MysqlUtil.getIndicesByUserid(userid);
            //step2:匹配
            int size = indices.length;
            for(int i=0;i<size;i++){
                String index = indices[i];
                String []indexGroup = index.split("_");
                if(null == indexGroup||indexGroup.length<0){
                    //sendErrorAuthenticateResponse(channel);
                    return false;
                }
                else{
                    //只取第一个
                    String index_input=indexGroup[0];
                    //校验是否存在
                    if(!systemMap.containsValue(index_input)){
                        //sendErrorAuthenticateResponse(channel);
                        return false;
                    }
                }
            }
            return true;

        }catch (Throwable e){
            e.printStackTrace();
            //sendErrorAuthenticateResponse(channel);
            return false;
        }
    }

    /**
     * 如果认证失败，发送认证失败结果到前台
     * @param channel
     */
    public static void sendErrorAuthenticateResponse(RestChannel channel,boolean flag) {
        String json_ok = "{\"认证\":\"成功\"}";
        String json_nok = "{\"认证\":\"失败\"}";
        try {
            BytesRestResponse bytesRestResponse=null;
            //失败
            if(!flag){
                bytesRestResponse = new BytesRestResponse(
                        RestStatus.UNAUTHORIZED, json_nok);

            }
            //成功
            else {
                bytesRestResponse = new BytesRestResponse(
                        RestStatus.OK, json_ok);
            }
            channel.sendResponse(bytesRestResponse);

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static String[] getIndices(QueryAction queryAction, RestChannel channel) throws Exception{
        SqlElasticRequestBuilder requestBuilder = queryAction.explain();
        ActionRequest request = requestBuilder.request();

        if (request instanceof SearchRequest) {
            //获取索引
            String []indices=((SearchRequest) request).indices();
            return indices;
        } else if (request instanceof DeleteByQueryRequest) {
            return null;
        }
        else if(request instanceof GetIndexRequest) {
            requestBuilder.getBuilder().execute( new GetIndexRequestRestListener(channel, (GetIndexRequest) request));
        }
        return null;
    }
}
