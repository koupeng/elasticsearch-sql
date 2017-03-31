package org.elasticsearch.plugin.nlpcn;

import com.alibaba.druid.support.json.JSONParser;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.nlpcn.executors.ActionRequestRestExecuterFactory;
import org.elasticsearch.plugin.nlpcn.executors.RestExecutor;
import org.elasticsearch.rest.*;
import org.nlpcn.es4sql.SearchDao;
import org.nlpcn.es4sql.query.QueryAction;
import org.nlpcn.es4sql.query.SqlElasticRequestBuilder;
import org.unicom.authority.AuthorityManager;

import java.util.Map;


public class RestSqlAction extends BaseRestHandler {

	@Inject
	public RestSqlAction(Settings settings, Client client, RestController restController) {
		super(settings, restController, client);
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_explain", this);
		restController.registerHandler(RestRequest.Method.POST, "/_sql", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql", this);
		//注册认证
		restController.registerHandler(RestRequest.Method.POST, "/_sql/_authenticate", this);
		restController.registerHandler(RestRequest.Method.GET, "/_sql/_authenticate", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, final Client client) throws Exception {
		String sql = request.param("sql");
		System.out.println("sql ="+sql);
		String userid=null;
		if (sql == null) {
			sql = request.content().toUtf8();
			JSONParser parser = new JSONParser(sql);
			Map map=  parser.parseMap();
			System.out.println("sql from content="+map.get("query"));
			System.out.println("userid from content="+map.get("userid"));

			sql=map.get("query")==null?"":map.get("query").toString();
			userid=map.get("userid")==null?"":map.get("userid").toString();
		}
		SearchDao searchDao = new SearchDao(client);
		QueryAction queryAction= searchDao.explain(sql);
		if(request.path().endsWith("/_authenticate")){
			System.out.println("_authenticate =");
			//校验
			boolean flag = AuthorityManager.authenticate(queryAction,channel,userid);
			System.out.println("flag ="+flag);
			AuthorityManager.sendErrorAuthenticateResponse(channel,flag);
			return;
		}

		// TODO add unittests to explain. (rest level?)
		if (request.path().endsWith("/_explain")) {
			String jsonExplanation = queryAction.explain().explain();
			BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, jsonExplanation);
			channel.sendResponse(bytesRestResponse);
		} else {
            Map<String, String> params = request.params();
            RestExecutor restExecutor = ActionRequestRestExecuterFactory.createExecutor(params.get("format"));
			restExecutor.execute(client,params,queryAction,channel);
		}
	}
}