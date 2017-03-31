package org.unicom.authority;

import java.util.ArrayList;
import java.util.HashMap;
/**
 * 将Elasticsearch-sql引入到LogCenter中
 * 此模块进行权限认证
 * Created by koupeng on 2017/3/30 16:37.
 */
public class AuthorityManager {

    /**
     * 认证
     * @param userid 用户ID
     * @param indices 此indices在前台经过了处理，只包含业务系统ID
     * @return
     */
    public static boolean authenticate(String userid,ArrayList<String> indices){
        //不是从LogCenter访问的，直接给所有权限
        if(userid==null || "".equals(userid) || indices == null || indices.size()<0){
            return true;
        }
        try{
            //step1:根据userid获取SystemMap
            HashMap<String,String> systemMap = MysqlUtil.getIndicesByUserid(userid);
            //step2:匹配
            int size = indices.size();
            for(int i=0;i<size;i++){
                String index = indices.get(i);
                if(!systemMap.containsValue(index)){
                    return false;
                }
            }
            return true;

        }catch (Throwable e){
            e.printStackTrace();
            return false;
        }
    }
}
