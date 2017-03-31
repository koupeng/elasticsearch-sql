package org.unicom.authority;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class MysqlUtil {
	/**
	 * 获取MysqlURL
	 * @return
	 */
	public static String getURL(){
		String url=
				"jdbc:mysql://10.161.24.231:3306/logcenter?useUnicode=true&characterEncoding=utf8";
		return url;
	}

	/**
	 * 关闭MySQL
	 * @param rs
	 * @param stmt
	 * @param conn
	 */
	public static void mysql_close( ResultSet rs, Statement stmt,Connection conn){
		try {
			if(rs!=null){
				rs.close();
			}
			if(stmt!=null){
				stmt.close();
			}
			if(conn!=null){
				conn.close();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * 根据用户ID，获取可用的索引
	 * @param userid
	 * @return
	 */
	public static HashMap<String,String> getIndicesByUserid(String userid){
		HashMap<String,String> systemMap = new HashMap<String,String>();
		Connection conn=null;
		Statement stmt=null;
		try{
		   /*
			* 连接MySQL数据库
			*/
		   String url=getURL();
		   Class.forName("com.mysql.jdbc.Driver");
		   conn=(Connection) DriverManager.getConnection(url,"logadmin","log123");
		   stmt=(Statement) conn.createStatement();
		   //step1-获取业务系统
		   StringBuffer sql=new StringBuffer("select a.system_id systemId,a.system_name systemName from lc_g_system a,lc_G_SYSTEM_STAFF b " +
					" where a.system_id=b.system_id and b.staff_id='");
		   sql.append(userid).append("'");


			ResultSet rs=stmt.executeQuery(sql.toString());

			while(rs.next()){
				String systemId = rs.getString("systemId");
				String systemName = rs.getString("systemName");
				systemMap.put(systemId,systemName);
				//System.out.println("systemId:"+systemId+" systemName："+systemName);
			}

		   ///依次关闭
			mysql_close(rs,stmt,conn);

		   return systemMap;
	    }catch(Exception ee){
		   ee.printStackTrace();
		   return systemMap;
	    }
	}

	public static void main(String[]args){
		getIndicesByUserid("admin");
	}
}
