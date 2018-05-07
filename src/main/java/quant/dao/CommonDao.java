package quant.dao;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hibernate.query.Query;
import org.hibernate.Session;

import quant.utils.HibernateUtil;

public class CommonDao {
	
	/**
	 * 通过hql和class获取数据对象
	 * @param hql 查询语句
	 * @param clazz 查询的类
	 * @return
	 */
	public <T> List<T> findByHql(String hql, Class<T> clazz){
		Session session = HibernateUtil.getSession();
		Query<T> query = session.createQuery(hql, clazz);
		List<T> result = query.getResultList();
		return result;
	}
	
	/**
	 * 通过hql和class获取数据对象
	 * @param hql 查询语句
	 * @param parameters 查询参数
	 * @param clazz 查询的类
	 * @return
	 */
	public <T> List<T> findByHql(String hql, Map<String, Object> parameters,Class<T> clazz){
		Session session = HibernateUtil.getSession();
		Query<T> query = session.createQuery(hql, clazz);
		for(Entry<String, Object> entry : parameters.entrySet()){
			query.setParameter(entry.getKey(), entry.getValue());
		}
		List<T> result = query.getResultList();
		return result;
	}
	
	/**
	 * 保存或更新一个对象
	 * @param object
	 */
	public void saveOrUpdate(Object object){
		Session session = HibernateUtil.getSession();
		session.beginTransaction();
		session.saveOrUpdate(object);
		session.getTransaction().commit();
		session.close();
	}
}
