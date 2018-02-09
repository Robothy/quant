package quant.dao;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import quant.utils.HibernateUtil;

public class CommonDao {
	
	@SuppressWarnings("unchecked")
	public <T> List<T> findByHql(String hql, Class<T> clazz){
		Session session = HibernateUtil.getSession();
		Query query = session.createQuery(hql);
		List<T> result = query.list();
		return result;
	}
	
	public List<Object> findByHql(String hql){
		return findByHql(hql, null);
	}
	
	
	
}
