package quant.utils;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

/**
 * @author robothy
 *
 */
public class HibernateUtil {
	private static final StandardServiceRegistry REGISTRY = new StandardServiceRegistryBuilder()
			.configure().build();
	private static SessionFactory FACTORY = null;
    static{
    	FACTORY = new org.hibernate.boot.MetadataSources(REGISTRY).buildMetadata().buildSessionFactory();
    }
    
    public static Session getSession(){
    	return FACTORY.openSession();
    }
    
    public static Session getCurrentSession(){
    	return FACTORY.getCurrentSession();
    }
    
    public static void closeSessionFactory(){
    	FACTORY.close();
    }
}
