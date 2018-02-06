package quant.dao;
// Generated 2018-2-6 18:26:35 by Hibernate Tools 4.3.1.Final

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import quant.entity.LiveOrderPairs;

/**
 * Home object for domain model class LiveOrderPairs.
 * @see quant.entity.LiveOrderPairs
 * @author Hibernate Tools
 */
public class LiveOrderPairsHome {

	private static final Log log = LogFactory.getLog(LiveOrderPairsHome.class);

	@PersistenceContext
	private EntityManager entityManager;

	public void persist(LiveOrderPairs transientInstance) {
		log.debug("persisting LiveOrderPairs instance");
		try {
			entityManager.persist(transientInstance);
			log.debug("persist successful");
		} catch (RuntimeException re) {
			log.error("persist failed", re);
			throw re;
		}
	}

	public void remove(LiveOrderPairs persistentInstance) {
		log.debug("removing LiveOrderPairs instance");
		try {
			entityManager.remove(persistentInstance);
			log.debug("remove successful");
		} catch (RuntimeException re) {
			log.error("remove failed", re);
			throw re;
		}
	}

	public LiveOrderPairs merge(LiveOrderPairs detachedInstance) {
		log.debug("merging LiveOrderPairs instance");
		try {
			LiveOrderPairs result = entityManager.merge(detachedInstance);
			log.debug("merge successful");
			return result;
		} catch (RuntimeException re) {
			log.error("merge failed", re);
			throw re;
		}
	}

	public LiveOrderPairs findById(int id) {
		log.debug("getting LiveOrderPairs instance with id: " + id);
		try {
			LiveOrderPairs instance = entityManager.find(LiveOrderPairs.class, id);
			log.debug("get successful");
			return instance;
		} catch (RuntimeException re) {
			log.error("get failed", re);
			throw re;
		}
	}
}
