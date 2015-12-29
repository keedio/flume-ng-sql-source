package org.keedio.flume.source;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.Context;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *
 */
public class HibernateHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HibernateHelper.class);

	private static SessionFactory factory;
	private Session session;
	private ServiceRegistry serviceRegistry;
	private Configuration config;
	private SQLSourceHelper sqlSourceHelper;

	/**
	 * Constructor to initialize hibernate configuration parameters
	 * @param sqlSourceHelper Contains the configuration parameters from flume config file
	 */
	public HibernateHelper(SQLSourceHelper sqlSourceHelper) {

		this.sqlSourceHelper = sqlSourceHelper;
		Context context = sqlSourceHelper.getContext();
		
		Map<String,String> hibernateProperties = context.getSubProperties("hibernate.");
		Iterator<Map.Entry<String,String>> it = hibernateProperties.entrySet().iterator();
		
		config = new Configuration();
		Map.Entry<String, String> e;
		
		while (it.hasNext()){
			e = it.next();
			config.setProperty("hibernate." + e.getKey(), e.getValue());
		}
	}

	/**
	 * Connect to database using hibernate
	 */
	public void establishSession() {

		LOG.info("Opening hibernate session");

		serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
	}

	/**
	 * Close database connection
	 */
	public void closeSession() {

		LOG.info("Closing hibernate session");

		session.close();
		factory.close();
	}

	/**
	 * Execute the selection query in the database
	 * @return The query result. Each Object is a cell content. <p>
	 * The cell contents use database types (date,int,string...), 
	 * keep in mind in case of future conversions/castings.
	 */
	@SuppressWarnings("unchecked")
	public List<List<Object>> executeQuery() {
		
		List<List<Object>> rowsList ;
		
		if (sqlSourceHelper.isCustomQuerySet()){
			if (sqlSourceHelper.getMaxRows() == 0){
				rowsList = session
						.createSQLQuery(sqlSourceHelper.buildQuery())
						.setResultTransformer(Transformers.TO_LIST).list();
			}else{
				rowsList = session
					.createSQLQuery(sqlSourceHelper.buildQuery())
					.setMaxResults(sqlSourceHelper.getMaxRows())
					.setResultTransformer(Transformers.TO_LIST).list();
			}
			if (!rowsList.isEmpty())
				sqlSourceHelper.setCurrentIndex(rowsList.get(rowsList.size()-1).get(0).toString());
		}
		else
		{
			if (sqlSourceHelper.getMaxRows() == 0){
				rowsList = session
						.createSQLQuery(sqlSourceHelper.getQuery())
						.setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()))
						.setResultTransformer(Transformers.TO_LIST).list();
			}else{
				rowsList = session
					.createSQLQuery(sqlSourceHelper.getQuery())
					.setFirstResult(Integer.parseInt(sqlSourceHelper.getCurrentIndex()))
					.setMaxResults(sqlSourceHelper.getMaxRows())
					.setResultTransformer(Transformers.TO_LIST).list();
			}
			sqlSourceHelper.setCurrentIndex(sqlSourceHelper.getCurrentIndex()
					+ rowsList.size());
		}
		
		

		return rowsList;
	}

	public void resetConnectionAndSleep() throws InterruptedException {
		
		long startTime = System.currentTimeMillis();
		
		session.close();
		factory.close();
		establishSession();
		
		long execTime = System.currentTimeMillis() - startTime;
		
		if (execTime < sqlSourceHelper.getRunQueryDelay())
			Thread.sleep(sqlSourceHelper.getRunQueryDelay() - execTime);
	}
}
