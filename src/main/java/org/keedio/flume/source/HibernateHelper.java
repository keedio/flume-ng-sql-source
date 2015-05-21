package org.keedio.flume.source;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateHelper {

	private static SessionFactory factory;
	private Session session;
	private Configuration config;
	private static final Logger log = LoggerFactory.getLogger(HibernateHelper.class);
	private SQLSourceHelper sqlSourceHelper;
	
	public HibernateHelper(SQLSourceHelper sqlSourceHelper){
		
		this.sqlSourceHelper = sqlSourceHelper;
		
		config = new Configuration()
	    .setProperty("hibernate.connection.url", sqlSourceHelper.getConnectionURL())
	    .setProperty("hibernate.connection.username", sqlSourceHelper.getUser())
		.setProperty("hibernate.connection.password", sqlSourceHelper.getPassword());
	}

	public void establishSession() {
		
		log.info("Opening hibernate session");
		
		ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();	
	}
	
	public void closeSession() {
		
		log.info("Closing hibernate session");
		
		session.close();
	}
	
	@SuppressWarnings("unchecked")
	public List<List <Object>> executeQuery(){
		
		List<List<Object>> rowsList = session.createSQLQuery(sqlSourceHelper.getQuery())
			    .setFirstResult(sqlSourceHelper.getCurrentIndex())
			    .setMaxResults(sqlSourceHelper.getMaxRows())
			    .setResultTransformer(Transformers.TO_LIST)
			    .list();
		
		sqlSourceHelper.setCurrentIndex(sqlSourceHelper.getCurrentIndex() + rowsList.size());
		
		return rowsList;
	}
}
