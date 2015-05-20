package org.keedio.flume.source;

import java.util.List;

import org.apache.flume.Context;
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
	private static ServiceRegistry serviceRegistry;
	private Session session;
	private Configuration config;
	private static final Logger log = LoggerFactory.getLogger(HibernateHelper.class);
	
	public HibernateHelper(Context context){
		config = new Configuration()
	    .setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLDialect")
	    .setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver")
	    .setProperty("hibernate.connection.url", "jdbc:mysql://dos:3306/employees")
	    .setProperty("hibernate.connection.username", "mvalle")
		.setProperty("hibernate.connection.password", "mvalle")
		.setProperty("hibernate.connection.pool_size", "5");
	}
	
	/*
	public void loadConfig(Context context){
		config = new Configuration()
	    .setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLDialect")
	    .setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver")
	    .setProperty("hibernate.connection.url", "jdbc:mysql://dos:3306/employees")
	    .setProperty("hibernate.connection.username", "mvalle")
		.setProperty("hibernate.connection.password", "mvalle")
		.setProperty("hibernate.connection.pool_size", "5");
		
		
		hibernate.connection.driver_class
		hibernate.connection.url
		hibernate.connection.username
		hibernate.connection.password
		hibernate.connection.pool_size
		hibernate.c3p0.min_size=1
		hibernate.c3p0.max_size=10
		hibernate.c3p0.timeout=1800
		hibernate.c3p0.max_statements=10
		
	}*/

	public void establishSession() {
		
		ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
				.applySettings(config.getProperties()).build();
		factory = config.buildSessionFactory(serviceRegistry);
		session = factory.openSession();
		//session.beginTransaction();  
		
	}
	
	@SuppressWarnings("unchecked")
	public List<List <Object>> executeQuery(SQLSourceHelper sqlSourceHelper){
		
		List<List<Object>> rowsList = session.createSQLQuery(sqlSourceHelper.getQuery())
			    //.setLong(0, sqlSourceHelper.getCurrentIndex())
			    .setFirstResult(sqlSourceHelper.getCurrentIndex())
			    .setMaxResults(sqlSourceHelper.getMaxRows())
			    .setResultTransformer(Transformers.TO_LIST)
			    .list();
		
		sqlSourceHelper.setCurrentIndex(sqlSourceHelper.getCurrentIndex() + rowsList.size());
		
		//session.getTransaction().commit();  
		return rowsList;
	}
}
