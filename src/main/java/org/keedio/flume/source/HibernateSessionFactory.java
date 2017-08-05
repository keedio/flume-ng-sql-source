package org.keedio.flume.source;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateSessionFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HibernateSessionFactory.class);
  private static final SessionFactory SESSION_FACTORY = buildSessionFactory();

  private static HibernateSessionFactory instance;

  private Session session;

  private HibernateSessionFactory() {}

  public static HibernateSessionFactory getInstance() {
    if (instance == null) instance = new HibernateSessionFactory();

    return instance;
  }

  private static SessionFactory buildSessionFactory() {
    try {
      Configuration configuration = config();
      Properties properties = configuration.getProperties();
      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(properties).build();

      return configuration.buildSessionFactory(serviceRegistry);

    } catch (Throwable ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  private static Configuration config() {
    Map<String, String> hibernateProperties =
        FlumeContext.getInstance().getContext().getSubProperties("hibernate.");
    LOG.debug(">>>> {}", hibernateProperties);

    Configuration config = new Configuration();

    for (Entry<String, String> entry : hibernateProperties.entrySet()) {
      config.setProperty("hibernate." + entry.getKey(), entry.getValue());
    }

    return config;
  }

  /** Connect to database using hibernate */
  private synchronized void establishSession() {

    LOG.info("Opening hibernate session");
    Boolean readOnlySession = FlumeContext.getInstance().getReadOnlySession();

    session = SESSION_FACTORY.openSession();
    session.setCacheMode(CacheMode.IGNORE);
    session.setDefaultReadOnly(readOnlySession);
  }

  /** Close database connection */
  private synchronized void closeSession() {

    LOG.info("Closing hibernate session");
    session.close();
  }

  public synchronized void resetConnection() throws InterruptedException {
    session.close();
    establishSession();
  }

  public synchronized Session getSession() throws InterruptedException {
    if (null == session) {
      establishSession();
    } else {
      if (!session.isConnected()) {
        resetConnection();
      }
    }

    return session;
  }

  public synchronized void shutdown() {
    closeSession();
    SESSION_FACTORY.close();
  }
}
