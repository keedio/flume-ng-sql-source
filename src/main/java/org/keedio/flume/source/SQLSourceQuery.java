package org.keedio.flume.source;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage hibernate sessions and perform queries
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public class SQLSourceQuery {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSourceQuery.class);

  private SQLSourceStatus sqlSourceStatus;

  /**
   * Constructor to initialize hibernate configuration parameters
   *
   * @param sqlSourceStatus Contains the configuration parameters from flume config file
   */
  public SQLSourceQuery(SQLSourceStatus sqlSourceStatus) {
    this.sqlSourceStatus = sqlSourceStatus;
  }

  /**
   * Execute the selection query in the database
   *
   * @return The query result. Each Object is a cell content.
   *     <p>The cell contents use database types (date,int,string...), keep in mind in case of
   *     future conversions/castings.
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public List<List<Object>> executeQuery() throws InterruptedException {

    List<List<Object>> rowsList = new ArrayList<List<Object>>();
    Query query;

    Session session = HibernateSessionFactory.getInstance().getSession();

    if (sqlSourceStatus.isCustomQuerySet()) {

      query = session.createSQLQuery(sqlSourceStatus.buildQuery());

      if (FlumeContext.getInstance().getMaxRows() != 0) {
        query = query.setMaxResults(FlumeContext.getInstance().getMaxRows());
      }
    } else {
      query =
          session
              .createSQLQuery(sqlSourceStatus.getQuery())
              .setFirstResult(Integer.parseInt(sqlSourceStatus.getCurrentIndex()));

      if (FlumeContext.getInstance().getMaxRows() != 0) {
        query = query.setMaxResults(FlumeContext.getInstance().getMaxRows());
      }
    }

    try {
      rowsList =
          query
              .setFetchSize(FlumeContext.getInstance().getMaxRows())
              .setResultTransformer(Transformers.TO_LIST)
              .list();
    } catch (Exception e) {
      LOG.error("Exception thrown, resetting connection.", e);
      HibernateSessionFactory.getInstance().resetConnection();
    }

    if (!rowsList.isEmpty()) {
      if (sqlSourceStatus.isCustomQuerySet()) {
        sqlSourceStatus.setCurrentIndex(rowsList.get(rowsList.size() - 1).get(0).toString());
      } else {
        sqlSourceStatus.setCurrentIndex(
            Integer.toString(
                (Integer.parseInt(sqlSourceStatus.getCurrentIndex()) + rowsList.size())));
      }
    }

    return rowsList;
  }

  /**
   * Converter from a List of Object List to a List of String arrays
   *
   * <p>Useful for csvWriter
   *
   * @param queryResult Query Result from hibernate executeQuery method
   * @return A list of String arrays, ready for csvWriter.writeall method
   */
  public List<String[]> getAllRows(List<List<Object>> queryResult) {

    List<String[]> allRows = new ArrayList<String[]>();

    if (queryResult == null || queryResult.isEmpty()) {
      return allRows;
    }

    String[] row = null;

    for (int i = 0; i < queryResult.size(); i++) {
      List<Object> rawRow = queryResult.get(i);
      row = new String[rawRow.size()];
      for (int j = 0; j < rawRow.size(); j++) {
        if (rawRow.get(j) != null) {
          row[j] = rawRow.get(j).toString();
        } else {
          row[j] = "";
        }
      }
      allRows.add(row);
    }

    return allRows;
  }
}
