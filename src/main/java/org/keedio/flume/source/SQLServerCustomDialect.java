package org.keedio.flume.source;

import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.type.StandardBasicTypes;

import java.sql.Types;

public class SQLServerCustomDialect extends SQLServerDialect {

	/**
	 * Initializes a new instance of the {@link SQLServerDialect} class.
	 */
	public SQLServerCustomDialect(){	
		registerHibernateType(Types.ARRAY, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.BIGINT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.BINARY, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.BIT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.BLOB, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.BOOLEAN, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.CHAR, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.CLOB, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.DATALINK, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.DATE, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.DECIMAL, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.DISTINCT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.DOUBLE, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.FLOAT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.INTEGER, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.JAVA_OBJECT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.LONGNVARCHAR, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.LONGVARBINARY, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.LONGVARCHAR, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.NCHAR, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.NCLOB, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.NULL, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.NUMERIC, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.NVARCHAR, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.OTHER, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.REAL, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.REF, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.ROWID, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.SMALLINT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.SQLXML, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.STRUCT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.TIME, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.TIMESTAMP, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.TINYINT, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.VARBINARY, StandardBasicTypes.STRING.getName());
		registerHibernateType(Types.VARCHAR, StandardBasicTypes.STRING.getName());
	}
}