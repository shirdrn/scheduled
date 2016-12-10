package cn.shiyanjun.platform.scheduled.dao;

import java.io.IOException;
import java.io.Reader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

public final class DaoFactory {

	private static final Log LOG = LogFactory.getLog(DaoFactory.class);
	public static final String DEFAULT_CONF_FILE = "mybatis-config.xml";
	private static SqlSessionFactory sqlSessionFactory;

	private DaoFactory() {
		super();
	}

	public SqlSession getSqlSession() {
		return sqlSessionFactory.openSession(true);
	}

	private static void loadConfiguration(String filePath) {
		Reader reader = null;
		try {
			reader = Resources.getResourceAsReader(filePath);
			sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.warn("Failed to close reader: " + e.getMessage());
				}
			}
		}
	}

	public SqlSessionFactory getSqlSessionFactory() {
		Preconditions.checkArgument(sqlSessionFactory != null);
		return sqlSessionFactory;
	}

	public static DaoFactory newInstance() {
		return newInstance(true);
	}

	public static DaoFactory newInstance(boolean autoCommit) {
		return newInstance(DEFAULT_CONF_FILE, autoCommit);
	}

	private static DaoFactory newInstance(String fileName, boolean autoCommit) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName), "Invalid mybatis config file!");
		loadConfiguration(fileName);
		return new DaoFactory();
	}

}
