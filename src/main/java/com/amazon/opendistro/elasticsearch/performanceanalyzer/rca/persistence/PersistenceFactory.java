package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.sql.SQLException;
import java.util.Map;

/**
 * The interfaces are called to read data for a resource by the runtime before calling the operate
 * method on the node, The runtime also calls it after the operate completes and returns the value
 * to persist the results. The write of the persistor is also called by the network interfaces to
 * persist what the remote node sends. The persistor, is adaptor based. It takes in which data-store
 * to persist the data in as mentioned in the rca.conf file. The available data-stores can be files
 * on disk in some format, SQl lite or S3 or anything new we want to persist to tomorrow. Users in
 * OSS can write an adaptor to their favorite data store.
 */
public class PersistenceFactory {
  public static Persistable create(RcaConf rcaConf) throws MalformedConfig, SQLException {
    Map<String, String> datastore = rcaConf.getDatastore();
    switch (datastore.get(RcaConsts.DATASTORE_TYPE_KEY)) {
      case "sqlite":
      case "SQLite":
      case "SQLITE":
        return new SQLitePersistor(
            datastore.get(RcaConsts.DATASTORE_LOC_KEY),
            datastore.get(RcaConsts.DATASTORE_FILENAME));
      default:
        String err =
            String.format(
                "The datastore value can only be %s, %s or %s", "sqlite", "SQLite", "SQLITE");
        throw new MalformedConfig(rcaConf.getConfigFileLoc(), err);
    }
  }
}
