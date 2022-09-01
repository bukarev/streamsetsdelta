import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.StructField;

Configuration conf = new Configuration();

conf.set("fs.azure.account.auth.type.csromanstorage.dfs.core.windows.net", "OAuth");
conf.set("fs.azure.account.oauth.provider.type.csromanstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
// App ID
conf.set("fs.azure.account.oauth2.client.id.csromanstorage.dfs.core.windows.net", "<YOUR REGISTERED APP ID>");
// Secret
conf.set("fs.azure.account.oauth2.client.secret.csromanstorage.dfs.core.windows.net","<YOUR CLIENT SECRET>");
// Tenant ID
conf.set("fs.azure.account.oauth2.client.endpoint.csromanstorage.dfs.core.windows.net", "https://login.microsoftonline.com/<YOUR TENANT ID>/oauth2/token");

DeltaLog log = DeltaLog.forTable(conf, "abfss://delta@csromanstorage.dfs.core.windows.net/test2");

Snapshot latestSnapshot = log.snapshot();
CloseableIterator dataIter = latestSnapshot.open();

cur_batch = sdc.createBatch();

entityName = '';
recCount = 0;
hasNext = true;

while (dataIter.hasNext() && hasNext) {

      recCount = recCount + 1;

      record = sdc.createRecord('generated data '+ recCount.toString());
      try{
      
        RowRecord row = dataIter.next();
        def names = row.getSchema().getFieldNames();
        record.value = sdc.createMap(true);
        for(String f : names) {
          switch(row.getSchema().get(f).getDataType()) {
                case StringType:
                     record.value[f] = row.getString(f);
                     break;
                case LongType:
                     record.value[f] = row.getLong(f);
                     break;
                case IntegerType:
                     record.value[f] = row.getInt(f);
                     break;
                case DoubleType:
                     record.value[f] = row.getDouble(f);
                     break;
                default:
                     break;
            }
        }

        cur_batch.add(record);

        
        // if the batch is full, process it and start a new one
        if (cur_batch.size() >= sdc.batchSize) {
            // blocks until all records are written to all destinations
            // (or failure) and updates offset
            // in accordance with delivery guarantee
            cur_batch.process(entityName, recCount.toString());
            cur_batch = sdc.createBatch();
            if (sdc.isStopped()) {
                hasNext = false;
            }
        }
      } catch (Exception e) {
        cur_batch.addError(record, e.toString())
        cur_batch.process(entityName, recCount.toString())
        hasNext = false;
      }
    } // of dataIter
  

// send out what's left in the last batch
if (cur_batch.size() + cur_batch.errorCount() + cur_batch.eventCount() > 0) {
    cur_batch.process(entityName, recCount.toString())
}

    dataIter.close();