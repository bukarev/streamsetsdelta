import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import io.delta.standalone.Operation;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import io.delta.standalone.types.StructType;
import java.net.URI;
import java.io.DataOutputStream;

import groovy.json.JsonSlurper;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.Metadata.Builder;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.StructField;

public class constants {
    static public String containerName = "<YOUR CONTAINER NAME>";
    static public String accountName = "<YOUR ACCOUNT NAME>";
    static public String tableName = "<TABLE NAME>";
    static public String tenantID = "<TENANT ID>";
    static public String clientID = "<CLIENT ID>";
    static public String clientSecret = "<CLIENT SECRET>";
}

public class SimpleAzureClient {
    
    private String token;

    public SimpleAzureClient(String tenant, String client, String secret) {
        def jsonSlurper = new JsonSlurper();
        def azConn = new URL("https://login.microsoftonline.com/"+tenant+"/oauth2/v2.0/token").openConnection();
        azConn.setRequestMethod("POST");
        azConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");    
        azConn.setRequestProperty("Accept", "application/json");        
        azConn.setDoOutput(true); 
        DataOutputStream dos = new DataOutputStream(azConn.getOutputStream());

      String params = "client_id="+client+"&scope=%20offline_access%20https%3A%2F%2F"+constants.accountName+".dfs.core.windows.net%2F.default&client_secret="+secret+"&grant_type=client_credentials";
        byte[] input = params.getBytes("utf-8");
        dos.write(input, 0, input.length);
        dos.flush();
        dos.close();

        def getRC = azConn.getResponseCode();
        if (getRC == 200) {
            token = jsonSlurper.parseText(azConn.getInputStream().getText())['access_token'];
            azConn.disconnect();
        }

    }

    public long getFileSize(String path) {
            
      def azConn = new URL("https://" + constants.accountName + ".dfs.core.windows.net/" + constants.containerName + path + "?action=getStatus").openConnection();
        azConn.setRequestMethod("HEAD");
        azConn.setRequestProperty("Authorization", "Bearer " + token);    

        def getRC = azConn.getResponseCode();
        if (getRC == 200) {
            Long fileSize = azConn.getHeaderField("Content-Length").toLong();            
            azConn.disconnect();
            return fileSize;
        }
    }
}

public class SimpleAvro {

    private String jsonSchema;
    public SimpleAvro(String jsonSchema) {
        this.jsonSchema = jsonSchema;
    }
    
    public StructType toStruct() {
        def jsonSlurper = new JsonSlurper();
        def avroSchema = jsonSlurper.parseText(this.jsonSchema);
        StructType schema = new StructType();
      for(Map<String,Map<String,String>> f : avroSchema['fields']) {
            switch(f['type']) {
                case "string":
                schema = schema.add(new StructField(f['name'], new StringType(), true));
                break;
                case "int":
                schema = schema.add(new StructField(f['name'], new IntegerType(), true));
                break;
                case "long":
                schema = schema.add(new StructField(f['name'], new LongType(), true));
                break;
                case "double":
                schema = schema.add(new StructField(f['name'], new DoubleType(), true));
                break;
            }
        }        
return schema;
    }
}

if(sdc.records.size() > 0) {
    String strSchema = sdc.records[0].attributes['avroSchema'];
    Schema avroSchema = Schema.parse(strSchema);

String fileName = UUID.randomUUID().toString() + ".snappy.parquet";
records = sdc.records;

fsClient = new SimpleAzureClient(constants.tenantID, constants.clientID, constants.clientSecret);

Configuration conf = new Configuration();

conf.set("fs.azure.account.auth.type." + constants.accountName + ".dfs.core.windows.net", "OAuth");
conf.set("fs.azure.account.oauth.provider.type." + constants.accountName+ ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
// App ID
conf.set("fs.azure.account.oauth2.client.id." + constants.accountName + ".dfs.core.windows.net", constants.clientID);
// Secret
conf.set("fs.azure.account.oauth2.client.secret." + constants.accountName+ ".dfs.core.windows.net",constants.clientSecret);
// Tenant ID
conf.set("fs.azure.account.oauth2.client.endpoint." + constants.accountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/"+constants.tenantID +"/oauth2/token");

DeltaLog log = DeltaLog.forTable(conf, "abfss://" + constants.containerName + "@" + constants.accountName + ".dfs.core.windows.net/" + constants.tableName);

// if target table doesn't exist, create it
if (!log.tableExists()) {
    SimpleAvro myAvroSchema = new SimpleAvro(strSchema);
    StructType mydeltaSchema = myAvroSchema.toStruct();
    Metadata mb = new Metadata.Builder().name(constants.tableName).description(constants.tableName).schema(mydeltaSchema).build();
    OptimisticTransaction txn = log.startTransaction();

    List<Metadata> createTables = new ArrayList<>();

    createTables.add(mb);
    txn.commit(createTables, new Operation(Operation.Name.CREATE_TABLE), "Zippy/1.0.0");
}

// first, write a Parquet file into delta table's directory
String filePath = "/" + constants.tableName + "/" + fileName;
String fullFilePath = "abfss://" + constants.containerName + "@" + constants.accountName+ ".dfs.core.windows.net" + filePath;
Path path = new Path(fullFilePath);

try {

  ParquetWriter writer = AvroParquetWriter
      .builder(path)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withSchema(avroSchema)
      .build();

  for (record in records) {
    GenericData.Record recordOut = new GenericData.Record(avroSchema);
    for (String name : record.value.keySet()) {
        recordOut.put(name, record.value[name]);
      }              
            writer.write(recordOut);
  } // of record by record

  writer.close();

// Update the delta Log for a new file
  Long newFileSize = fsClient.getFileSize(filePath);
  Map<String, String> emptyMapfile = Collections.emptyMap();
  AddFile action1 = new AddFile(
            fullFilePath,
            emptyMapfile,
            newFileSize,
            System.currentTimeMillis(),
            true, // isDataChange
            null, // stats
            null  // tags
        );
  List<AddFile> totalCommitFiles = new ArrayList<>();
  totalCommitFiles.add(action1);

  OptimisticTransaction txn = log.startTransaction();
  String commResult = txn.commit(totalCommitFiles, new Operation(Operation.Name.UPDATE), "Zippy/1.0.0").toString();
  
// use the transaction result as an output record  
        newRecord = sdc.createRecord(1);
        newRecord.value['commitResult'] = commResult;     
        sdc.output.write(newRecord)
    } catch (e) {
        // Write a record to the error pipeline
        sdc.log.error(e.toString(), e)
        //sdc.error.write(record, e.toString())
    }
} // of checking for batch size