# Schema Validation and Transformation POC

The purpose of this POC is to implement simple validation of schema and data, and simple transformation using DynamoDB for configuration.


## What have been implemented
1. Configuration is stored in DynamoDB swmi_<env>_obj_mapping. CFN environment-dynamodb.yaml creates this table.
2. Design the schema of this table. An item example is as below to describe the schema.

```
{
   "tobj":"customer",                                           # target object
   "sobj":"raw_customer",                                       # source object
   "sconn":{                                                    # source connection details
      "options":{                                                   # Glue pyspark connection_options
         "paths":[
            "s3://yh-swmi-test/landing/raw_customer/"
         ]
      },
      "format":"csv",                                               # Glue pyspark connection format
      "format_options":{                                            # Glue pyspark connection format_options
         "withHeader":"true"
      },
      "type":"s3"                                                   # Glue pyspark connection type
   },
   "exconn":{                                                   # exception (error records) connection details
      "type":"s3",                                                  # Glue pyspark connection type
      "options":{                                                   # Glue pyspark connection options
         "path":"s3://yh-swmi-test/except/ex_customer/"
      },
      "format":"parquet"                                            # Glue pyspark connection format
   },
   "tconn":{                                                    # target connection details
      "type":"s3",                                                  # Glue pyspark connection type
      "options":{                                                   # Glue pyspark connection options
         "path":"s3://yh-swmi-test/export/customer/"
      },
      "format":"parquet"                                            # Glue pyspark connection format
   },
   "mappings":[                                                 # list of definition column mapping and validation from source object to target object
      {
         "tgt":{                                                    # target column details
            "name":"cust_id",                                           # column name
            "type":"string"                                             # data type
         },
         "map_type":"sql",                                          # mapping type. "sql" is the only option. It uses pyspark sql. simply cast data type only if omitted.
         "map_rule":"concat('SWM_', raw_id)",                       # mapping rule in pyspark sql. simply cast data type only if omitted.
         "src":{                                                    # source column details. no source column if omitted.
            "name":"raw_id",                                            # column name
            "type":"string",                                            # data type
            "vals":[                                                # list of validation definition. No validation if this omitted.
               {                                                        # the 1st rule
                  "type":"re",                                              # validation type, re: regular expression (pyspark style). sql: pyspark sql
                  "msg":"raw_id must be digit",                             # error message for invalid record. Use source column name if omitted.
                  "rule":"^\\d+$"                                           # rule, regular expression if type=re or pyspark sql if type=sql
               },
               {                                                        # the 2nd rule
                  "type":"sql",
                  "msg":"raw_id must be less than 1000",
                  "rule":"cast(raw_id as int) < 1000"
               }
            ]
         }
      },
      {                                                             # no mapping rule is defined. Therefore, only cast string from source to int in target.
         "tgt":{
            "name":"cust_age",
            "type":"int"
         },
         "src":{
            "name":"raw_age",
            "type":"string",
            "vals":[
               {
                  "type":"sql",
                  "msg":"raw_age must be between 18 and 65",
                  "rule":"cast(raw_age as int) between 18 and 65"
               }
            ]
         }
      },
      {                                                                     # No source column is defined. Therefore, create the target column from the sql only.
         "tgt":{
            "name":"update_ts",
            "type":"timestamp"
         },
         "map_rule":"current_timestamp()",
         "map_type":"sql"
      },
      {                                                                     # No mapping is defined. Therefore, create the target column is cast from source column.
         "tgt":{
            "name":"cust_comment",
            "type":"string"
         },
         "src":{
            "name":"comment",
            "type":"string",
            "vals":[
               {
                  "type":"re",
                  "msg":"comment must start with comment",
                  "rule":"^comment"
               }
            ]
         }
      }
   ]
}

```

3. A Glue job to carry out validation and transformation with rules defined in the DynamoDB.
Reuse the glue/demo/hello/hello.py glue job for POC.

## What will be added in the future

1. Integrate with Lambda function which initialize the pipeline
2. Add support to split validation and transformation
3. Add metadata columns
4. Add error level, such as warning, reject record, reject file and reject batch
