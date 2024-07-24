The demo is ment to play with in an interactive way from databricks. Even thought the demo is runnable from the VSCode, executing a notebook all at once may not provide the correnc outcome.
The DLT notebook needs to be redefined more heavily to be executable on aws. It also needs a setup from the DLT UI, so this one notebook will not be easily executable on your own. Also, it is not executable from VSCode as the DLT uses different type of cluster.
If you want to try it, 


Prerequisities to run the demo:
Access to predefined s3 location:
  In cluster Environment variables have following lines:
  
  CHECKPOINTPATH=dbfs:/tmp/dbx_workshop_checkpoints
  SCHEMAPATH=dbfs:/tmp/dbx_workshop_schemas
  DATA_LOCATION=s3a://{{secrets/<<scope>>/<<access_key>>}}:{{secrets/<<scope>>/<<encoded_secret_key>>}}@{aws_bucket_name}

  Fill with real values e.g.:
  DATA_LOCATION=s3a://{{secrets/aws_scope/access_key}}:{{secrets/aws_scope/encoded_secret_key}}@bucketname

  Note: there is a notebook secrets_helper for creating secret scopes and secrets