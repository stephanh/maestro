
hadoop jar maestro-example-assembly-0.5.0-20140704032331-e95b53e.jar
    com.twitter.scalding.Tool \
    au.com.cba.omnia.maestro.schema.jobs.Check \
    --hdfs \
    --schema acct_limt.schema \
    --input  /user/lippmebe-adm/acct_limt/000000_0 \
    --output /user/lippmebe-adm/maestro/acct_limt-check 

