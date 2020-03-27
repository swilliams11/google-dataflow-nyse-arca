# google-dataflow-nyse-arca

This repository demonstrates how to create a DataFlow ETL to read data GCP cloud storage, process it, and write it to 
GCP BigQuery.  This particular example uses a large data set (12GB extracted), so there could be a significant cost
to running this example in your GCP environment.

## Getting started  

### Prerequisites
#### 1. GCP Setup. 
* [Create a GCP bucket.](https://cloud.google.com/storage/docs/creating-buckets)
* [Enabled the APIs.](https://console.cloud.google.com/flows/enableapi?apiid=dataflow,compute_component,logging,storage_component,storage_api,bigquery,pubsub,datastore.googleapis.com,cloudresourcemanager.googleapis.com)
* [Create a GCP Service Account](https://console.cloud.google.com/apis/credentials/serviceaccountkey) and download the credential to your local machine. 
  * From the Service account list, select New service account.
  * In the Service account name field, enter a name.
  * From the Role list, select Project > Owner.
* [Create a BigQuery dataset.](https://cloud.google.com/bigquery/docs/datasets#create-dataset)

  `bq mk your_project_id:nyse_arca_java`
  
#### 2. Data Preparation 
* Download the file named `EQY_US_ALL_ARCA_BOOK_20130403.csv.gz` from the [NYSE Arca's FTP site](ftp://ftp.nyxdata.com/Historical%20Data%20Samples/TAQ%20NYSE%20ArcaBook/).

TODO - add curl command to download and upload to GCP here.

* You must unzip the `.gz` file once you upload it to GCP.  Follow the [Bulk decompress Cloud Storage file template](https://cloud.google.com/dataflow/docs/guides/templates/provided-utilities#bulkdecompressgcsfiles).

* Upload the `big-query-schema.json` to Google Cloud Storage.
Upload the file with this [documentation](https://cloud.google.com/storage/docs/uploading-objects).

  `gsutil cp big-query-schema.json gs://[DESTINATION_BUCKET_NAME]/`

##### Optional
You can create a smaller file from the larger file with the following linux command and then upload this file instead.

`sed -n -e '1,10000p' EQY_US_ALL_ARCA_BOOK_20130403.csv > EQY_US_ALL_ARCA_BOOK_20130403_smallfile.csv` 

#### 3. Setup your local environment. 
Open your IDE (IntelliJ) and then open the terminal tab and execute the following commands.
* `export PROJECT=$(gcloud config get-value core/project)`
* `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credential.json`
* `export BUCKET=parent_path_to_gcp_bucket_you_created_earlier`


#### 4. From your IDE (IntelliJ) terminal tab
* Execute the following:
```shell script
mvn clean compile
```

This creates a batch job description named `nys-arca-book-job-all-spec.json`.
```shell script
 mvn compile exec:java -Dexec.mainClass=com.swilliams11.googlecloud.dataflow.nyse.arca.NyseArcaTransform -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
 --project=$PROJECT \
 --stagingLocation=gs://$BUCKET/staging \
 --tempLocation=gs://$BUCKET/temp \
 --templateLocation=gs://$BUCKET/nyse-arca-book-job-all-spec.json \
 --runner=DataflowRunner"
```
##### NYSE - 14GB file
Execute the job with the required options.  BE CAREFUL - This command assumes your are using the **14GB file** and it consumes GCP resources.
  
```shell script
gcloud dataflow jobs run nyse-arca-book-java-all \                                                                                            
 --gcs-location=gs://$BUCKET/nyse-arca-book-job-all-spec.json \
 --zone=us-central1-f \
 --parameters=JSONPath=gs://$BUCKET/big-query-schema.json,inputFilePattern=gs://$BUCKET/EQY_US_ALL_ARCA_BOOK_20130403.csv,outputTable=$PROJECT:nyse_arca_java.eqy_arca_book_20130403_all,bigQueryLoadingTemporaryDirectory=gs://$BUCKET/bq_load_temp/
```

##### NYSE - Small File (10,000 records)
Execute the following command if you created the smaller file. 

```shell script
gcloud dataflow jobs run nyse-arca-book-java-all \                                                                                            
 --gcs-location=gs://$BUCKET/nyse-arca-book-job-all-spec.json \
 --zone=us-central1-f \
 --parameters=JSONPath=gs://$BUCKET/big-query-schema.json,inputFilePattern=gs://$BUCKET/EQY_US_ALL_ARCA_BOOK_20130403_smallfile.csv,outputTable=$PROJECT:nyse_arca_java.eqy_arca_book_20130403_all,bigQueryLoadingTemporaryDirectory=gs://$BUCKET/bq_load_temp/
```

## Source Files
* `big_query_schema.json` is the schema that the Big Query par do function uses to create the table and insert records into the table.
* `com.swilliams11.googlecloud.dataflow.nyse.arca.NyseArcaTransform` contains the source code. 


## Results
### DataFlow Job
This is the job graph.
![Job Details](/images/jobsummary.png)

It took about 15 minutes to process 213M records (14GB file).
![Job Metrics](/images/jobmetrics.png)
