# FINOS Legend Hackathon
## EPAM Systems

We are going to ingest data model from Legend into BigQuery and would use Bigquery as a vehicle to dynamically extend the model based on ML heuristics. That model will be finally fed back into Legend.  The integration between Legend and GCP and BigQuery can provide a powerful and flexible platform for data modeling and analysis. By leveraging the strengths of both platforms, companies can create accurate and comprehensive data models that can support a wide range of business needs!

Please, refer to the file LegendHackathon.pdf for complementary illustrations.

The following steps have been conducted in the course of the development activities for the FINOS Legend Hackathon:

1. Create a GCP Project secure-bonus-282818 
2. Create GCP Resorces:<br/>
  2.1 Compute engine for hosting FINOS Legend (exposed to the internet via external IP: 34.67.4.146 )<br/>
  2.2 Google Cloud Composer environment (Version 1.20.12) "legend" with Apache Airflow distribution (Version 2.4.3)<br/>
  2.3 GCS Bucket "legend_bucket"<br/>
  2.4 BigQuery data set "legend"<br/>
3. Conduct Docker driven installation of FINOS Legend on GCP compute engine instance created in step 2.1<br/>
4. Create simple data model in Legend which comprises of the following entities:<br/>
-----------------------------------------------------  
          Class entities::Employee
          {
            id: String[1];
            name: String[1];
            location: String[1];
            email: String[1];
            organisation: entities::Organisation[1];
            jobtitle: String[1];
            manager: entities::Employee[1];
          }

          Class entities::Organisation
          {
            id: String[1];
            name: String[1];
            location: String[1];
            employees: entities::Employee[1..*];
          }

-----------------------------------------------------  

5. Commit the model via FINOS Legend to the GitLab project : https://gitlab.com/igorkleiman/epam
6. Create Apache Airflow DAG "legend.py" which comprises of the following steps: 

    - extract Employee.pure and Organisation.pure files from GiLab repository https://gitlab.com/igorkleiman/epam. Use private_token "legend_new" for authentication. 
    - copy these files to GCP storage bucket gs://legend_bucket in GCP project secure-bonus-282818 
    - convert Employee.pure and Organisation.pure to Employee.json and Organisation.json accordingy. Employee.json and Organisation.json need to be compatible with BigQuery
    - Create Tables employee and organisaition in the data set secure-bonus-282818.legend. Use Employee.json and Organisation.json for that.
    - Alter table employee by adding a new column "newcolumn"
    - Extract tables employee and organisation to Employee.json and Organisation.json
    -  convert Employee.json to Employee.pure. Convert Organisation.json to Organisation.pure
    - Push Employee.pure and Organisation.pure to gitlab project epam. Use private token "legend_new"
7. Deploy legend.py to Apache Airflow DAG storage bucket: gs://europe-west6-legend-3b3a8318-bucket/dags 
8. Run legend DAG
9. Check FINOS Legend for changes: entity Employee should obtain new column "newcolumn"

The integration between Legend and Google Cloud Platform (GCP) and BigQuery can provide several benefits for data management and analysis, including:

### Seamless data integration<br/>
By ingesting data models from Legend into BigQuery, you can seamlessly integrate data modeling and data analysis workflows. This allows you to create and update data models in Legend, while also leveraging the powerful data analysis capabilities of BigQuery.

### Scalability and flexibility<br/>
BigQuery is a cloud-based data warehousing and analytics service that can store and process large amounts of data. This means that you can easily scale your data processing capabilities as your data needs grow. In addition, BigQuery supports a wide range of data formats, including JSON, which can be used to store and analyze data models.

### Machine learning integration<br/>
By using BigQuery as a vehicle to dynamically extend your data model based on ML heuristics, you can create a flexible and scalable data processing pipeline that can adapt to changing data needs. This allows you to take advantage of the latest machine learning techniques to improve the accuracy and relevance of your data models.

### Collaborative modeling<br/>
The integration between Legend and BigQuery allows you to collaborate on data modeling and analysis projects with other users within your organization. This can improve communication and coordination between different teams, leading to more accurate and effective data models.

