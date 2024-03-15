# pysaprk_project
 implemented within Docker, utilizing Amazon S3 for storage, PostgreSQL for database management


# prerequisites
 before you begin, make sure you completed these steps.
  - Install docker 
  - build pyspark Image(Ater creating dockerfile run below command)
    
     command:- docker build -t image_name:latest .
    
  - pull PostgreSQL Image
    
    command:-. docker pull postgres
             . docker run --name postgres-env -p 5432:5432 -e POSTGRES_PASSWORD=root -d postgres

# Overview

 - Dumping CSV into Amazon S3
   
- Reading from S3 and Creating a Pyspark Table
  
- Storing Data in PostgreSQL
  
- Reading from postgeSQL and converting Data to Parquet Format
  
- Storing Parquet Files in S3

# Code snippets

 - Removing special characters

![remove_special_character_code](https://github.com/Arun3041/pysaprk_project/assets/59720713/dbfa7221-9fa7-47ea-98a7-876945f6411d)

![output](https://github.com/Arun3041/pysaprk_project/assets/59720713/3ec7d032-3d02-4476-bb17-f607e7e0c730)

-uploading csv to S3 Bucket


![upload_to_s3](https://github.com/Arun3041/pysaprk_project/assets/59720713/be3764ef-7b5e-429a-86f9-b3cf2cacecb4)

![s3_bucket_file](https://github.com/Arun3041/pysaprk_project/assets/59720713/83ab4ee5-05f3-497e-a531-f0ee666d8e41)


-reading from s3 and writing into postgres

![csv_to_postgres](https://github.com/Arun3041/pysaprk_project/assets/59720713/2b9fb469-be03-478d-b922-295c003da40e)

![data in postgres](https://github.com/Arun3041/pysaprk_project/assets/59720713/e1b5d8b4-3ab7-4abb-84bc-1a37ebb38438)


- reading from postgres and convert it into parquet format and again dumped into s3

   ![postgres_to_parquet_code](https://github.com/Arun3041/pysaprk_project/assets/59720713/f8009614-acc8-45dc-ab0e-2c45ccafd0dd)

  ![parquet_s3](https://github.com/Arun3041/pysaprk_project/assets/59720713/c5983c6b-ce63-438f-9d27-6484bf21cebb)




  


