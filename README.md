Data Engineer_Assessment_Incubyte
Below Is The Command To Run The ETL Job
java -jar target/incubyte-apache-beam-1.0-SNAPSHOT.jar \   
    --inputFile=/Users/ashutoshdash/Desktop/incubyte-java-repo/incubyte-apache-beam/src/main/resources/customer_data.txt \
    --stagingDbUrl=jdbc:mysql://localhost:3306/incubyte \
    --targetDbUrl=jdbc:mysql://localhost:3306/incubyte \
    --dbUsername=root \
    --dbPassword=root
