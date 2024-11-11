package org.incubyte;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.sql.*;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class HospitalETLPipeline {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(HospitalETLConfig.class);
        HospitalETLConfig options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HospitalETLConfig.class);
        // Load data into staging table
        loadToStagingTable(options);
        // Load data from staging table to country tables
        loadToCountryTables(options);

    }

    public static void loadToStagingTable(HospitalETLConfig options) {
        Pipeline pipeline = Pipeline.create(options);
        // Register the CustomerCoder explicitly
        pipeline.getCoderRegistry().registerCoderForClass(Customer.class, CustomerCoder.of());
        // Step 1: Read and parse data from .txt file and write to staging table
        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ParseCustomerData", ParDo.of(new ParseCustomerDataFn()))
                // Step 2: Write to staging table
                .apply("WriteToStagingTable", JdbcIO.<Customer>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", options.getStagingDbUrl())
                                .withUsername(options.getDbUsername())
                                .withPassword(options.getDbPassword()))
                        .withStatement("INSERT INTO customer_staging_ab (customer_name, customer_id, open_date, last_consulted_date, vaccination_id, doctor_name, state, country, dob, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        .withPreparedStatementSetter((element, statement) -> {
                            statement.setString(1, element.customerName);
                            statement.setString(2, element.customerId);
                            statement.setDate(3, Date.valueOf(element.openDate.toString()));
                            statement.setDate(4, element.lastConsultedDate != null ? Date.valueOf(element.lastConsultedDate.toString()) : null);
                            statement.setString(5, element.vaccinationId);
                            statement.setString(6, element.doctorName);
                            statement.setString(7, element.state);
                            statement.setString(8, element.country);
                            statement.setDate(9, Date.valueOf(element.dateOfBirth.toString()));
                            statement.setBoolean(10, element.isActive);
                        }));
        pipeline.run().waitUntilFinish();
    }

    public static void loadToCountryTables(HospitalETLConfig options) {
        Pipeline pipeline = Pipeline.create(options);
        // Register the CustomerCoder explicitly
        pipeline.getCoderRegistry().registerCoderForClass(Customer.class, CustomerCoder.of());
        pipeline.apply("ReadFromStagingTable", JdbcIO.<Customer>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", options.getStagingDbUrl())
                                .withUsername(options.getDbUsername())
                                .withPassword(options.getDbPassword()))
                        .withQuery("SELECT customer_name, customer_id, open_date, last_consulted_date, vaccination_id, doctor_name, state, country, dob, is_active FROM customer_staging_ab")
                        .withRowMapper((JdbcIO.RowMapper<Customer>) resultSet -> {
                            Customer customer = new Customer();
                            customer.customerName = resultSet.getString("customer_name");
                            customer.customerId = resultSet.getString("customer_id");
                            customer.openDate = resultSet.getDate("open_date").toLocalDate();
                            customer.lastConsultedDate = resultSet.getDate("last_consulted_date") != null ? resultSet.getDate("last_consulted_date").toLocalDate() : null;
                            customer.vaccinationId = resultSet.getString("vaccination_id");
                            customer.doctorName = resultSet.getString("doctor_name");
                            customer.state = resultSet.getString("state");
                            customer.country = resultSet.getString("country");
                            customer.dateOfBirth = resultSet.getDate("dob").toLocalDate();
                            customer.isActive = resultSet.getBoolean("is_active");
                            //System.out.println(customer);
                            return customer;
                        })
                        // Register the CustomerCoder for this read transform
                        .withCoder(CustomerCoder.of()))
                .apply("CalculateDerivedFields", ParDo.of(new CalculateDerivedFieldsFn()))
//                .apply("PrintDataAfterCalculateDerivedFields", ParDo.of(new DoFn<Customer, Customer>() {
//                    @ProcessElement
//                    public void processElement(@Element Customer customer, OutputReceiver<Customer> receiver) {
//                        // Print the Customer data with updated fields
//                        System.out.println("Customer Data After CalculateDerivedFields : " + customer.toString());
//                        receiver.output(customer);
//                    }
//                }));
                .apply("WriteToCountryTables", ParDo.of(new WriteToCountryTableFn(options.getTargetDbUrl(), options.getDbUsername(), options.getDbPassword())));
        pipeline.run().waitUntilFinish();
    }

    // Function to parse customer data from text file
    public static class ParseCustomerDataFn extends DoFn<String, Customer> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Customer> receiver) {
            //System.out.println(line);
            String newData = line.replaceAll("\\s+", "");
            String[] fields = newData.split("\\|");
            if (fields[1].equals("D")) {
                Customer customer = new Customer();
                customer.customerName = fields[2];
                customer.customerId = fields[3];
                customer.openDate = LocalDate.parse(fields[4], DateTimeFormatter.ofPattern("yyyyMMdd"));
                customer.lastConsultedDate = fields[5].isEmpty() ? null : LocalDate.parse(fields[4], DateTimeFormatter.ofPattern("yyyyMMdd"));
                customer.vaccinationId = fields[6];
                customer.doctorName = fields[7];
                customer.state = fields[8];
                customer.country = fields[9];
                customer.dateOfBirth = LocalDate.parse(fields[10], DateTimeFormatter.ofPattern("yyyyMMdd"));
                customer.isActive = fields[11].equals("A");
                //System.out.println("customer"+customer.toString());
                receiver.output(customer);
            }
        }
    }

    // Function to calculate derived fields
    public static class CalculateDerivedFieldsFn extends DoFn<Customer, Customer> {
        @ProcessElement
        public void processElement(@Element Customer customer, OutputReceiver<Customer> receiver) {
            // Create a new Customer object, copying existing data and calculating derived fields
            Customer updatedCustomer = new Customer(
                    customer.getCustomerName(),
                    customer.getCustomerId(),
                    customer.getOpenDate(),
                    customer.getLastConsultedDate(),
                    customer.getVaccinationId(),
                    customer.getDoctorName(),
                    customer.getState(),
                    customer.getCountry(),
                    customer.getDateOfBirth(),
                    customer.getIsActive()
            );

            // Calculate age and daysSinceLastConsulted
            int age = Period.between(customer.getDateOfBirth(), LocalDate.now()).getYears();
            int daysSinceLastConsulted = customer.getLastConsultedDate() != null
                    ? Period.between(customer.getLastConsultedDate(), LocalDate.now()).getDays()
                    : 0;

            updatedCustomer.setAge(age);
            updatedCustomer.setDaysSinceLastConsulted(daysSinceLastConsulted);

            // Output the new updated Customer
            System.out.println("Updated Customer : "+updatedCustomer);
            receiver.output(updatedCustomer);
        }
    }


    // Function to write to individual country tables
    public static class WriteToCountryTableFn extends DoFn<Customer, Void> {
        private final String dbUrl;
        private final String username;
        private final String password;

        public WriteToCountryTableFn(String dbUrl, String username, String password) {
            this.dbUrl = dbUrl;
            this.username = username;
            this.password = password;
        }

        @ProcessElement
        public void processElement(@Element Customer customer) {
            //System.out.println("Customer Details Inside Write Into Country Table : "+customer);
            String tableName;

            // Check country and set the target table accordingly
            switch (customer.country.toUpperCase()) {
                case "IND":
                    tableName = "table_india_ab";
                    break;
                case "AUS":
                    tableName = "table_australia_ab";
                    break;
                case "USA":
                    tableName = "table_usa_ab";
                    break;
                default:
                    // Skip processing if country is not IND or AUS
                    //System.out.println("Skipping record for unsupported country: " + customer.country);
                    return;
            }

            String insertSQL = "INSERT INTO " + tableName + " (customer_name, customer_id, open_date, last_consulted_date, vaccination_id, doctor_name, state, country, dob, is_active, age, days_since_last_consulted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (Connection connection = DriverManager.getConnection(dbUrl, username, password);
                 PreparedStatement statement = connection.prepareStatement(insertSQL)) {

                statement.setString(1, customer.customerName);
                statement.setString(2, customer.customerId);
                statement.setDate(3, Date.valueOf(customer.openDate.toString()));
                statement.setDate(4, customer.lastConsultedDate != null ? Date.valueOf(customer.lastConsultedDate.toString()) : null);
                statement.setString(5, customer.vaccinationId);
                statement.setString(6, customer.doctorName);
                statement.setString(7, customer.state);
                statement.setString(8, customer.country);
                statement.setDate(9, Date.valueOf(customer.dateOfBirth.toString()));
                statement.setBoolean(10, customer.isActive);
                statement.setInt(11, customer.age);
                statement.setInt(12, customer.daysSinceLastConsulted);

                statement.executeUpdate();
                //System.out.println("Inserted record for country: " + customer.country);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}


