package org.incubyte;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.sun.org.apache.xalan.internal.lib.ExsltDatetime.formatDate;

public class CustomerCoder extends Coder<Customer> {
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");  // Adjust to your format

    String customerName;
    String customerId;
    LocalDate openDate;
    LocalDate lastConsultedDate;
    String vaccinationId;
    String doctorName;
    String state;
    String country;
    LocalDate dateOfBirth;
    boolean isActive;
    int age;
    int daysSinceLastConsulted;


    @Override
    public void encode(Customer customer, OutputStream outStream) throws  IOException {
        DataOutputStream dataOut = new DataOutputStream(outStream);
        STRING_CODER.encode(customer.getCustomerName(), outStream);
        STRING_CODER.encode(customer.getCustomerId(), outStream);
        STRING_CODER.encode(formatLocalDate(customer.getOpenDate()), outStream);
        STRING_CODER.encode(formatLocalDate(customer.getLastConsultedDate()), outStream);
        STRING_CODER.encode(customer.getVaccinationId(), outStream);
        STRING_CODER.encode(customer.getDoctorName(), outStream);
        STRING_CODER.encode(customer.getState(), outStream);
        STRING_CODER.encode(customer.getCountry(), outStream);
        STRING_CODER.encode(formatLocalDate(customer.getDateOfBirth()), outStream);
        STRING_CODER.encode(Boolean.toString(customer.getIsActive()), outStream);
        dataOut.writeInt(customer.getAge());
        dataOut.writeInt(customer.getDaysSinceLastConsulted());
    }

    @Override
    public Customer decode(InputStream inStream) throws  IOException {
        DataInputStream dataIn = new DataInputStream(inStream);
        String customerName = STRING_CODER.decode(inStream);
        String customerId = STRING_CODER.decode(inStream);
        LocalDate openDate = parseLocalDate(STRING_CODER.decode(inStream));  // Convert String to LocalDate
        LocalDate lastConsultedDate = parseLocalDate(STRING_CODER.decode(inStream));
        String vaccinationId = STRING_CODER.decode(inStream);
        String doctorName = STRING_CODER.decode(inStream);
        String state = STRING_CODER.decode(inStream);
        String country = STRING_CODER.decode(inStream);
        LocalDate dob = parseLocalDate(STRING_CODER.decode(inStream));
        boolean isActive = Boolean.parseBoolean(STRING_CODER.decode(inStream));  // Convert String to boolean
        int age = dataIn.readInt();
        int daysSinceLastConsulted = dataIn.readInt();

        Customer customer = new Customer(customerName, customerId, openDate, lastConsultedDate, vaccinationId,
                doctorName, state, country, dob, isActive);
        customer.setAge(age);
        customer.setDaysSinceLastConsulted(daysSinceLastConsulted);
        return customer;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
        return java.util.Collections.emptyList(); // No arguments needed
    }

    @Override
    public void verifyDeterministic() throws @UnknownKeyFor@NonNull@Initialized NonDeterministicException {

    }

    public static CustomerCoder of() {
        return new CustomerCoder();
    }


    // Helper method to format LocalDate to String
    private static String formatLocalDate(LocalDate localDate) {
        return localDate != null ? localDate.format(DATE_FORMATTER) : null;
    }

    // Helper method to parse String to LocalDate
    private static LocalDate parseLocalDate(String dateString) {
        return dateString != null ? LocalDate.parse(dateString, DATE_FORMATTER) : null;
    }
}
