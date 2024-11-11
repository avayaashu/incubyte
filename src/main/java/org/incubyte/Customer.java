package org.incubyte;

import java.time.LocalDate;

public class Customer {
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

    public Customer() {
    }

    public Customer(String customerName, String customerId, LocalDate openDate, LocalDate lastConsultedDate, String vaccinationId, String doctorName, String state, String country, LocalDate dateOfBirth, boolean isActive) {
        this.customerName = customerName;
        this.customerId = customerId;
        this.openDate = openDate;
        this.lastConsultedDate = lastConsultedDate;
        this.vaccinationId = vaccinationId;
        this.doctorName = doctorName;
        this.state = state;
        this.country = country;
        this.dateOfBirth = dateOfBirth;
        this.isActive = isActive;
    }

    public Customer(String customerName, String customerId, LocalDate openDate, LocalDate lastConsultedDate, String vaccinationId, String doctorName, String state, String country, LocalDate dateOfBirth, boolean isActive, int age, int daysSinceLastConsulted) {
        this.customerName = customerName;
        this.customerId = customerId;
        this.openDate = openDate;
        this.lastConsultedDate = lastConsultedDate;
        this.vaccinationId = vaccinationId;
        this.doctorName = doctorName;
        this.state = state;
        this.country = country;
        this.dateOfBirth = dateOfBirth;
        this.isActive = isActive;
        this.age = age;
        this.daysSinceLastConsulted = daysSinceLastConsulted;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public LocalDate getOpenDate() {
        return openDate;
    }

    public void setOpenDate(LocalDate openDate) {
        this.openDate = openDate;
    }

    public LocalDate getLastConsultedDate() {
        return lastConsultedDate;
    }

    public void setLastConsultedDate(LocalDate lastConsultedDate) {
        this.lastConsultedDate = lastConsultedDate;
    }

    public String getVaccinationId() {
        return vaccinationId;
    }

    public void setVaccinationId(String vaccinationId) {
        this.vaccinationId = vaccinationId;
    }

    public String getDoctorName() {
        return doctorName;
    }

    public void setDoctorName(String doctorName) {
        this.doctorName = doctorName;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(LocalDate dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public boolean getIsActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    // Getter and Setter for age
    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    // Getter and Setter for daysSinceLastConsulted
    public int getDaysSinceLastConsulted() {
        return daysSinceLastConsulted;
    }

    public void setDaysSinceLastConsulted(int daysSinceLastConsulted) {
        this.daysSinceLastConsulted = daysSinceLastConsulted;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "customerName='" + customerName + '\'' +
                ", customerId='" + customerId + '\'' +
                ", openDate=" + openDate +
                ", lastConsultedDate=" + lastConsultedDate +
                ", vaccinationId='" + vaccinationId + '\'' +
                ", doctorName='" + doctorName + '\'' +
                ", state='" + state + '\'' +
                ", country='" + country + '\'' +
                ", dateOfBirth=" + dateOfBirth +
                ", isActive=" + isActive +
                ", age=" + age +
                ", daysSinceLastConsulted=" + daysSinceLastConsulted +
                '}';
    }
}
