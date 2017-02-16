package com.sparkTutorial.sparkSql;

import java.io.Serializable;

public class Response implements Serializable {
    private String country;
    private float ageMidPoint;
    private String occupation;
    private float salaryMidPoint;

    public Response(String country, float ageMidPoint, String occupation, float salaryMidPoint) {
        this.country = country;
        this.ageMidPoint = ageMidPoint;
        this.occupation = occupation;
        this.salaryMidPoint = salaryMidPoint;
    }

    public Response() {
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public float getAgeMidPoint() {
        return ageMidPoint;
    }

    public void setAgeMidPoint(float ageMidPoint) {
        this.ageMidPoint = ageMidPoint;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public float getSalaryMidPoint() {
        return salaryMidPoint;
    }

    public void setSalaryMidPoint(float salaryMidPoint) {
        this.salaryMidPoint = salaryMidPoint;
    }
}
