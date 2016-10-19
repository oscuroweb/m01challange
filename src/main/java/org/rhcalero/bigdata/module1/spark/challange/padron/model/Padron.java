package org.rhcalero.bigdata.module1.spark.challange.padron.model;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * Padron:
 * <p>
 * POJO class of padron info
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 19, 2016
 */
public class Padron implements Serializable {

    /** Generated serial version. */
    private static final long serialVersionUID = -5579737239623799865L;

    /** Log instance. */
    private static Logger log = Logger.getLogger(Padron.class.getName());

    /** Line separator. */
    private static final String SEPARATOR = ",";

    /** Minimum number of fields. */
    private static final int NUM_FIELDS = 7;

    private Integer nhop;

    /** Date of birth. */
    private Integer birthDate;

    /** Gender code. */
    private Integer gender;

    /** Code of province. */
    private Integer provinceCod;

    /** Code of town. */
    private Integer townCod;

    /** Code of district. */
    private Integer districtCod;

    /** Code of nationality. */
    private Integer nationalityCod;

    public Padron(String line) {
        String[] padronArray = line.split(SEPARATOR);

        if (padronArray.length >= NUM_FIELDS) {
            try {
                nhop = Integer.parseInt(padronArray[0]);
                birthDate = Integer.parseInt(padronArray[1]);
                gender = Integer.parseInt(padronArray[2]);
                provinceCod = Integer.parseInt(padronArray[3]);
                townCod = Integer.parseInt(padronArray[4]);
                nationalityCod = Integer.parseInt(padronArray[5]);
                districtCod = Integer.parseInt(padronArray[6]);
            } catch (NumberFormatException e) {
                log.warn("[WARNING] Invalid line format");
                nhop = null;
                birthDate = null;
                gender = null;
                provinceCod = null;
                townCod = null;
                nationalityCod = null;
                districtCod = null;
            }
        } else {
            String errorMessage = "[ERROR] Incorrect passwd line number of fields for line " + line;
            log.fatal(errorMessage);
            throw new RuntimeException(errorMessage);
        }

    }

    /**
     * Method getNhop.
     * <p>
     * Get method for attribute nhop
     * </p>
     * 
     * @return the nhop
     */
    public Integer getNhop() {
        return nhop;
    }

    /**
     * Method setNhop.
     * <p>
     * Set method for attribute nhop
     * </p>
     * 
     * @param nhop the nhop to set
     */
    public void setNhop(Integer nhop) {
        this.nhop = nhop;
    }

    /**
     * Method getBirthDate.
     * <p>
     * Get method for attribute birthDate
     * </p>
     * 
     * @return the birthDate
     */
    public Integer getBirthDate() {
        return birthDate;
    }

    /**
     * Method setBirthDate.
     * <p>
     * Set method for attribute birthDate
     * </p>
     * 
     * @param birthDate the birthDate to set
     */
    public void setBirthDate(Integer birthDate) {
        this.birthDate = birthDate;
    }

    /**
     * Method getGender.
     * <p>
     * Get method for attribute gender
     * </p>
     * 
     * @return the gender
     */
    public Integer getGender() {
        return gender;
    }

    /**
     * Method setGender.
     * <p>
     * Set method for attribute gender
     * </p>
     * 
     * @param gender the gender to set
     */
    public void setGender(Integer gender) {
        this.gender = gender;
    }

    /**
     * Method getProvinceCod.
     * <p>
     * Get method for attribute provinceCod
     * </p>
     * 
     * @return the provinceCod
     */
    public Integer getProvinceCod() {
        return provinceCod;
    }

    /**
     * Method setProvinceCod.
     * <p>
     * Set method for attribute provinceCod
     * </p>
     * 
     * @param provinceCod the provinceCod to set
     */
    public void setProvinceCod(Integer provinceCod) {
        this.provinceCod = provinceCod;
    }

    /**
     * Method getTownCod.
     * <p>
     * Get method for attribute townCod
     * </p>
     * 
     * @return the townCod
     */
    public Integer getTownCod() {
        return townCod;
    }

    /**
     * Method setTownCod.
     * <p>
     * Set method for attribute townCod
     * </p>
     * 
     * @param townCod the townCod to set
     */
    public void setTownCod(Integer townCod) {
        this.townCod = townCod;
    }

    /**
     * Method getDistrictCod.
     * <p>
     * Get method for attribute districtCod
     * </p>
     * 
     * @return the districtCod
     */
    public Integer getDistrictCod() {
        return districtCod;
    }

    /**
     * Method setDistrictCod.
     * <p>
     * Set method for attribute districtCod
     * </p>
     * 
     * @param districtCod the districtCod to set
     */
    public void setDistrictCod(Integer districtCod) {
        this.districtCod = districtCod;
    }

    /**
     * Method getNationalityCod.
     * <p>
     * Get method for attribute nationalityCod
     * </p>
     * 
     * @return the nationalityCod
     */
    public Integer getNationalityCod() {
        return nationalityCod;
    }

    /**
     * Method setNationalityCod.
     * <p>
     * Set method for attribute nationalityCod
     * </p>
     * 
     * @param nationalityCod the nationalityCod to set
     */
    public void setNationalityCod(Integer nationalityCod) {
        this.nationalityCod = nationalityCod;
    }

}
