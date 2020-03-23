package com.brd.sdc.api.beans;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-16 14:19
 */
public class Ipslog {
    private String time;
    private String report_equipment;
    private String attack_means;
    private String event_name;
    private String source_address;
    private String action;
    private String rule_number;
    private String events_number;
    private String agreement_summary;
    private String popularity;
    private String degree_of_danger;
    private String service_type;
    private String network_interface;
    private String vlan_id;
    private String destination_address;
    private String source_mac;
    private String destination_mac;
    private String source_port;
    private String destination_port;
    private String original_message;

    @Override
    public String toString() {
        return "Ipslog{" +
                "time='" + time + '\'' +
                ", report_equipment='" + report_equipment + '\'' +
                ", attack_means='" + attack_means + '\'' +
                ", event_name='" + event_name + '\'' +
                ", source_address='" + source_address + '\'' +
                ", action='" + action + '\'' +
                ", rule_number='" + rule_number + '\'' +
                ", events_number='" + events_number + '\'' +
                ", agreement_summary='" + agreement_summary + '\'' +
                ", popularity='" + popularity + '\'' +
                ", degree_of_danger='" + degree_of_danger + '\'' +
                ", service_type='" + service_type + '\'' +
                ", network_interface='" + network_interface + '\'' +
                ", vlan_id='" + vlan_id + '\'' +
                ", destination_address='" + destination_address + '\'' +
                ", source_mac='" + source_mac + '\'' +
                ", destination_mac='" + destination_mac + '\'' +
                ", source_port='" + source_port + '\'' +
                ", destination_port='" + destination_port + '\'' +
                ", original_message='" + original_message + '\'' +
                '}';
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getReport_equipment() {
        return report_equipment;
    }

    public void setReport_equipment(String report_equipment) {
        this.report_equipment = report_equipment;
    }

    public String getAttack_means() {
        return attack_means;
    }

    public void setAttack_means(String attack_means) {
        this.attack_means = attack_means;
    }

    public String getEvent_name() {
        return event_name;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public String getSource_address() {
        return source_address;
    }

    public void setSource_address(String source_address) {
        this.source_address = source_address;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getRule_number() {
        return rule_number;
    }

    public void setRule_number(String rule_number) {
        this.rule_number = rule_number;
    }

    public String getEvents_number() {
        return events_number;
    }

    public void setEvents_number(String events_number) {
        this.events_number = events_number;
    }

    public String getAgreement_summary() {
        return agreement_summary;
    }

    public void setAgreement_summary(String agreement_summary) {
        this.agreement_summary = agreement_summary;
    }

    public String getPopularity() {
        return popularity;
    }

    public void setPopularity(String popularity) {
        this.popularity = popularity;
    }

    public String getDegree_of_danger() {
        return degree_of_danger;
    }

    public void setDegree_of_danger(String degree_of_danger) {
        this.degree_of_danger = degree_of_danger;
    }

    public String getService_type() {
        return service_type;
    }

    public void setService_type(String service_type) {
        this.service_type = service_type;
    }

    public String getNetwork_interface() {
        return network_interface;
    }

    public void setNetwork_interface(String network_interface) {
        this.network_interface = network_interface;
    }

    public String getVlan_id() {
        return vlan_id;
    }

    public void setVlan_id(String vlan_id) {
        this.vlan_id = vlan_id;
    }

    public String getDestination_address() {
        return destination_address;
    }

    public void setDestination_address(String destination_address) {
        this.destination_address = destination_address;
    }

    public String getSource_mac() {
        return source_mac;
    }

    public void setSource_mac(String source_mac) {
        this.source_mac = source_mac;
    }

    public String getDestination_mac() {
        return destination_mac;
    }

    public void setDestination_mac(String destination_mac) {
        this.destination_mac = destination_mac;
    }

    public String getSource_port() {
        return source_port;
    }

    public void setSource_port(String source_port) {
        this.source_port = source_port;
    }

    public String getDestination_port() {
        return destination_port;
    }

    public void setDestination_port(String destination_port) {
        this.destination_port = destination_port;
    }

    public String getOriginal_message() {
        return original_message;
    }

    public void setOriginal_message(String original_message) {
        this.original_message = original_message;
    }
}
