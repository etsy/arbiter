package com.etsy.arbiter;

import java.util.Map;
/**
 * Represents Credential type in Oozie workflow 
 *
 * @author venky
 */
public class Credential {
	private String name;
    private String type;
    private Map<String, String> configurationProperties;
    
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getConfigurationProperties() {
		return configurationProperties;
	}

	public void setConfigurationProperties(Map<String, String> configurationProperties) {
		this.configurationProperties = configurationProperties;
	}

}
