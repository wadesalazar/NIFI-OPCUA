/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kentender.nifi.nifi_opcua_services;

import static org.opcfoundation.ua.utils.EndpointUtil.selectByProtocol;
import static org.opcfoundation.ua.utils.EndpointUtil.selectBySecurityPolicy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.opcfoundation.ua.application.Client;
import org.opcfoundation.ua.application.SessionChannel;
import org.opcfoundation.ua.builtintypes.DataValue;
import org.opcfoundation.ua.builtintypes.ExpandedNodeId;
import org.opcfoundation.ua.builtintypes.LocalizedText;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.builtintypes.UnsignedInteger;
import org.opcfoundation.ua.common.ServiceFaultException;
import org.opcfoundation.ua.common.ServiceResultException;
import org.opcfoundation.ua.core.ActivateSessionResponse;
import org.opcfoundation.ua.core.Attributes;
import org.opcfoundation.ua.core.BrowseDescription;
import org.opcfoundation.ua.core.BrowseDirection;
import org.opcfoundation.ua.core.BrowseRequest;
import org.opcfoundation.ua.core.BrowseResponse;
import org.opcfoundation.ua.core.BrowseResult;
import org.opcfoundation.ua.core.EndpointDescription;
import org.opcfoundation.ua.core.IdType;
import org.opcfoundation.ua.core.Identifiers;
import org.opcfoundation.ua.core.ReadRequest;
import org.opcfoundation.ua.core.ReadResponse;
import org.opcfoundation.ua.core.ReadValueId;
import org.opcfoundation.ua.core.ReferenceDescription;
import org.opcfoundation.ua.core.TimestampsToReturn;
import org.opcfoundation.ua.transport.security.KeyPair;
import org.opcfoundation.ua.transport.security.SecurityPolicy;
import org.opcfoundation.ua.utils.EndpointUtil;


@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class StandardOPCUAService extends AbstractControllerService implements OPCUAService {
	
	// Redundant application varibles
	private static String applicationName = "Apache Nifi";
	private static String securityPolicy = "None";
	private static String url = "";
	
	// Create session variables
	private static SessionChannel mySession = null;
	private static ActivateSessionResponse activateSessionResponse = null;

	public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("Endpoint URL")
            .description("the opc.tcp address of the opc ua server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SECURITY_POLICY = new PropertyDescriptor
            .Builder().name("Security Policy")
            .description("How should Nifi authenticate with the UA server")
            .required(true)
            .allowableValues("None", "Basic128Rsa15", "Basic256", "Basic256Rsa256")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor
            .Builder().name("Application Name")
            .description("The application name is used to label certificates identifying this application")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(SECURITY_POLICY);
        props.add(APPLICATION_NAME);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
    	
    	final ComponentLog logger = getLogger();
    	
    	KeyPair myClientApplicationInstanceCertificate = null;
    	KeyPair myHttpsCertificate = null;
    	Client myClient = null;
    	EndpointDescription endpoint = null;
    	
    	// should be handled through getter & setter ???
    	applicationName = context.getProperty(APPLICATION_NAME).getValue();
    	securityPolicy = context.getProperty(SECURITY_POLICY).getValue();
    	url = context.getProperty(ENDPOINT).getValue();
    	
		// Initialize OPC UA Client
    	logger.debug("Creating Certificates");
    	myHttpsCertificate = Utils.getHttpsCert(applicationName);
    	myClientApplicationInstanceCertificate = getCertificates(securityPolicy, applicationName);
    	
    	logger.debug("Creating Client");
    	myClient = createClient(myClientApplicationInstanceCertificate, myHttpsCertificate, applicationName);
    	
    	logger.debug("Validating URL as endpoint");
    	endpoint = validateEndpoint(myClient, url, securityPolicy);
    	
    	logger.debug("Initialization Complete");
    	
    	// Create and activate session
  		try {
  			mySession = myClient.createSessionChannel(endpoint);
  			activateSessionResponse = mySession.activate();
  			
  		} catch (ServiceResultException e1) {
  			// TODO Auto-generated catch block THIS NEEDS TO FAIL IN A SPECIAL WAY TO BE RE TRIED 
  			e1.printStackTrace();
  		}
		
  		logger.debug("OPC UA client session ready");

    }

    @OnDisabled
    public void shutdown() {
    	// Close the session 
        
        /*
         * ( is this necessary or common practice.  
         * Timeouts clean up abandoned sessions ??? )*
         *  - yes, a client that is aware it will not 
         *  communicate again should close its connection
         * 
         */
        
        try {
			mySession.close();
		} catch (ServiceFaultException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceResultException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

	@Override
	public byte[] getValue(String reqTagname) throws ProcessException {

		
		// TODO presently this method accepts a tag name as input and fetches a value for that tag
		// A future version will need to be able to acquire a value from a specific time in the past 
		
		final ComponentLog logger = getLogger();
		
		String serverResponse = "";
		
		try {
			mySession.activate(activateSessionResponse.getServerNonce());
		} catch (ServiceResultException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
        ReadValueId[] NodesToRead = { 
			new ReadValueId(NodeId.parseNodeId(reqTagname), Attributes.Value, null, null )
		};
        
        // Form OPC request
  		ReadRequest req = new ReadRequest();		
  		req.setMaxAge(500.00);
  		req.setTimestampsToReturn(TimestampsToReturn.Both);
  		req.setRequestHeader(null);
  		req.setNodesToRead(NodesToRead);

  		// Submit OPC Read and handle response
  		try{
  			ReadResponse readResponse = mySession.Read(req);
            DataValue[] values = readResponse.getResults();
            // TODO need to check the result for errors and other quality issues
            serverResponse = reqTagname + "," + values[0].getValue().toString()  + ","+ values[0].getServerTimestamp().toString();
              
          }catch (Exception e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  			
  		}
  		
        return serverResponse.getBytes(); 
	}

	@Override
	public String getNameSpace(String print_indentation, int max_recursiveDepth, ExpandedNodeId expandedNodeId) throws ProcessException {
		
		return parseNodeTree(print_indentation, 0, max_recursiveDepth, expandedNodeId);
	
	}
	
	private EndpointDescription validateEndpoint(Client client, String security_policy, String url){
	
		// TODO This method should provide feedback
		
		final ComponentLog logger = getLogger();
    	
		// Retrieve and filter end point list
		// TODO need to move this to service or on schedule method
				
		EndpointDescription[] endpoints = null;
		
		try {
			endpoints = client.discoverEndpoints(url);
		} catch (ServiceResultException e1) {
			// TODO Auto-generated catch block
			
			logger.error(e1.getMessage());
		}
		
		switch (security_policy) {
			
			case "Basic128Rsa15":{
				endpoints = selectBySecurityPolicy(endpoints,SecurityPolicy.BASIC128RSA15);
				break;
			}
			case "Basic256": {
				endpoints = selectBySecurityPolicy(endpoints,SecurityPolicy.BASIC256);
				break;
			}	
			case "Basic256Rsa256": {
				endpoints = selectBySecurityPolicy(endpoints,SecurityPolicy.BASIC256SHA256);
				break;
			}
			default :{
				endpoints = selectBySecurityPolicy(endpoints,SecurityPolicy.NONE);
				logger.error("No security mode specified");
				break;
			}
		}
		
		// For now only opc.tcp has been implemented
		endpoints = selectByProtocol(endpoints, "opc.tcp");
		
		// Finally confirm the provided end point is in the list
		endpoints = EndpointUtil.selectByUrl(endpoints, url);
		
		// There should only be one item left in the list
		return endpoints[0];
		
	}
	
	private KeyPair getCertificates(String security_policy, String application_name ){
		
		final ComponentLog logger = getLogger();
    			
    	// Load Client's certificates from file or create new certs
		switch (security_policy) {
		
			case "None":{
				
			}case "Basic128Rsa15":{
			
				return Utils.getCert(application_name, SecurityPolicy.BASIC128RSA15);
				
			}case "Basic256": {
				
				return Utils.getCert(application_name, SecurityPolicy.BASIC256);
				
			}case "Basic256Rsa256": {
				
				return Utils.getCert(application_name, SecurityPolicy.BASIC256SHA256);
				
			}
		}
		return null;
	}
	
	private Client createClient(
			KeyPair myClientApplicationInstanceCertificate, 
			KeyPair myHttpsCertificate, 
			String applicationName){
		
		Locale ENGLISH = Locale.ENGLISH;
		
		// Build client application
		Client client = Client.createClientApplication( myClientApplicationInstanceCertificate ); 
		client.getApplication().getHttpsSettings().setKeyPair(myHttpsCertificate);
		client.getApplication().addLocale( ENGLISH );
		client.getApplication().setApplicationName( new LocalizedText(applicationName, Locale.ENGLISH) );
		client.getApplication().setProductUri( "urn:" + applicationName );
		
		return client;
		
	}
	
	private static String parseNodeTree(
			String print_indentation, 
			int recursiveDepth, 
			int max_recursiveDepth, 
			ExpandedNodeId expandedNodeId){
		
		
		StringBuilder stringBuilder = new StringBuilder();
		
		// Conditions for exiting this function
		// If provided node is null ( should not happen )
		if(expandedNodeId == null){	return null; }
		
		// Have we already reached the max depth? Exit if so
		if (recursiveDepth > max_recursiveDepth){ return null; }
		
		// Describe the request for given node
		BrowseDescription[] NodesToBrowse = new BrowseDescription[1];
		NodesToBrowse[0] = new BrowseDescription();
		NodesToBrowse[0].setBrowseDirection(BrowseDirection.Forward);
		
		// Set node to browse to given Node
		if(expandedNodeId.getIdType() == IdType.String){

			NodesToBrowse[0].setNodeId( new NodeId(expandedNodeId.getNamespaceIndex(), (String) expandedNodeId.getValue()) );
		}else if(expandedNodeId.getIdType() == IdType.Numeric){

			NodesToBrowse[0].setNodeId( new NodeId(expandedNodeId.getNamespaceIndex(), (UnsignedInteger) expandedNodeId.getValue()) );
		}else if(expandedNodeId.getIdType() == IdType.Guid){

			NodesToBrowse[0].setNodeId( new NodeId(expandedNodeId.getNamespaceIndex(), (UUID) expandedNodeId.getValue()) );
		}else if(expandedNodeId.getIdType() == IdType.Opaque){

			NodesToBrowse[0].setNodeId( new NodeId(expandedNodeId.getNamespaceIndex(), (byte[]) expandedNodeId.getValue()) );
		} else {
			// Return if no matches. Is this not a valid node?
		}
		
		// Form request
		BrowseRequest browseRequest = new BrowseRequest();
		browseRequest.setNodesToBrowse(NodesToBrowse);
		
		// Form response, make request 
		BrowseResponse browseResponse = new BrowseResponse();
		try {
			browseResponse = mySession.Browse(browseRequest);
		} catch (ServiceFaultException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServiceResultException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Get results
		BrowseResult[] browseResults = browseResponse.getResults();
		
		// Retrieve reference descriptions for the result set 
		// 0 index is assumed !!!
		ReferenceDescription[] referenceDesc = browseResults[0].getReferences();
		
		// Situation 1: There are no result descriptions because we have hit a leaf
		if(referenceDesc == null){
			return null;
		}
		
		// Situation 2: There are results descriptions and each node must be parsed
		for(int k = 0; k < referenceDesc.length; k++){
				
			// Print indentation	
			switch (print_indentation) {
			
				case "Yes":{
					for(int j = 0; j < recursiveDepth; j++){
						stringBuilder.append("- ");
					}
				}
			}
			
			// Print the current node
			stringBuilder.append(referenceDesc[k].getNodeId() + System.lineSeparator());
			
			// Print the child node(s)
			stringBuilder.append(parseNodeTree(print_indentation, recursiveDepth + 1, max_recursiveDepth, referenceDesc[k].getNodeId()));
			
		}
		
		return stringBuilder.toString();
		
		// we have exhausted the child nodes of the given node
		
	}


}