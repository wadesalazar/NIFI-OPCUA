package com.kentender.nifi.nifi_opcua_bundle;

import static org.opcfoundation.ua.utils.EndpointUtil.selectByProtocol;
import static org.opcfoundation.ua.utils.EndpointUtil.selectBySecurityPolicy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.opcfoundation.ua.application.Client;
import org.opcfoundation.ua.application.SessionChannel;
import org.opcfoundation.ua.builtintypes.ExpandedNodeId;
import org.opcfoundation.ua.builtintypes.LocalizedText;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.builtintypes.UnsignedInteger;
import org.opcfoundation.ua.common.ServiceFaultException;
import org.opcfoundation.ua.common.ServiceResultException;
import org.opcfoundation.ua.core.BrowseDescription;
import org.opcfoundation.ua.core.BrowseDirection;
import org.opcfoundation.ua.core.BrowseRequest;
import org.opcfoundation.ua.core.BrowseResponse;
import org.opcfoundation.ua.core.BrowseResult;
import org.opcfoundation.ua.core.EndpointDescription;
import org.opcfoundation.ua.core.IdType;
import org.opcfoundation.ua.core.Identifiers;
import org.opcfoundation.ua.core.ReferenceDescription;
import org.opcfoundation.ua.transport.security.Cert;
import org.opcfoundation.ua.transport.security.KeyPair;
import org.opcfoundation.ua.transport.security.PrivKey;
import org.opcfoundation.ua.transport.security.SecurityPolicy;
import org.opcfoundation.ua.utils.CertificateUtils;

@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Retrieves the namespace from an OPC UA server")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class GetExpandedNodeIds extends AbstractProcessor {
	
	
	// TODO clean up static vars and implement private where needed
	final Locale ENGLISH = Locale.ENGLISH;
	static int max_recursiveDepth = 0;
	static int recursiveDepth = 0;
	static StringBuilder stringBuilder = new StringBuilder();
	static String url = "";
	static String applicationName = "";
	static KeyPair myClientApplicationInstanceCertificate = null;
	static KeyPair myHttpsCertificate = null;
	static String outputFilename = null;
	static String print_indentation = null;
	static String starting_node = null;
	static EndpointDescription[] endpoints = null;
	static Client myClient = null;
	
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
    
    public static final PropertyDescriptor STARTING_NODE = new PropertyDescriptor
            .Builder().name("Starting Node")
            .description("From what node should Nifi begin browsing the node tree. Default is the root node.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RECURSIVE_DEPTH = new PropertyDescriptor
            .Builder().name("Recursive Depth")
            .description("Maxium depth from the starting node to read, Default is 1")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor
            .Builder().name("Application Name")
            .description("The application name is used to label certificates identifying this application")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor OUTFILE_NAME = new PropertyDescriptor
            .Builder().name("Output filename")
            .description("File path and name used for output")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
    public static final PropertyDescriptor PRINT_INDENTATION = new PropertyDescriptor
            .Builder().name("Print Indentation")
            .description("Should Nifi add indentation to the output text")
            .required(true)
            .allowableValues("No", "Yes")
            .defaultValue("No")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ENDPOINT);
        descriptors.add(RECURSIVE_DEPTH);
        descriptors.add(SECURITY_POLICY);
        descriptors.add(STARTING_NODE);
        descriptors.add(APPLICATION_NAME);
        descriptors.add(OUTFILE_NAME);
        descriptors.add(PRINT_INDENTATION);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	
    	final ComponentLog logger = getLogger();
		
    	// Set variables
    	applicationName = context.getProperty(APPLICATION_NAME).getValue();
    	outputFilename = context.getProperty(OUTFILE_NAME).getValue();
		print_indentation = context.getProperty(PRINT_INDENTATION).getValue();
		max_recursiveDepth = Integer.valueOf(context.getProperty(RECURSIVE_DEPTH).getValue());
		url = context.getProperty(ENDPOINT).getValue();
		starting_node = context.getProperty(STARTING_NODE).getValue();
		
		// Load Client's certificates from file or create new certs
		if (context.getProperty(SECURITY_POLICY).getValue() == "None"){
			// Build OPC Client
			myClientApplicationInstanceCertificate = null;
						
		} else {

			myHttpsCertificate = Utils.getHttpsCert(applicationName);
			
			// Load or create HTTP and Client's Application Instance Certificate and key
			switch (context.getProperty(SECURITY_POLICY).getValue()) {
				
				case "Basic128Rsa15":{
					myClientApplicationInstanceCertificate = Utils.getCert(applicationName, SecurityPolicy.BASIC128RSA15);
					break;
					
				}case "Basic256": {
					myClientApplicationInstanceCertificate = Utils.getCert(applicationName, SecurityPolicy.BASIC256);
					break;
					
				}case "Basic256Rsa256": {
					myClientApplicationInstanceCertificate = Utils.getCert(applicationName, SecurityPolicy.BASIC256SHA256);
					break;
				}
			}
		}
		
		// Create Client
		// TODO need to move this to service or on schedule method
		myClient = Client.createClientApplication( myClientApplicationInstanceCertificate ); 
		myClient.getApplication().getHttpsSettings().setKeyPair(myHttpsCertificate);
		myClient.getApplication().addLocale( ENGLISH );
		myClient.getApplication().setApplicationName( new LocalizedText(applicationName, Locale.ENGLISH) );
		myClient.getApplication().setProductUri( "urn:" + applicationName );
		
		// Retrieve and filter end point list
		// TODO need to move this to service or on schedule method
		
		try {
			endpoints = myClient.discoverEndpoints(url);
		} catch (ServiceResultException e1) {
			// TODO Auto-generated catch block
			
			logger.error(e1.getMessage());
		}
		
		switch (context.getProperty(SECURITY_POLICY).getValue()) {
			
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
				
    }
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		final ComponentLog logger = getLogger();
		recursiveDepth = 0;
		
		// Create a session using end point description
		SessionChannel mySession = null;
		
		try {
			mySession = myClient.createSessionChannel(endpoints[0]);
			mySession.activate();	
		} catch (ServiceResultException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		}
		
		// Set the starting node and parse the node tree
		if ( starting_node == null) {
			logger.debug("Parse the root node " + new ExpandedNodeId(Identifiers.RootFolder));
			parseNodeTree(mySession, new ExpandedNodeId(Identifiers.RootFolder));
			
		} else {
			logger.debug("Parse the result list for node " + new ExpandedNodeId(NodeId.parseNodeId(starting_node)));
			parseNodeTree(mySession, new ExpandedNodeId(NodeId.parseNodeId(starting_node)));
		}
		
		// Write the results back out to a flow file
		FlowFile flowFile = session.create();
        if ( flowFile == null ) {
        	logger.error("Flowfile is null");
        }
		
		flowFile = session.write(flowFile, new OutputStreamCallback() {
            public void process(OutputStream out) throws IOException {
            	out.write(stringBuilder.toString().getBytes());
            	
            }
		});
        
		// Transfer data to flow file
        session.transfer(flowFile, SUCCESS);
        
        // Reset our stringBuilder
        stringBuilder.setLength(0);
        
        // Close the session 
        try {
			mySession.close();
		} catch (ServiceFaultException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		} catch (ServiceResultException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
        /*
         * ( is this necessary or common practice.  
         * Timeouts clean up abandoned sessions ??? )*
         */
	}
	
	private static void parseNodeTree(SessionChannel sessionChannel, ExpandedNodeId expandedNodeId){
		
		// Conditions for exiting this function
		// If provided node is null ( should not happen )
		if(expandedNodeId == null){	return; }
		
		// If we have already reached the max depth
		if (recursiveDepth > max_recursiveDepth){ return; } else { recursiveDepth++;}
		
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
			// return if no matches, not a valid node?
		}
		
		// Form request
		BrowseRequest browseRequest = new BrowseRequest();
		browseRequest.setNodesToBrowse(NodesToBrowse);
		
		// Form response, make request 
		BrowseResponse browseResponse = new BrowseResponse();
		try {
			browseResponse = sessionChannel.Browse(browseRequest);
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
		// 0 index is assumed 
		ReferenceDescription[] referenceDesc = browseResults[0].getReferences();
		
		// Situation 1: There are no result descriptions because we have hit a leaf
		if(referenceDesc == null){
			recursiveDepth--;
			return;
		}
		
		// Situation 2: There are results descriptions and each node must be parsed
		for(int k = 0; k < referenceDesc.length; k++){
				
			//Print indentation	
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
			parseNodeTree(sessionChannel, referenceDesc[k].getNodeId());
		
		}
		
		// we have exhausted the child nodes of the given node
		recursiveDepth--;
		return;
		
	}

}