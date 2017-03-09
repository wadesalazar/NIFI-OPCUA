package com.kentender.nifi.nifi_opcua_bundle;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.opcfoundation.ua.builtintypes.ExpandedNodeId;
import org.opcfoundation.ua.builtintypes.NodeId;
import org.opcfoundation.ua.core.Identifiers;
import com.kentender.nifi.nifi_opcua_services.OPCUAService;

@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Retrieves the namespace from an OPC UA server")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class GetNodeIds extends AbstractProcessor {
	
	private static String starting_node = null;
	private static String print_indentation = "No";
	private static Integer max_recursiveDepth;
	
	public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
			  .name("OPC UA Service")
			  .description("Specifies the OPC UA Service that can be used to access data")
			  .required(true)
			  .identifiesControllerService(OPCUAService.class)
			  .build();
    
    public static final PropertyDescriptor STARTING_NODE = new PropertyDescriptor
            .Builder().name("Starting Node")
            .description("From what node should Nifi begin browsing the node tree. Default is the root node.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor RECURSIVE_DEPTH = new PropertyDescriptor
            .Builder().name("Recursive Depth")
            .description("Maxium depth from the starting node to read, Default is 0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
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
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(RECURSIVE_DEPTH);
        descriptors.add(STARTING_NODE);
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
		
		print_indentation = context.getProperty(PRINT_INDENTATION).getValue();
		max_recursiveDepth = Integer.valueOf(context.getProperty(RECURSIVE_DEPTH).getValue());
		starting_node = context.getProperty(STARTING_NODE).getValue();
				
    }
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		final ComponentLog logger = getLogger();
		StringBuilder stringBuilder = new StringBuilder();
		
		 // Submit to getValue
        final OPCUAService opcUAService = context.getProperty(OPCUA_SERVICE)
        		.asControllerService(OPCUAService.class);
        
        if(opcUAService.updateSession()){
        	logger.debug("Session current");
        }else {
        	logger.debug("Session update failed");
        }
        
		// Set the starting node and parse the node tree
		if ( starting_node == null) {
			logger.debug("Parse the root node " + new ExpandedNodeId(Identifiers.RootFolder));
			stringBuilder.append(opcUAService.getNameSpace(print_indentation, max_recursiveDepth, new ExpandedNodeId(Identifiers.RootFolder)));
			
		} else {
			logger.debug("Parse the result list for node " + new ExpandedNodeId(NodeId.parseNodeId(starting_node)));
			stringBuilder.append(opcUAService.getNameSpace(print_indentation, max_recursiveDepth, new ExpandedNodeId(NodeId.parseNodeId(starting_node))));
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
        
	}
	

}