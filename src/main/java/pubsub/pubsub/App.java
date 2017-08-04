package pubsub.pubsub;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.pubsub.v1.TopicName;
public class App 
{
	static final String PROJECT_ID = "pubsub-1";
	static TopicName topicName = TopicName.create(PROJECT_ID, "testing");
	
    public static void main( String[] args )
    {
    	processmessage();
	}
    public static void processmessage()
    {
    	DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    	options.setRunner(DataflowPipelineRunner.class);
    	options.setProject("pubsub-1");
	    options.setStreaming(true);
	    options.setStagingLocation("gs://testingpapajohns/output");
	    System.out.println("Before pipeline");
	    Pipeline pipeline = Pipeline.create(options);
	    PCollection<String> entities = pipeline.apply(PubsubIO.Read.topic("projects/pubsub-1/topics/testing"));
	    System.out.println("After PCollection entities");
	    entities.apply(ParDo.of(new StringToEntity("Default","Task")))
	    .apply(DatastoreIO.v1().write().withProjectId("pubsub-1"));
	    pipeline.run();
	    System.out.println("welcoem");
    }
    static class StringToEntity extends DoFn<String, Entity> {
    	private final String namespace;
        private final String kind;
        StringToEntity(String namespace, String kind) {
		       this.namespace = namespace;
		       this.kind = kind;
		       //ancestorKey = makeAncestorKey(namespace, kind);
		     }
    	public void processElement(ProcessContext c)  {
    		 c.output(makeEntity(c.element()));
    	}
    	public Entity makeEntity(String content) throws JSONException {
    		Entity.Builder entityBuilder = Entity.newBuilder();
    		JSONObject jObject = new JSONObject(content);
    		Key.Builder keyBuilder = DatastoreHelper.makeKey(kind,jObject.get("customerId").toString());
    		entityBuilder.getMutableProperties().put("orderId", makeValue(jObject.get("orderId").toString()).build());
    		entityBuilder.getMutableProperties().put("customerId", makeValue(jObject.get("customerId").toString()).build());
    		entityBuilder.getMutableProperties().put("total", makeValue(jObject.get("total").toString()).build());
    		entityBuilder.getMutableProperties().put("orderDate", makeValue(jObject.get("orderDate").toString()).build());
    	    entityBuilder.setKey(keyBuilder.build());
			return null;
    	}
    }
}
