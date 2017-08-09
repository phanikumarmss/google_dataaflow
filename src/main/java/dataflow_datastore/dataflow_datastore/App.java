package dataflow_datastore.dataflow_datastore;

import org.apache.beam.sdk.transforms.DoFn;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import javax.annotation.Nullable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONException;
import org.json.JSONObject;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
/**
 * Hello world!
 *
 */
public class App 
{
	static class CreateEntityFn extends DoFn<String, Entity> {
	     private final String namespace;
	     private final String kind;
	     //private final Key ancestorKey;

	     CreateEntityFn(String namespace, String kind) {
	       this.namespace = namespace;
	       this.kind = kind;

	       //ancestorKey = makeAncestorKey(namespace, kind);
	     }

	     public Entity makeEntity(String content) throws JSONException {
	       Entity.Builder entityBuilder = Entity.newBuilder();
	  
	      // JSONObject jObject = new JSONObject(content);
	       JSONObject jObject=new JSONObject(content);
	       
	      
	       Key.Builder keyBuilder = DatastoreHelper.makeKey(kind,jObject.get("customerId").toString());
	       entityBuilder.setKey(keyBuilder.build());
	       
	       try{
	    	         entityBuilder.getMutableProperties().put("orderId", makeValue(jObject.get("orderId").toString()).build());
	       }catch(NullPointerException npe)
	       {
	    	   npe.printStackTrace();
	       }
	       try{
	    	   entityBuilder.getMutableProperties().put("customerId", makeValue(jObject.get("customerId").toString()).build());
	       }catch(NullPointerException npe)
	       {
	    	   npe.printStackTrace();
	       }
	      
	       try{
	       entityBuilder.getMutableProperties().put("total", makeValue(jObject.get("total").toString()).build());
	       }catch(NullPointerException npe)
	       {
	    	   npe.printStackTrace();
	       }
	       
	       try{
	       entityBuilder.getMutableProperties().put("orderDate", makeValue(jObject.get("orderDate").toString()).build());
	       }catch(NullPointerException npe)
	       {
	    	   npe.printStackTrace();
	       }
	       return entityBuilder.build();
	     }

	     @ProcessElement
	     public void processElement(ProcessContext c) throws JSONException {
	       c.output(makeEntity(c.element()));
	     }
	   }
	
	
	
	
	 public interface Options extends PipelineOptions 
	 {
		 @Description("Topic")
	     //@Default.String("projects/superb-watch-172816/subscriptions/subbq")
	     @Default.String("projects/pubsub-1/topics/testing")
	     String gettopic();
	     void settopic(String value);
	     @Description("Cloud Datastore Entity Kind")
	     @Default.String("Task")
	     String getKind();
	     void setKind(String value);
	     @Description("Dataset namespace")
	     String getNamespace();
	     void setNamespace(@Nullable String value);
	     @Description("Dataset ID to read from Cloud Datastore")
	     @Validation.Required
	     @Default.String("pubsub-1")
	     String getDataset();
	     void setDataset(String value);
	 }
	 public static void main(String... args)
	 {
		 String arg[]={"--runner=DataflowRunner","--dataset=pubsub-1","--project=pubsub-1"};
		  Options options = PipelineOptionsFactory.fromArgs(arg).withValidation().as(Options.class);
		  Pipeline p = Pipeline.create(options);
		  options.setTempLocation("gs://testingpapajohns/output");
		  PCollection<String> pcoll=  p.apply(PubsubIO.readStrings().fromTopic(options.gettopic()));
		  pcoll.apply(ParDo.of(new CreateEntityFn("Default","Task")))
		  .apply(DatastoreIO.v1().write().withProjectId("pubsub-1"));  
		  p.run();
	}
}
