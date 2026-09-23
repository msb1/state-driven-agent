package com.statedriven.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/** Reactive OpenAI-compatible chat-completions client. */
@Component
final class ModelClient {
 private final ObjectMapper json;
 private final Environment environment;
 private final WebClient http=WebClient.builder().build();

 ModelClient(ObjectMapper json,Environment environment){this.json=json;this.environment=environment;}

 Mono<ObjectNode> decide(ObjectNode config,ArrayNode messages){
  ObjectNode body=json.createObjectNode();
  body.put("model",config.path("model").asText());body.set("messages",messages);
  ArrayNode tools=body.putArray("tools");
  for(JsonNode tool:config.path("tools")){ObjectNode item=tools.addObject();item.put("type","function");item.set("function",tool);}
  body.put("tool_choice","auto");body.put("temperature",config.at("/llm/temperature").asDouble(.1));
  return post(config,body).map(reply->{
   JsonNode choice=reply.at("/choices/0/message");ObjectNode decision=json.createObjectNode();
   JsonNode call=choice.at("/tool_calls/0");
   if(!call.isMissingNode()){
    try{decision.put("type","tool");decision.put("id",call.path("id").asText("call_1"));decision.put("name",call.at("/function/name").asText());decision.set("arguments",json.readTree(call.at("/function/arguments").asText("{}")));}
    catch(Exception error){decision.put("type","final");decision.put("content","LLM emitted invalid tool arguments: "+error.getMessage());}
   }else{decision.put("type","final");decision.put("content",choice.path("content").asText("No final answer was returned."));}
   return decision;
  });
 }

 Mono<String> compact(ObjectNode config,String goal,ArrayNode historical,String existing,boolean hybrid){
  String prompt=hybrid?HYBRID_PROMPT:PLAIN_PROMPT;
  StringBuilder source=new StringBuilder();
  for(JsonNode message:historical){
   if(source.length()>0)source.append('\n');
   source.append(message.path("role").asText()).append(": ");
   JsonNode content=message.get("content");
   if(content!=null&&!content.isNull()&&!content.asText().isEmpty())source.append(content.asText());
   else {JsonNode calls=message.get("tool_calls");if(calls!=null&&!calls.isNull()&&!calls.isEmpty())source.append(pythonRepr(calls));}
  }
  ObjectNode body=json.createObjectNode();body.put("model",config.path("model").asText());body.put("temperature",0);
  ArrayNode messages=body.putArray("messages");
  messages.addObject().put("role","system").put("content",prompt);
  String ledger=existing==null||existing.isEmpty()?"none":existing;
  messages.addObject().put("role","user").put("content","Goal: "+goal+"\nPrevious ledger: "+ledger+"\nHistorical prefix:\n"+source);
  return post(config,body)
   .map(reply->reply.at("/choices/0/message/content").asText())
   .filter(result->!result.isEmpty())
   .onErrorResume(error->Mono.empty())
   .switchIfEmpty(Mono.fromSupplier(()->fallback(goal,historical,hybrid)));
 }

 private Mono<JsonNode> post(ObjectNode config,ObjectNode body){
  String base=config.at("/llm/base_url").asText().replaceAll("/+$","");
  String suffix="/chat/completions";
  String primary=base.endsWith(suffix)?base:base+suffix;
  String alternate=base.endsWith("/v1")?base.substring(0,base.length()-3)+suffix:null;
  String key=environment.getProperty(config.at("/llm/api_key_env").asText("OPENAI_API_KEY"),"local-not-required");
  Duration timeout=Duration.ofMillis(Math.round(config.at("/llm/timeout_seconds").asDouble(90)*1000));
  Mono<JsonNode> request=postUrl(primary,key,body,timeout);
  if(alternate!=null&&!alternate.equals(primary))request=request.onErrorResume(WebClientResponseException.NotFound.class,notFound->postUrl(alternate,key,body,timeout));
  return request;
 }

 private Mono<JsonNode> postUrl(String url,String key,ObjectNode body,Duration timeout){
  return http.post().uri(url).header("Authorization","Bearer "+key).bodyValue(body).retrieve().bodyToMono(JsonNode.class).timeout(timeout);
 }

 private static String fallback(String goal,ArrayNode historical,boolean hybrid){
  if(hybrid)return "<COMPACTED_STATE>\n<USER_GOAL>\n"+goal+"\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- No verified global lessons extracted.\n</GLOBAL_LESSON_LEDGER>\n<DEAD_ENDS>\n- No verified dead ends extracted; inspect the retained raw working buffer.\n</DEAD_ENDS>\n<CURRENT_LOCAL_PIVOT>\nReview the retained raw working buffer and continue from the latest verified state.\n</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>";
  StringBuilder text=new StringBuilder();
  for(JsonNode message:historical){if(text.length()>0)text.append(' ');JsonNode content=message.get("content");if(content!=null&&!content.isNull()&&!content.asText().isEmpty())text.append(content.asText());else{JsonNode calls=message.get("tool_calls");if(calls!=null&&!calls.isNull()&&!calls.isEmpty())text.append(pythonRepr(calls));}}
  int codePoints=text.codePointCount(0,text.length());
  String tail=codePoints>1800?text.substring(text.offsetByCodePoints(0,codePoints-1800)):text.toString();
  return "- Core Objective: "+goal+"\n- Universal Truths Discovered: none verified\n- Dead Ends: "+tail+"\n- Current Local Pivot: Review retained raw turns.";
 }

 /** Python's str(list[dict]) representation used by LlmClient.compact(). */
 private static String pythonRepr(JsonNode value){
  if(value.isTextual())return pythonString(value.asText());
  if(value.isNull())return "None";
  if(value.isBoolean())return value.asBoolean()?"True":"False";
  if(value.isNumber())return value.asText();
  if(value.isArray()){StringBuilder out=new StringBuilder("[");for(JsonNode child:value){if(out.length()>1)out.append(", ");out.append(pythonRepr(child));}return out.append(']').toString();}
  if(value.isObject()){StringBuilder out=new StringBuilder("{");Iterator<Map.Entry<String,JsonNode>> fields=value.fields();while(fields.hasNext()){Map.Entry<String,JsonNode> field=fields.next();if(out.length()>1)out.append(", ");out.append(pythonString(field.getKey())).append(": ").append(pythonRepr(field.getValue()));}return out.append('}').toString();}
  return "None";
 }

 private static String pythonString(String value){
  char quote=value.indexOf('\'')>=0&&value.indexOf('"')<0?'"':'\'';
  StringBuilder out=new StringBuilder().append(quote);
  for(int i=0;i<value.length();i++){char c=value.charAt(i);if(c=='\\')out.append("\\\\");else if(c==quote)out.append('\\').append(c);else if(c=='\n')out.append("\\n");else if(c=='\r')out.append("\\r");else if(c=='\t')out.append("\\t");else out.append(c);}
  return out.append(quote).toString();
 }

 private static final String HYBRID_PROMPT="""
You are the hybrid compaction and reflection engine for an autonomous agent.

Analyze the historical prefix only. The primary agent separately preserves its recent raw
working buffer, so do not reproduce, summarize, truncate, or invent raw messages here.
Extract durable environmental constraints and lessons as imperative operational rules.
Keep dead ends precise and short. Preserve only verified facts; mark uncertainty instead
of promoting guesses to rules. Return exactly this structure and no surrounding prose:

<COMPACTED_STATE>
<USER_GOAL>
Restate the original goal without changing its parameters or definitions.
</USER_GOAL>
<GLOBAL_LESSON_LEDGER>
- Imperative rules for permanent constraints or discoveries; none if no verified lessons.
</GLOBAL_LESSON_LEDGER>
<DEAD_ENDS>
- One-sentence failed approaches and why they failed; none if no verified dead ends.
</DEAD_ENDS>
<CURRENT_LOCAL_PIVOT>
State the most important active hypothesis or next operational focus in one sentence.
</CURRENT_LOCAL_PIVOT>
</COMPACTED_STATE>

The raw working buffer is retained by the primary agent outside this response.""".stripTrailing();

 private static final String PLAIN_PROMPT="""
Create a compacted context state for an agent. Preserve only verified facts.
Use exactly these headings:
- Core Objective
- Universal Truths Discovered
- Dead Ends
- Current Local Pivot
Do not invent facts. The original goal is protected separately. Do not include raw history.""".stripTrailing();
}
