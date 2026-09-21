package com.statedriven.agent;

import com.fasterxml.jackson.databind.*; import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.charset.StandardCharsets; import java.nio.file.*; import java.security.MessageDigest; import java.util.*; import java.util.regex.*;
import org.springframework.stereotype.Component; import org.springframework.core.env.Environment;

@Component final class ConfigRepository {
  record Snapshot(String ref,String sha256,ObjectNode config) {}
  private static final Pattern ENV=Pattern.compile("\\$\\{([A-Z0-9_]+)(?::-([^}]*))?}");
  private final ObjectMapper yaml=new ObjectMapper(new YAMLFactory()); private final Path root; private final Environment environment;
  ConfigRepository(ObjectMapper ignored,Environment environment){this.environment=environment;root=Path.of(environment.getProperty("AGENT_CONFIG_ROOT","../config")).toAbsolutePath().normalize();}
  Path root(){return root;}
  Snapshot resolve(String ref){try {if(ref==null||ref.isBlank()||Path.of(ref).isAbsolute())throw new IllegalArgumentException("config_ref must be a non-empty relative YAML path"); Path p=root.resolve(ref).normalize(); if(!p.startsWith(root)||!Files.isRegularFile(p)||!(ref.endsWith(".yaml")||ref.endsWith(".yml")))throw new IllegalArgumentException("config_ref must identify a YAML file within AGENT_CONFIG_ROOT"); byte[] b=Files.readAllBytes(p); JsonNode raw=yaml.readTree(expand(new String(b,StandardCharsets.UTF_8))); if(raw==null||!raw.path("agent").isObject())throw new IllegalArgumentException("config must contain an agent mapping"); ObjectNode a=(ObjectNode)raw.path("agent"); validate(a); return new Snapshot(root.relativize(p).toString().replace('\\','/'),hex(MessageDigest.getInstance("SHA-256").digest(b)),a);}catch(Exception e){throw new IllegalArgumentException(e.getMessage(),e);}}
  List<Snapshot> list(){try(var paths=Files.walk(root)){return paths.filter(p->Files.isRegularFile(p)&&(p.toString().endsWith(".yaml")||p.toString().endsWith(".yml"))).sorted().map(p->{try{return resolve(root.relativize(p).toString());}catch(Exception e){return null;}}).filter(Objects::nonNull).toList();}catch(Exception e){return List.of();}}
  private static void validate(ObjectNode a){for(String k:List.of("name","system_prompt","model","llm","memory","tools"))if(!a.has(k))throw new IllegalArgumentException("agent missing "+k);JsonNode w=a.path("workflow");if(w.isMissingNode())return;Set<String> ids=new HashSet<>();for(JsonNode p:w.path("phases"))if(!ids.add(p.path("id").asText()))throw new IllegalArgumentException("workflow phase IDs must be unique");if(!ids.contains(w.path("entry_phase").asText()))throw new IllegalArgumentException("workflow.entry_phase must name a configured phase");for(JsonNode p:w.path("phases"))for(JsonNode t:p.path("transitions")){String to=t.path("to").asText();if(!"complete".equals(to)&&!ids.contains(to))throw new IllegalArgumentException("workflow transition targets unknown phase");if("evidence".equals(t.path("when").asText())&&t.path("evidence").isEmpty())throw new IllegalArgumentException("workflow evidence transition requires evidence");}}
  private String expand(String s){Matcher m=ENV.matcher(s);StringBuffer b=new StringBuffer();while(m.find())m.appendReplacement(b,Matcher.quoteReplacement(environment.getProperty(m.group(1),m.group(2)==null?"":m.group(2))));m.appendTail(b);return b.toString();}
  private static String hex(byte[] b){StringBuilder s=new StringBuilder();for(byte x:b)s.append(String.format("%02x",x));return s.toString();}
}
