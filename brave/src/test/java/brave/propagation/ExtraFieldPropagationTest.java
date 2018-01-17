package brave.propagation;

import brave.Tracing;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

import static brave.propagation.Propagation.KeyFactory.STRING;
import static org.assertj.core.api.Assertions.assertThat;

public class ExtraFieldPropagationTest {
  Propagation.Factory factory = ExtraFieldPropagation.newFactory(
      B3Propagation.FACTORY, "x-vcap-request-id", "x-amzn-trace-id"
  );
  Map<String, String> carrier = new LinkedHashMap<>();
  TraceContext.Injector<Map<String, String>> injector = factory.create(STRING).injector(Map::put);
  TraceContext.Extractor<Map<String, String>> extractor =
      factory.create(STRING).extractor(Map::get);

  String awsTraceId =
      "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=1";
  String uuid = "f4308d05-2228-4468-80f6-92a8377ba193";

  TraceContext context = factory.decorate(TraceContext.newBuilder()
      .traceId(1L)
      .spanId(2L)
      .sampled(true)
      .build());

  @Test public void get() {
    context = contextWithAmazonTraceId();

    assertThat(ExtraFieldPropagation.get(context, "x-amzn-trace-id"))
        .isEqualTo(awsTraceId);
  }

  @Test public void get_null_if_not_extraField() {
    assertThat(ExtraFieldPropagation.get(context, "x-amzn-trace-id"))
        .isNull();
  }

  @Test public void current_get() {
    context = contextWithAmazonTraceId();

    try (Tracing t = Tracing.newBuilder().propagationFactory(factory).build();
         CurrentTraceContext.Scope scope = t.currentTraceContext().newScope(context)) {
      assertThat(ExtraFieldPropagation.current("x-amzn-trace-id"))
          .isEqualTo(awsTraceId);
    }
  }

  @Test public void current_get_null_if_no_current_context() {
    try (Tracing t = Tracing.newBuilder().propagationFactory(factory).build()) {
      assertThat(ExtraFieldPropagation.current("x-amzn-trace-id"))
          .isNull();
    }
  }

  @Test public void current_get_null_if_nothing_current() {
    assertThat(ExtraFieldPropagation.current("x-amzn-trace-id"))
        .isNull();
  }

  @Test public void current_set() {
    try (Tracing t = Tracing.newBuilder().propagationFactory(factory).build();
         CurrentTraceContext.Scope scope = t.currentTraceContext().newScope(context)) {
      ExtraFieldPropagation.current("x-amzn-trace-id", awsTraceId);

      assertThat(ExtraFieldPropagation.current("x-amzn-trace-id"))
          .isEqualTo(awsTraceId);
    }
  }

  @Test public void current_set_noop_if_no_current_context() {
    try (Tracing t = Tracing.newBuilder().propagationFactory(factory).build()) {
      ExtraFieldPropagation.current("x-amzn-trace-id", awsTraceId); // doesn't throw
    }
  }

  @Test public void current_set_noop_if_nothing_current() {
    ExtraFieldPropagation.current("x-amzn-trace-id", awsTraceId); // doesn't throw
  }

  @Test public void toString_extra() {
    ExtraFieldPropagation.Extra extra = new ExtraFieldPropagation.Extra();
    extra.set("x-vcap-request-id", uuid);

    assertThat(extra)
        .hasToString("ExtraFieldPropagation{x-vcap-request-id=" + uuid + "}");
  }

  @Test public void toString_two() {
    ExtraFieldPropagation.Extra extra = new ExtraFieldPropagation.Extra();
    extra.set("x-amzn-trace-id", awsTraceId);
    extra.set("x-vcap-request-id", uuid);

    assertThat(extra).hasToString(
        "ExtraFieldPropagation{x-amzn-trace-id=" + awsTraceId + ", x-vcap-request-id=" + uuid + "}"
    );
  }

  @Test public void inject_extra() {
    ExtraFieldPropagation.Extra extra = new ExtraFieldPropagation.Extra();
    extra.set("x-vcap-request-id", uuid);
    context = context.toBuilder().extra(Collections.singletonList(extra)).build();

    injector.inject(context, carrier);

    assertThat(carrier).containsEntry("x-vcap-request-id", uuid);
  }

  @Test public void inject_two() {
    ExtraFieldPropagation.Extra extra = new ExtraFieldPropagation.Extra();
    extra.set("x-amzn-trace-id", awsTraceId);
    extra.set("x-vcap-request-id", uuid);
    context = context.toBuilder().extra(Collections.singletonList(extra)).build();

    injector.inject(context, carrier);

    assertThat(carrier)
        .containsEntry("x-amzn-trace-id", awsTraceId)
        .containsEntry("x-vcap-request-id", uuid);
  }

  @Test public void extract_extra() {
    Propagation.B3_STRING.<Map<String, String>>injector(Map::put).inject(context, carrier);
    carrier.put("x-amzn-trace-id", awsTraceId);

    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);
    assertThat(extracted.context().toBuilder().extra(Collections.emptyList()).build())
        .isEqualTo(context);
    assertThat(extracted.context().extra())
        .hasSize(1);

    ExtraFieldPropagation.Extra extra =
        (ExtraFieldPropagation.Extra) extracted.context().extra().get(0);
    assertThat(extra.fields)
        .hasSize(1)
        .containsEntry("x-amzn-trace-id", awsTraceId);
  }

  @Test public void extract_two() {
    Propagation.B3_STRING.<Map<String, String>>injector(Map::put).inject(context, carrier);
    carrier.put("x-amzn-trace-id", awsTraceId);
    carrier.put("x-vcap-request-id", uuid);

    TraceContextOrSamplingFlags extracted = extractor.extract(carrier);
    assertThat(extracted.context().toBuilder().extra(Collections.emptyList()).build())
        .isEqualTo(context);
    assertThat(extracted.context().extra())
        .hasSize(1);

    ExtraFieldPropagation.Extra extra =
        (ExtraFieldPropagation.Extra) extracted.context().extra().get(0);
    assertThat(extra.fields)
        .containsEntry("x-amzn-trace-id", awsTraceId)
        .containsEntry("x-vcap-request-id", uuid);
  }

  TraceContext contextWithAmazonTraceId() {
    Propagation.B3_STRING.<Map<String, String>>injector(Map::put).inject(context, carrier);
    carrier.put("x-amzn-trace-id", awsTraceId);
    return extractor.extract(carrier).context();
  }
}
