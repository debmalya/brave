package brave.propagation;

import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Allows you to propagate predefined request-scoped fields, usually but not always HTTP headers.
 *
 * <p>For example, if you are in a Cloud Foundry environment, you might want to pass the request
 * ID:
 * <pre>{@code
 * // when you initialize the builder, define the extra field you want to propagate
 * tracingBuilder.propagationFactory(
 *   ExtraFieldPropagation.newFactory(B3Propagation.FACTORY, "x-vcap-request-id")
 * );
 *
 * // later, you can tag that request ID or use it in log correlation
 * requestId = ExtraFieldPropagation.current("x-vcap-request-id");
 *
 * // You can also set or override the value similarly, which might be needed if a new request
 * ExtraFieldPropagation.current("x-country-code", "FO");
 * }</pre>
 *
 * <p>You may also need to propagate a trace context you aren't using. For example, you may be in an
 * Amazon Web Services environment, but not reporting data to X-Ray. To ensure X-Ray can co-exist
 * correctly, pass-through its tracing header like so.
 *
 * <pre>{@code
 * tracingBuilder.propagationFactory(
 *   ExtraFieldPropagation.newFactory(B3Propagation.FACTORY, "x-amzn-trace-id")
 * );
 * }</pre>
 */
public final class ExtraFieldPropagation<K> implements Propagation<K> {

  /** Wraps an underlying propagation implementation, pushing one or more fields */
  public static Propagation.Factory newFactory(Propagation.Factory delegate, String... names) {
    return new Factory(delegate, Arrays.asList(names));
  }

  /** Wraps an underlying propagation implementation, pushing one or more fields */
  public static Propagation.Factory newFactory(Propagation.Factory delegate,
      Collection<String> names) {
    return new Factory(delegate, names);
  }

  /** Returns the value of the field with the specified key or null if not available */
  @Nullable public static String current(String name) {
    TraceContext context = currentTraceContext();
    return context != null ? get(context, name) : null;
  }

  /** Sets the current value of the field with the specified key */
  @Nullable public static void current(String name, String value) {
    TraceContext context = currentTraceContext();
    if (context != null) set(context, name, value);
  }

  @Nullable static TraceContext currentTraceContext() {
    Tracing tracing = Tracing.current();
    return tracing != null ? tracing.currentTraceContext().get() : null;
  }

  /** Returns the value of the field with the specified key or null if not available */
  @Nullable public static String get(TraceContext context, String name) {
    if (context == null) throw new NullPointerException("context == null");
    if (name == null) throw new NullPointerException("name == null");
    name = name.toLowerCase(Locale.ROOT); // since not all propagation handle mixed case
    for (Object extra : context.extra()) {
      if (extra instanceof Extra) return ((Extra) extra).get(name);
    }
    return null;
  }

  /** Returns the value of the field with the specified key or null if not available */
  @Nullable public static void set(TraceContext context, String name, String value) {
    if (context == null) throw new NullPointerException("context == null");
    if (name == null) throw new NullPointerException("name == null");
    name = name.toLowerCase(Locale.ROOT); // since not all propagation handle mixed case
    if (value == null) throw new NullPointerException("value == null");
    for (Object extra : context.extra()) {
      if (extra instanceof Extra) {
        ((Extra) extra).set(name, value);
        return;
      }
    }
  }

  static final class Factory extends Propagation.Factory {
    final Propagation.Factory delegate;
    final List<String> names;

    Factory(Propagation.Factory delegate, Collection<String> names) {
      if (delegate == null) throw new NullPointerException("field == null");
      if (names == null) throw new NullPointerException("names == null");
      if (names.isEmpty()) throw new NullPointerException("names.length == 0");
      this.delegate = delegate;
      this.names = new ArrayList<>();
      for (String name : names) {
        this.names.add(name.toLowerCase(Locale.ROOT));
      }
    }

    @Override public boolean supportsJoin() {
      return delegate.supportsJoin();
    }

    @Override public boolean requires128BitTraceId() {
      return delegate.requires128BitTraceId();
    }

    @Override public final <K> Propagation<K> create(Propagation.KeyFactory<K> keyFactory) {
      Map<String, K> names = new LinkedHashMap<>();
      for (String name : this.names) {
        names.put(name, keyFactory.create(name));
      }
      return new ExtraFieldPropagation<>(delegate.create(keyFactory), names);
    }

    @Override public TraceContext decorate(TraceContext context) {
      TraceContext result = delegate.decorate(context);
      for (Object extra : result.extra()) {
        if (extra instanceof Extra) return result;
      }
      List<Object> extra = new ArrayList<>(result.extra());
      extra.add(new Extra());
      return result.toBuilder().extra(Collections.unmodifiableList(extra)).build();
    }
  }

  final Propagation<K> delegate;
  final List<K> keys;
  final Map<String, K> nameToKey;

  ExtraFieldPropagation(Propagation<K> delegate, Map<String, K> nameToKey) {
    this.delegate = delegate;
    this.nameToKey = nameToKey;
    List<K> keys = new ArrayList<>(delegate.keys());
    keys.addAll(nameToKey.values());
    this.keys = Collections.unmodifiableList(keys);
  }

  @Override public List<K> keys() {
    return keys;
  }

  @Override public <C> Injector<C> injector(Setter<C, K> setter) {
    return new ExtraFieldInjector<>(delegate.injector(setter), setter, nameToKey);
  }

  @Override public <C> Extractor<C> extractor(Getter<C, K> getter) {
    Extractor<C> extractorDelegate = delegate.extractor(getter);
    return new ExtraFieldExtractor<>(extractorDelegate, getter, nameToKey);
  }

  static final class Extra {
    final LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    void set(String name, String value) {
      synchronized (fields) {
        fields.put(name, value);
      }
    }

    String get(String name) {
      final String result;
      synchronized (fields) {
        result = fields.get(name);
      }
      return result;
    }

    <C, K> void setAll(C carrier, Setter<C, K> setter, Map<String, K> nameToKey) {
      final Set<Map.Entry<String, String>> entrySet;
      synchronized (fields) {
        entrySet = new LinkedHashSet<>(fields.entrySet());
      }
      for (Map.Entry<String, String> field : entrySet) {
        K key = nameToKey.get(field.getKey());
        if (key == null) continue;
        setter.put(carrier, nameToKey.get(field.getKey()), field.getValue());
      }
    }

    @Override public String toString() {
      return "ExtraFieldPropagation" + fields;
    }
  }

  static final class ExtraFieldInjector<C, K> implements Injector<C> {
    final Injector<C> delegate;
    final Propagation.Setter<C, K> setter;
    final Map<String, K> nameToKey;

    ExtraFieldInjector(Injector<C> delegate, Setter<C, K> setter, Map<String, K> nameToKey) {
      this.delegate = delegate;
      this.setter = setter;
      this.nameToKey = nameToKey;
    }

    @Override public void inject(TraceContext traceContext, C carrier) {
      for (Object extra : traceContext.extra()) {
        if (extra instanceof Extra) {
          ((Extra) extra).setAll(carrier, setter, nameToKey);
          break;
        }
      }
      delegate.inject(traceContext, carrier);
    }
  }

  static final class ExtraFieldExtractor<C, K> implements Extractor<C> {
    final Extractor<C> delegate;
    final Propagation.Getter<C, K> getter;
    final Map<String, K> names;

    ExtraFieldExtractor(Extractor<C> delegate, Getter<C, K> getter, Map<String, K> names) {
      this.delegate = delegate;
      this.getter = getter;
      this.names = names;
    }

    @Override public TraceContextOrSamplingFlags extract(C carrier) {
      TraceContextOrSamplingFlags result = delegate.extract(carrier);

      Extra extra = new Extra(); // always allocate in case fields are added late
      for (Map.Entry<String, K> field : names.entrySet()) {
        String maybeValue = getter.get(carrier, field.getValue());
        if (maybeValue == null) continue;
        extra.set(field.getKey(), maybeValue);
      }
      return result.toBuilder().addExtra(extra).build();
    }
  }
}
