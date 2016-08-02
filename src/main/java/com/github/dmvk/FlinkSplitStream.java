package com.github.dmvk;

import lombok.Value;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class FlinkSplitStream {

  private static final int N = 1000;

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final List<Msg> src = new ArrayList<>();
    final Msg.Type[] types = new Msg.Type[]{Msg.Type.FIRST, Msg.Type.SECOND, Msg.Type.THIRD};
    for (int i = 0; i < N; i++) {
      src.add(new Msg(types[i % 3], i));
    }

    final SplitStream<Msg> split = env.addSource(
        new FromIteratorFunction<>(new SerializableIterator<>(src))).returns(Msg.class)
        .split((OutputSelector<Msg>) m ->
            Collections.singletonList(m.type.toString()));

    split.select(Msg.Type.FIRST.toString())
        .addSink(new PrintSinkFunction<>());

    split.select(Msg.Type.SECOND.toString())
        .addSink(new PrintSinkFunction<>());

//    split.select(Msg.Type.THIRD.toString())
//        .addSink(new PrintSinkFunction<>());

    env.execute();
  }

  @Value
  private static class Msg implements Serializable {
    enum Type {FIRST, SECOND, THIRD}
    private final Type type;
    private final int id;
  }

  private static class SerializableIterator<T> implements Iterator<T>, Serializable {

    private final List<T> l;
    private transient Iterator<T> iterator;

    SerializableIterator(List<T> l) {
      this.l = l;
    }

    private Iterator<T> getIterator() {
      if (iterator == null) {
        iterator = l.iterator();
      }
      return iterator;
    }

    @Override
    public boolean hasNext() {
      return getIterator().hasNext();
    }

    @Override
    public T next() {
      return getIterator().next();
    }
  }
}
