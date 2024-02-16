package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.experimental.UtilityClass;
import org.mockito.Mockito;

@UtilityClass
public class LambdaTestUtils {

  public <T, U> Function<T, U> spiedFunction(Function<T, U> func) {
    Function<T, U> spy = (Function<T, U>) Mockito.spy(Function.class);
    Mockito.doAnswer(it -> {
      T item = (T) it.getArguments()[0];
      return func.apply(item);
    }).when(spy).apply(Mockito.any());
    return spy;
  }

  public <T> Consumer<T> spiedConsumer(Consumer<T> consumer) {
    Consumer<T> spy = (Consumer<T>) Mockito.spy(Consumer.class);
    Mockito.doAnswer(it -> {
      T item = (T) it.getArguments()[0];
      consumer.accept(item);
      return null;
    }).when(spy).accept(Mockito.any());
    return spy;
  }

  public <T, U> BiConsumer<T, U> spiedBiConsumer(BiConsumer<T, U> biConsumer) {
    BiConsumer<T, U> spy = (BiConsumer<T, U>) Mockito.spy(BiConsumer.class);
    Mockito.doAnswer(it -> {
      biConsumer.accept((T) it.getArguments()[0], (U) it.getArguments()[1]);
      return null;
    }).when(spy).accept(Mockito.any(), Mockito.any());
    return spy;
  }


  public BooleanSupplier spiedBooleanSupplier(BooleanSupplier consumer) {
    BooleanSupplier spy = (BooleanSupplier) Mockito.spy(BooleanSupplier.class);
    Mockito.doAnswer(it -> {
      consumer.getAsBoolean();
      return null;
    }).when(spy).getAsBoolean();
    return spy;
  }

//  public <T> Consumer<T, U> spiedBiConsumer(Consumer<T> consumer) {
//    Consumer<T> spy = (Consumer<T>) Mockito.spy(Consumer.class);
//    Mockito.doAnswer(it -> {
//      T item = (T) it.getArguments()[0];
//      consumer.accept(item);
//      return null;
//    }).when(spy).accept(Mockito.any());
//    return spy;
//  }
}
