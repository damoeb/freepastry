/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
/*
 * Created on Apr 6, 2005
 */
package rice.environment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import rice.Destructible;
import rice.environment.exception.ExceptionStrategy;
import rice.environment.exception.simple.SimpleExceptionStrategy;
import rice.environment.logging.LogManager;
import rice.environment.logging.Slf4jLogManager;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.processing.simple.SimpleProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simple.SimpleTimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.selector.SelectorLoop;


/**
 * Used to provide properties, timesource, loggers etc to the FreePastry
 * apps and components.
 *
 * @author Jeff Hoye
 */
public class Environment implements Destructible {
  @SuppressWarnings("MutablePublicArray") // public API, passed to the Environment constructors
  public static final String[] defaultParamFileArray = {"freepastry"};

  private SelectorLoop selectorManager;
  private Processor processor;
  private RandomSource randomSource;
  private TimeSource time;
  private LogManager logManager;
  private Parameters params;
  private Logger logger;
  private ExceptionStrategy exceptionStrategy;

  private HashSet<Destructible> destructables = new HashSet<Destructible>();

  // the data-path futures still pending; destroy() fails them
  private final PendingFutures pendingFutures = new PendingFutures();

  /**
   * Constructor.  You can provide null values for all parameters except params, which will result
   * in a default choice.  If you want different defaults, consider extending Environment
   * and providing your own chooseDefaults() method.
   *
   * @param selectorLoop the SelectorLoop.  Default: rice.selector.SelectorLoop
   * @param proc the Processor.  Default: rice.environment.processing.simple.SimpleProcessor,
   *   or rice.environment.processing.sim.SimProcessor if environment_use_sim_processor is set
   * @param randomSource the RandomSource.  Default: rice.environment.random.simple.SimpleRandomSource
   * @param time the TimeSource.  Default: rice.environment.time.simple.SimpleTimeSource
   * @param logManager the LogManager.  Default: rice.environment.logging.Slf4jLogManager
   * @param params the Parameters.  Required.
   * @param strategy the ExceptionStrategy.  Default: rice.environment.exception.simple.SimpleExceptionStrategy
   * @throws IllegalArgumentException if params is null
   */
  @SuppressWarnings("NullAway") // chooseDefaults() replaces every null argument before the fields are used
  public Environment(@Nullable SelectorLoop selectorLoop, @Nullable Processor proc, @Nullable RandomSource randomSource, @Nullable TimeSource time, @Nullable LogManager logManager, Parameters params, @Nullable ExceptionStrategy strategy) {
    this.selectorManager = selectorLoop;
    this.randomSource = randomSource;
    this.time = time;
    this.logManager = logManager;
    this.params = params;
    this.processor = proc;
    this.exceptionStrategy = strategy;

    if (params == null) {
      throw new IllegalArgumentException("params cannot be null");
    }

    // choose defaults for all non-specified parameters
    chooseDefaults();
    logger = this.logManager.getLogger(getClass(), null);


    this.selectorManager.setEnvironment(this);

    addDestructable(this.time);
  }

  /**
   * Convenience for defaults.
   *
   * @param orderedDefaultFiles the parameter files to load from the classpath, in order
   * @param paramFileName the file where parameters are saved
   */
  public Environment(String[] orderedDefaultFiles, @Nullable String paramFileName) {
    this(null,null,null,null,null,new SimpleParameters(orderedDefaultFiles,paramFileName), null);
  }

  public static Environment directEnvironment(int randomSeed) {
    SimpleRandomSource srs = new SimpleRandomSource(randomSeed, null);
    Environment env = directEnvironment(srs);
    srs.setLogManager(env.getLogManager());
    return env;
  }

  public static Environment directEnvironment() {
    return directEnvironment(null);
  }

  public static Environment directEnvironment(@Nullable RandomSource randomSource) {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);
    DirectTimeSource dts = new DirectTimeSource(params);
    LogManager logManager = generateDefaultLogManager();
    dts.setLogManager(logManager);
    SelectorLoop selector = generateDefaultSelectorManager(dts,logManager,randomSource);
    dts.setSelectorManager(selector);
    Processor proc = new SimProcessor(selector);
    Environment ret = new Environment(selector,proc,randomSource,dts,logManager,
        params, generateDefaultExceptionStrategy(logManager));
    return ret;
  }

  public Environment(@Nullable String paramFileName) {
    this(defaultParamFileArray,paramFileName);
  }

  /**
   * Convenience for defaults.  Has no parameter file to load/store.
   */
  public Environment() {
    this(null);
  }

  /**
   * Can be easily overridden by a subclass.
   */
  protected void chooseDefaults() {
    // choose defaults for all non-specified parameters
    if (time == null) {
      time = generateDefaultTimeSource();
    }
    if (logManager == null) {
      logManager = generateDefaultLogManager();
    }
    if (randomSource == null) {
      randomSource = generateDefaultRandomSource(params,logManager);
    }
    if (selectorManager == null) {
      selectorManager = generateDefaultSelectorManager(time, logManager, randomSource);
    }
    if (processor == null) {
      if (EnvironmentConfig.fromParameters(params).useSimProcessor()) {
        processor = new SimProcessor(selectorManager);
      } else {
        processor = generateDefaultProcessor();
      }
    }

    if (exceptionStrategy == null) {
      exceptionStrategy = generateDefaultExceptionStrategy(logManager);
    }
  }

  public static ExceptionStrategy generateDefaultExceptionStrategy(LogManager manager) {
    return new SimpleExceptionStrategy(manager);
  }

  public static RandomSource generateDefaultRandomSource(Parameters params, LogManager logging) {
    OptionalLong seed = EnvironmentConfig.fromParameters(params).randomSeed();
    if (seed.isEmpty()) {
      return new SimpleRandomSource(logging);
    }
    return new SimpleRandomSource(seed.getAsLong(), logging);
  }

  public static TimeSource generateDefaultTimeSource() {
    return new SimpleTimeSource();
  }

  /**
   * @return the default LogManager, which hands out SLF4J loggers
   */
  public static LogManager generateDefaultLogManager() {
    return new Slf4jLogManager();
  }

  public static SelectorLoop generateDefaultSelectorManager(TimeSource time, LogManager logging, @Nullable RandomSource randomSource) {
    return new SelectorLoop("Default", time, logging, randomSource);
  }

  public static Processor generateDefaultProcessor() {
    return new SimpleProcessor("Default");
  }

  // Accessors
  public SelectorLoop getSelectorManager() {
    return selectorManager;
  }
  public Processor getProcessor() {
    return processor;
  }
  public RandomSource getRandomSource() {
    return randomSource;
  }
  public TimeSource getTimeSource() {
    return time;
  }
  public LogManager getLogManager() {
    return logManager;
  }
  public Parameters getParameters() {
    return params;
  }

  /**
   * Tears down the environment.  Calls params.store(), then on the selector thread destroys the
   * Destructables, fails the {@linkplain #track tracked} futures that are still pending with an
   * {@link EnvironmentDestroyedException}, and destroys the selector and the processor.
   */
  @Override
  public void destroy() {
    try {
      params.store();
    } catch (IOException ioe) {
      logger.warn("Error during shutdown", ioe);
    }
    if (getSelectorManager().isSelectorThread()) {
      callDestroyOnDestructables();
    } else {
      getSelectorManager().invoke(() -> callDestroyOnDestructables());
    }
  }

  private void callDestroyOnDestructables() {
    for (Destructible destructible : new ArrayList<Destructible>(destructables)) {
      destructible.destroy();
    }
    // nothing will complete them now: the selector is going away
    pendingFutures.failAll();
    selectorManager.destroy();
    processor.destroy();
  }

  /**
   * Registers a pending data-path future of this environment: {@link #destroy()} fails it with an
   * {@link EnvironmentDestroyedException} (on the selector thread) if it is still pending then, instead of leaving
   * its callers waiting forever. Once the environment is destroyed, a tracked future fails at once. A future is
   * forgotten when it completes. Every source of a data-path future that may stay pending calls this.
   *
   * @param future the future
   * @return {@code future}
   */
  public <T extends @Nullable Object> CompletableFuture<T> track(CompletableFuture<T> future) {
    return pendingFutures.track(future);
  }

  /**
   * @return how many tracked futures are still pending
   */
  int pendingFutureCount() {
    return pendingFutures.size();
  }

  public void addDestructable(Destructible destructable) {
    if (destructable == null) {
      logger.warn("addDestructable(null)", new Exception("Stack Trace"));
      return;
    }
    destructables.add(destructable);

  }

  public void removeDestructable(Destructible destructable) {
    if (destructable == null) {
      logger.warn("addDestructable(null)", new Exception("Stack Trace"));
      return;
    }
    destructables.remove(destructable);
  }

  public ExceptionStrategy getExceptionStrategy() {
    return exceptionStrategy;
  }

  public ExceptionStrategy setExceptionStrategy(ExceptionStrategy newStrategy) {
    ExceptionStrategy ret = exceptionStrategy;
    exceptionStrategy = newStrategy;
    return ret;
  }
  public Environment cloneEnvironment(String prefix) {
    return cloneEnvironment(prefix, false, false);
  }

  public Environment cloneEnvironment(String prefix, boolean cloneSelector, boolean cloneProcessor) {
    // new logManager
    LogManager lman = cloneLogManager(prefix);

    TimeSource timeSource = cloneTimeSource(lman);

    // new random source
    RandomSource rand = cloneRandomSource(lman);

    // new selector
    SelectorLoop sman = cloneSelectorManager(prefix, timeSource, rand, lman, cloneSelector);

    // new processor
    Processor proc = cloneProcessor(prefix, lman, cloneProcessor);

    // build the environment
    Environment ret = new Environment(sman, proc, rand, getTimeSource(), lman,
        getParameters(), getExceptionStrategy());

    // gain shared fate with the rootEnvironment
    addDestructable(ret);

    return ret;
  }

  protected TimeSource cloneTimeSource(LogManager lman) {
    return getTimeSource();
  }

  protected LogManager cloneLogManager(String prefix) {
    LogManager lman = getLogManager();
    if (lman instanceof Slf4jLogManager slm) {
      lman = slm.clone(prefix);
    }
    return lman;
  }

  protected SelectorLoop cloneSelectorManager(String prefix, TimeSource timeSource, RandomSource randomSource, LogManager lman, boolean cloneSelector) {
    SelectorLoop sman = getSelectorManager();
    if (cloneSelector) {
      sman = new SelectorLoop(prefix + " Selector",
          timeSource, lman, randomSource);
    }
    return sman;
  }

  protected Processor cloneProcessor(String prefix, LogManager lman, boolean cloneProcessor) {
    Processor proc = getProcessor();
    if (cloneProcessor) {
      proc = new SimpleProcessor(prefix + " Processor");
    }

    return proc;
  }

  protected RandomSource cloneRandomSource(LogManager lman) {
    long randSeed = getRandomSource().nextLong();
    return new SimpleRandomSource(randSeed, lman);
  }
}
