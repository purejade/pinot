package com.linkedin.pinot.server.starter;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.server.conf.ServerConf;


/**
*
* A Standalone Server which will run on the port configured in the properties file.
* and accept queries. All configurations needed to run the server is provided
* in the config file passed. No external cluster manager integration available (yet)
*
*/
public class SingleNodeServerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(SingleNodeServerStarter.class);

  public static final String SERVER_CONFIG_OPT_NAME = "server_conf";
  public static final String PINOT_PROPERTIES = "pinot.properties";

  private static String _serverConfigPath;
  private static ServerConf _serverConf;
  private static ServerInstance _serverInstance;

  private static void processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = new Options();
    cliOptions.addOption(SERVER_CONFIG_OPT_NAME, true, "Server Config file");
    CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);

    if (!cmd.hasOption(SERVER_CONFIG_OPT_NAME)) {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    _serverConfigPath = cmd.getOptionValue(SERVER_CONFIG_OPT_NAME);
    buildServerConfig(new File(_serverConfigPath, PINOT_PROPERTIES));
  }

  /**
   * Construct from config file path
   * @param configFilePath Path to the config file
   * @return 
   * @throws Exception
   */
  public static void buildServerConfig(File configFilePath) throws Exception {
    if (!configFilePath.exists()) {
      LOGGER.error("configuration file: " + configFilePath.getAbsolutePath() + " does not exist.");
      throw new ConfigurationException("configuration file: " + configFilePath.getAbsolutePath() + " does not exist.");
    }

    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(configFilePath);
    _serverConf = new ServerConf(serverConf);
  }

  public static class ShutdownHook extends Thread {
    private final ServerInstance _serverInstance;

    public ShutdownHook(ServerInstance serverInstance) {
      _serverInstance = serverInstance;
    }

    @Override
    public void run() {
      LOGGER.info("Running shutdown hook");
      if (_serverInstance != null) {
        _serverInstance.shutDown();
      }

      LOGGER.info("Shutdown completed !!");
    }
  }

  // Below is the sample arg put in the running parameters.
  // --server_conf src/test/resources/conf
  public static void main(String[] args) throws Exception {
    //Process Command Line to get config and port
    processCommandLineArgs(args);

    LOGGER.info("Trying to create a new ServerInstance!");
    _serverInstance = new ServerInstance();
    LOGGER.info("Trying to initial ServerInstance!");
    _serverInstance.init(_serverConf);
    LOGGER.info("Trying to start ServerInstance!");
    _serverInstance.start();

    LOGGER.info("Adding ShutdownHook!");
    ShutdownHook shutdownHook = new ShutdownHook(_serverInstance);
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    LOGGER.info("Wait for 10 seconds to shutdown");
    Thread.sleep(10000);

    LOGGER.info("Wake up! Try to shutdown ServerInstance!");
    _serverInstance.shutDown();
  }

}
