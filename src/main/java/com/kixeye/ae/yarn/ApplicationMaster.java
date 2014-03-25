package com.kixeye.ae.yarn;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

public class ApplicationMaster {
  private Configuration conf;
  private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());
  private Options opts;

  public ApplicationMaster() {
    conf = new YarnConfiguration();
    opts = new Options();

    opts.addOption(Constants.OPT_CONTAINER_MEM, true, "container memory");
    opts.addOption(Constants.OPT_CONTAINER_COUNT, true, "number of containers");


  }


}
