package com.kixeye.ae.yarn;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ApplicationMaster {
  private AMRMClientAsync<ContainerRequest> resourceManager;
  private NMClient nodeManager;

  private Configuration conf;
  private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());
  private Options opts;
  private int containerMem;

  private int containerCount;
  private AtomicInteger completedContainerCount = new AtomicInteger();
  private AtomicInteger allocatedContainerCount = new AtomicInteger();
  private AtomicInteger failedContainerCount = new AtomicInteger();
  private AtomicInteger requestedContainerCount = new AtomicInteger();

  private String appMasterHostname = "";     //TODO: What should this really be?
  private int appMasterRpcPort = 0;          //TODO: What should this really be?
  private String appMasterTrackingUrl = "";  //TODO: What should this really be?

  private boolean done;

  public ApplicationMaster() {
    conf = new YarnConfiguration();
    opts = new Options();

    opts.addOption(Constants.OPT_CONTAINER_MEM, true, "container memory");
    opts.addOption(Constants.OPT_CONTAINER_COUNT, true, "number of containers");
  }

  public void init(String[] args) throws ParseException {
    LOG.setLevel(Level.INFO);
    CommandLine cliParser = new GnuParser().parse(this.opts, args);
    done = false;

    this.containerMem = Integer.parseInt( cliParser.getOptionValue(Constants.OPT_CONTAINER_MEM) );
    this.containerCount = Integer.parseInt( cliParser.getOptionValue(Constants.OPT_CONTAINER_COUNT) );
  }

  public boolean run() throws IOException, YarnException {
    // Initialize clients to RM and NMs.
    AMRMClientAsync.CallbackHandler rmListener = new RMCallbackHandler();
    resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, rmListener);
    resourceManager.init(conf);
    resourceManager.start();

    nodeManager = NMClient.createNMClient();
    nodeManager.init(conf);
    nodeManager.start();

    // Register with RM
    resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);


    // Ask RM to give us a bunch of containers
    for (int i = 0; i < containerCount; i++) {
      ContainerRequest containerReq = setupContainerReqForRM();
      resourceManager.addContainerRequest(containerReq);
    }

    while (!done) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
      }
    }// while

    // Un-register with ResourceManager
    resourceManager.unregisterApplicationMaster( FinalApplicationStatus.SUCCEEDED, "", "");
    return true;
  }

  private ContainerRequest setupContainerReqForRM() {
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);
    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMem);
    //capability.setVirtualCores(1);
    ContainerRequest containerReq = new ContainerRequest(
        capability,
        null /* hosts String[] */,
        null /* racks String [] */,
        priority);
    return containerReq;
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    // CallbackHandler for RM.
    // Execute a program when the container is allocated
    // Reallocate upon failure.


    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus status: statuses) {
        assert (status.getState() == ContainerState.COMPLETE);

        int exitStatus = status.getExitStatus();
        if (exitStatus != ContainerExitStatus.SUCCESS) {
          if (exitStatus != ContainerExitStatus.ABORTED) {
            failedContainerCount.incrementAndGet();
          }
          allocatedContainerCount.decrementAndGet();
          requestedContainerCount.decrementAndGet();
        } else {
          completedContainerCount.incrementAndGet();
        }
      }

      int askAgainCount = containerCount - requestedContainerCount.get();
      requestedContainerCount.addAndGet(askAgainCount);

      if (askAgainCount > 0) {
        // need to reallocate failed containers
        for (int i = 0; i < askAgainCount; i++) {
          ContainerRequest req = setupContainerReqForRM();
          resourceManager.addContainerRequest(req);
        }
      }

      if (completedContainerCount.get() == containerCount) {
        done = true;
      }
    }

    public void onContainersAllocated(List<Container> containers) {

    }

    public void onNodesUpdated(List<NodeReport> updated) {

    }

    public void onError(Throwable e) {

    }

    // Called when the ResourceManager wants the ApplicationMaster to shutdown
    // for being out of sync etc. The ApplicationMaster should not unregister
    // with the RM unless the ApplicationMaster wants to be the last attempt.
    public void onShutdownRequest() {

    }

    public float getProgress() {
      return 0;
    }
  }

}
