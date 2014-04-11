package com.demandcube.yarn;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;


public class PythonStreamingAM extends ApplicationMaster {
  //private static final Logger LOG = Logger.getLogger(PythonStreamingAM.class.getName());

  public PythonStreamingAM() {
    super();
  }

  @Override
  protected List<String> buildCommandList(int startingFrom, int containerCnt, String commandTemplate) {
    // TODO Auto-generated method stub
    List<String> r = new ArrayList<String>();
    int stopAt = startingFrom + containerCnt;
    for (int i = startingFrom; i < stopAt; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append(commandTemplate).append(" ").append(String.valueOf(i));
      //String cmd = String.format(commandTemplate, i);
      String cmd = sb.toString();
      LOG.info("curr i : " + i);
      LOG.info(cmd);
      r.add(cmd);
    }
    return r;
  }

  public static void main(String[] args) {
    System.out.println("ApplicationMaster::main"); //xxx
    ApplicationMaster am = new PythonStreamingAM();
    try {
      am.init(args);
    } catch (ParseException e) {
      System.out.println("parse error: " + e);
      System.exit(0);
    }

    try {
      am.run();
    } catch (Exception e) {
      System.out.println("am.run throws: " + e);
      e.printStackTrace();
      System.exit(0);
    }
  }

}
