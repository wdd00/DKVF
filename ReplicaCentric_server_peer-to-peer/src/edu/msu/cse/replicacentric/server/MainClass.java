package edu.msu.cse.replicacentric.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
    public static void main(String args[]) {
        ConfigReader cnfReader = new ConfigReader(args[0]);
        ReplicaCentricServer rcServer = new ReplicaCentricServer(cnfReader);
        rcServer.runAll();
    }
}
