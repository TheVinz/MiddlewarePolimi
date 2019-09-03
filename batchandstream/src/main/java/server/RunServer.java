package main.java.server;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import main.java.server.actors.Supervisor;

import java.io.File;
import java.io.IOException;

public class RunServer {
    public static void main(String[] args) throws IOException {
        Config conf= ConfigFactory.parseFile(new File("conf/server.conf"));
        ActorSystem sys= ActorSystem.create("Server", conf);
        sys.actorOf(Supervisor.props(),"ServerActor");

        System.in.read();
        sys.terminate();
    }

}
