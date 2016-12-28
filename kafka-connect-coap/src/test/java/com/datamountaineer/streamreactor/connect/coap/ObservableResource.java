package com.datamountaineer.streamreactor.connect.coap;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.server.resources.CoapExchange;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by andrew@datamountaineer.com on 28/12/2016.
 * stream-reactor
 */
public class ObservableResource extends CoapResource {

    public ObservableResource(String name) {
      super(name);
      setObservable(true);
      setObserveType(CoAP.Type.CON);
    }

    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();


    @Override
    public void handleGET(CoapExchange exchange) {
      exchange.setMaxAge(2);

      if (queue.size() >= 0) {
        try {
          String msg = queue.take();
          System.out.println("Sending message " + msg);
          exchange.respond(CoAP.ResponseCode.CONTENT, msg);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void handlePOST(CoapExchange exchange) {
      try {
        queue.put(exchange.getRequestText());
        System.out.println("Got message " + exchange.getRequestText());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      exchange.respond(CoAP.ResponseCode.CHANGED);
    }

    @Override
    public void handlePUT(CoapExchange exchange) {
      try {
        queue.put(exchange.getRequestText());
        System.out.println("Got message " + exchange.getRequestText());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      exchange.respond(CoAP.ResponseCode.CHANGED);
    }
}


