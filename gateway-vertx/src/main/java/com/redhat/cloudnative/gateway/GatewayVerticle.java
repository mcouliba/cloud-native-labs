package com.redhat.cloudnative.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.client.WebClient;
import io.vertx.rxjava.ext.web.codec.BodyCodec;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import io.vertx.rxjava.servicediscovery.ServiceDiscovery;
import io.vertx.rxjava.servicediscovery.types.HttpEndpoint;
import rx.Observable;
import rx.Single;

public class GatewayVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayVerticle.class);
    
    private WebClient catalog;
    private WebClient inventory;
    // Cart Attribute
    private WebClient cart;
    
    @Override
    public void start() {
        Router router = Router.router(vertx);
        // Enable TraceInterceptor handler
//        router.route()
//            .order(-1)
//            .handler(TracingInterceptor.create());
        router.route()
            .handler(CorsHandler.create("*")
                .allowedMethod(HttpMethod.GET));
        router.get("/*").handler(StaticHandler.create("assets"));
        router.get("/health").handler(this::health);
        router.get("/api/products").handler(this::products);
        // Cart Route
//        router.get("/api/cart/:cardId").handler(this::getCartHandler);
        
        ServiceDiscovery.create(vertx, discovery -> {
             // Catalog lookup
             Single<WebClient> catalogDiscoveryRequest = HttpEndpoint.rxGetWebClient(discovery,
             rec -> rec.getName().equals("catalog"))
             .onErrorReturn(t -> WebClient.create(vertx, new WebClientOptions()
                     .setDefaultHost(System.getProperty("catalog.api.host", "localhost"))
                     .setDefaultPort(Integer.getInteger("catalog.api.port", 9000))));

            // Inventory lookup
            Single<WebClient> inventoryDiscoveryRequest = HttpEndpoint.rxGetWebClient(discovery,
                    rec -> rec.getName().equals("inventory"))
                    .onErrorReturn(t -> WebClient.create(vertx, new WebClientOptions()
                            .setDefaultHost(System.getProperty("inventory.api.host", "localhost"))
                            .setDefaultPort(Integer.getInteger("inventory.api.port", 9001))));

            // Cart lookup
            Single<WebClient> cartDiscoveryRequest = HttpEndpoint.rxGetWebClient(discovery,
                    rec -> rec.getName().equals("cart"))
                    .onErrorReturn(t -> WebClient.create(vertx, new WebClientOptions()
                            .setDefaultHost(System.getProperty("inventory.api.host", "localhost"))
                            .setDefaultPort(Integer.getInteger("inventory.api.port", 9002))));

            // Zip all 3 requests
            Single.zip(catalogDiscoveryRequest, inventoryDiscoveryRequest, cartDiscoveryRequest,
                (cg, i, ct) -> {
                    // When everything is done
                    catalog = cg;
                    inventory = i;
                    cart = ct;
                    return vertx.createHttpServer()
                        .requestHandler(router::accept)
                        .listen(Integer.getInteger("http.port", 8080));
                }).subscribe();
        });
    }
    
    private void products(RoutingContext rc) {
        // Retrieve catalog
        catalog.get("/api/catalog")
            .as(BodyCodec.jsonArray()).rxSend()
            .map(resp -> {
                if (resp.statusCode() != 200) {
                    new RuntimeException("Invalid response from the catalog: " + resp.statusCode());
                }
                return resp.body();
            })
            .flatMap(products ->
                // For each item from the catalog, invoke the inventory service
                Observable.from(products)
                    .cast(JsonObject.class)
                    .flatMapSingle(product ->
                        inventory.get("/api/inventory/" + product.getString("itemId"))
                            .as(BodyCodec.jsonObject())
                            .rxSend()
                            .map(resp -> {
                                if (resp.statusCode() != 200) {
                                    LOG.warn("Inventory error for {}: status code {}",
                                            product.getString("itemId"), resp.statusCode());
                                    return product.copy();
                                }
                                
                                return product.copy().put("availability", 
                                    new JsonObject().put("quantity", resp.body().getInteger("quantity")));
                            }))
                    .toList().toSingle()
            )
            .subscribe(
                list -> rc.response().end(Json.encodePrettily(list)),
                error -> rc.response().end(new JsonObject().put("error", error.getMessage()).toString())
            );
    }
    
    private void getCartHandler(RoutingContext rc) {
        String cardId = rc.request().getParam("cardId");
        
        // Retrieve cart
        cart
        .get("/api/cart/" + cardId)
        .as(BodyCodec.jsonObject())
        .rxSend()
        .subscribe(
            resp -> {
                if (resp.statusCode() != 200) {
                    new RuntimeException("Invalid response from the cart: " + resp.statusCode());
                }
                rc.response().end(Json.encodePrettily(resp.body()));
            },
            error -> rc.response().end(new JsonObject().put("error", error.getMessage()).toString())
        );
    }

    private void health(RoutingContext rc) {
        // Check Catalog and Inventory Service up and running
        catalog.get("/").rxSend()
            .subscribe(
                catalogCallOk -> {
                    inventory.get("/").rxSend()
                        .subscribe(
                            inventoryCallOk -> rc.response().setStatusCode(200).end(new JsonObject().put("status", "UP").toString()),
                            error -> rc.response().setStatusCode(503).end(new JsonObject().put("status", "DOWN").toString())
                        );
                },
                error -> rc.response().setStatusCode(503).end(new JsonObject().put("status", "DOWN").toString())
            );
    }
}