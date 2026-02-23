package com.pm.patientservice.grpc;

import billing.BillingRequest;
import billing.BillingResponse;
import billing.BillingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BillingServiceGrpClient {
    private static final Logger log = LoggerFactory.getLogger(BillingServiceGrpClient.class);
    private final BillingServiceGrpc.BillingServiceBlockingStub blockingStub;

    public BillingServiceGrpClient(
            @Value("${billing.service.address:localhost}") String serverAddress,
            @Value("${billing.service.grpc.port:9001}") int serverPort
    ){
        log.info("Connecting to billing service at {}:{}",serverAddress,serverPort);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(serverAddress,serverPort)
                .usePlaintext()
                .build();

        blockingStub = BillingServiceGrpc.newBlockingStub(channel);
    }

    public BillingResponse createBillingAccount(String patientId, String name, String email){
        BillingRequest request = BillingRequest.newBuilder()
                .setEmail(email)
                .setName(name)
                .setPatientId(patientId)
                .build();

        BillingResponse response = blockingStub.createBillingAccount(request);

        log.info("Received response from billing service via GRPC: {}", response);
        return  response;
    }
}
