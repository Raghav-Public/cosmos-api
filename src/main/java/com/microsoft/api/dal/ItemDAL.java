package com.microsoft.api.dal;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.azure.cosmos.ConnectionPolicy;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosClientException;
import com.azure.cosmos.CosmosPagedFlux;
import com.azure.cosmos.models.CosmosAsyncContainerResponse;
import com.azure.cosmos.models.CosmosAsyncDatabaseResponse;
import com.azure.cosmos.models.CosmosAsyncItemResponse;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.microsoft.api.model.StoreItem;

import reactor.core.publisher.Mono;

public class ItemDAL {
	
	private String host;
	private String masterKey;
	private String databaseName;
	private String collectionName;
	private String partitionId;
	
	private CosmosAsyncClient client;
	private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;
    
    
	public String getHost() {
		return host;
	}
	public void setUrl(String host) {
		this.host = host;
	}
	public String getMasterKey() {
		return masterKey;
	}
	public void setMasterKey(String masterKey) {
		this.masterKey = masterKey;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getCollectionName() {
		return collectionName;
	}
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	public String getPartitionId() {
		return this.partitionId;
	}
	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}
	
	public ItemDAL(String host, String masterKey, String databaseName, String collectionName) {
		this.setUrl(host);
		this.setMasterKey(masterKey);
		this.setDatabaseName(databaseName);
		this.setCollectionName(collectionName);
		try {
			createClient();
			createDatabaseIfNotExists();
			createContainerIfNotExists();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createClient() throws Exception {
		System.out.println("Using Azure Cosmos DB endpoint: " + this.getHost());

        ConnectionPolicy defaultPolicy = ConnectionPolicy.getDefaultPolicy();
        client = new CosmosClientBuilder()
            .setEndpoint(this.getHost())
            .setKey(this.getMasterKey())
            .setConnectionPolicy(defaultPolicy)
            .setConsistencyLevel(ConsistencyLevel.SESSION)
            .buildAsyncClient();
	}
	
	private void createDatabaseIfNotExists() throws Exception {
        System.out.println("Create database " + this.getDatabaseName() + " if not exists.");
        Mono<CosmosAsyncDatabaseResponse> databaseIfNotExists = 
        				client.createDatabaseIfNotExists(this.getDatabaseName());
        databaseIfNotExists.flatMap(databaseResponse -> {
            database = databaseResponse.getDatabase();
            System.out.println("Checking database " + database.getId() + " completed!\n");
            return Mono.empty();
        }).block();
    }
	
	private void createContainerIfNotExists() throws Exception {
        System.out.println("Create container " + this.getCollectionName() + " if not exists.");
        CosmosContainerProperties containerProperties = 
        					new CosmosContainerProperties(
        							this.getCollectionName(), "/" + this.getPartitionId());
        Mono<CosmosAsyncContainerResponse> containerIfNotExists = 
        					database.createContainerIfNotExists(containerProperties, 400);
        
        //  Create container with 400 RU/s
        containerIfNotExists.flatMap(containerResponse -> {
            container = containerResponse.getContainer();
            System.out.println("Checking container " + container.getId() + " completed!\n");
            return Mono.empty();
        }).block();
    }
	
	//Point read with id and partition key
	public Mono<CosmosAsyncItemResponse<StoreItem>> getStoreItem(String id, String partitionId) {
		final CountDownLatch completionLatch = new CountDownLatch(1);
        
		Mono<CosmosAsyncItemResponse<StoreItem>> asyncItemResponseMono = 
								container.readItem(id, new PartitionKey(partitionId), StoreItem.class);                        
        return asyncItemResponseMono;
	}
	public CosmosPagedFlux<StoreItem> queryItems(String query) {
        FeedOptions queryOptions = new FeedOptions();
        //queryOptions.setEnableCrossPartitionQuery(true); //No longer needed in SDK v4
        //  Set populate query metrics to get metrics around query executions
        queryOptions.setPopulateQueryMetrics(true);

        CosmosPagedFlux<StoreItem> pagedFluxResponse = container.queryItems(
            query, queryOptions, StoreItem.class);
        return pagedFluxResponse;
    }
	public Mono<CosmosAsyncItemResponse<StoreItem>> createUpdateItem(StoreItem storeItem) {
		return container.upsertItem(storeItem);
	}
	public Mono<CosmosAsyncItemResponse> deleteItem(String storeItemId, String partitionId) {
		return container.deleteItem(storeItemId, new PartitionKey(partitionId));
	}
}
