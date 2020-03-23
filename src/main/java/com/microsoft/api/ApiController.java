package com.microsoft.api;

import java.util.Optional;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.azure.cosmos.CosmosPagedFlux;
import com.azure.cosmos.models.CosmosAsyncItemResponse;
import com.microsoft.api.dal.ItemDAL;
import com.microsoft.api.model.StoreItem;

import reactor.core.publisher.Mono;

@RestController
public class ApiController {
	private static String HOST = "https://rdsqlsink.documents.azure.com:443/";
	private static String MASTERKEY = "xnPzWC3Db0vo0Pme7oWlIzUIdCa8fdHq4WHdgtBbhP4JBUHEfFd3LduDF201xQbO4rj18LZai70c53ptondpRg==";
	private static String DATABASENAME = "testdb";
	private static String CONTAINERNAME = "gsfnew";
	
	
	private ItemDAL itemDAL = new ItemDAL(HOST, MASTERKEY, DATABASENAME, CONTAINERNAME);
	
	//point query
	@GetMapping("/api/v1/items/{id}/{type}")
	public Mono<CosmosAsyncItemResponse<StoreItem>> getStoreItem
												(@PathVariable(value = "id") String id,
												@PathVariable(value = "type") Optional<String> type) {
		String defaultType = "metadata";
		if(type.isPresent()) {
			defaultType = type.get();
		}
		String pId = id + "_" + defaultType;
		return itemDAL.getStoreItem(id, pId);
	}
	@GetMapping("/api/v1/items")
	public CosmosPagedFlux<StoreItem> getStoreItemBy(
												@RequestParam(value = "type") Optional<String> type,
												@RequestParam(value = "city") Optional<String> city) {
		
		String defaultType = "metadata";
		String defaultCity = "Austin";
		
		if(type.isPresent()) {
			defaultType = type.get();
		}
		if(city.isPresent()) {
			defaultCity = city.get();
		}
		
		String query = "SELECT * FROM " + CONTAINERNAME;
		query += " WHERE " + CONTAINERNAME + ".type='" + defaultType +"'";
		query += " AND " + CONTAINERNAME + ".city='" + defaultCity + "'";
		System.out.println(query);
		return itemDAL.queryItems(query);
	}
	
	@PutMapping("/api/v1/items/")
	public Mono<CosmosAsyncItemResponse<StoreItem>> createStoreItem(@RequestBody StoreItem storeItem) {
		return itemDAL.createUpdateItem(storeItem);
	}
	@DeleteMapping("/api/v1/items/{id}/{pk}")
	public Mono<CosmosAsyncItemResponse> deleteItem(
					@PathVariable(value = "id") String id,
					@PathVariable(value = "pk") String pk) {
		return itemDAL.deleteItem(id, pk);
	}
}
