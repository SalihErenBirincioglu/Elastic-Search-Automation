package com.turkcell.elasticquery;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.domain.Range;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ElasticqueryApplication {

    public static void main(String[] args) throws IOException {
        
    	SpringApplication.run(ElasticqueryApplication.class, args);

        final String passw = ""; //Elastic şifresi 


        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", passw)); //elastic auth

        System.out.println("Creating credentials builder");
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("", 9200))   // Elastic ip port 
        		.setHttpClientConfigCallback(b->b.setDefaultCredentialsProvider(credentialsProvider)); 
                
        		
        		/*  ALTERNATİF 
        		
        		.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    
                	@Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
				*/
        Scanner sc= new Scanner(System.in);
        System.out.println("Creating client");
        RestHighLevelClient client = new RestHighLevelClient(builder);
        
        String stringStartDate;
        String stringEndDate;
        System.out.println("Enter start date in format yyyy-mm-dd");
        stringStartDate=sc.nextLine();
        System.out.println("Enter end date in format yyyy-mm-dd");
        stringEndDate=sc.nextLine();
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate startDate = LocalDate.parse(stringStartDate, formatter);
        LocalDate endDate = LocalDate.parse(stringEndDate, formatter);
        System.out.println(startDate); // 2010-01-02
        System.out.println(endDate); // 2010-01-02
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery())
        		.postFilter(QueryBuilders.matchQuery("tcmonit_retag_namespace_name", "ngne-test"))
        		.postFilter(QueryBuilders.matchQuery("message", "automation"))
                .postFilter(QueryBuilders.rangeQuery("date_time").gte(startDate).lte(endDate))
                .from(0)
                .size(100)
                .timeout(new TimeValue(3, TimeUnit.MINUTES))
                .sort(new FieldSortBuilder("TOKEN_ID").order(SortOrder.ASC))
                .sort(new ScoreSortBuilder().order(SortOrder.DESC));

       SearchRequest searchRequest = new SearchRequest()
    		   .source(sourceBuilder);
       
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);


        for (SearchHit hit : searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
            try {
                String filename = "C:\\Users\\Salih Eren\\Desktop\\responses.txt";
                FileWriter fw = new FileWriter(filename, true);
                fw.write(hit.getSourceAsString());
                fw.write("\n");
                fw.close();
            } catch (IOException ioe) {
                System.err.println("IOException: " + ioe.getMessage());
            }
        }

    }

}
