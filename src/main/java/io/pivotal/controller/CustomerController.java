package io.pivotal.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.gemfire.client.ClientCacheFactoryBean;
import org.springframework.data.gemfire.client.ClientRegionFactoryBean;
import org.springframework.data.gemfire.listener.ContinuousQueryDefinition;
import org.springframework.data.gemfire.listener.ContinuousQueryListenerContainer;
import org.springframework.data.gemfire.listener.adapter.ContinuousQueryListenerAdapter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import io.pivotal.domain.Customer;
import io.pivotal.listener.CustomerCQListener;
import io.pivotal.service.CustomerSearchService;

@RestController
public class CustomerController {
	
	@Autowired
	io.pivotal.repo.pcc.CustomerRepository pccCustomerRepository;
	
	@Autowired
	io.pivotal.repo.jpa.CustomerRepository jpaCustomerRepository;
	
	@Autowired
	ClientRegionFactoryBean<String, Customer> customerRegionFactory;
	
	@Autowired
	CustomerSearchService customerSearchService;
	
	@Autowired
    private SimpMessagingTemplate webSocket;
	
	Fairy fairy = Fairy.create();
	
	ContinuousQueryListenerContainer cqContainer = null;
	

	@RequestMapping(method = RequestMethod.GET, path = "/showcache")
	@ResponseBody
	public String show() throws Exception {
		StringBuilder result = new StringBuilder();
		
		pccCustomerRepository.findAll().forEach(item->result.append(item+"<br/>"));

		return result.toString();
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/clearcache")
	@ResponseBody
	public String clearCache() throws Exception {
		Region<String, Customer> customerRegion = customerRegionFactory.getObject();
		customerRegion.removeAll(customerRegion.keySetOnServer());
		return "Region cleared";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/showdb")
	@ResponseBody
	public String showDB() throws Exception {
		StringBuilder result = new StringBuilder();
		Pageable topTen = new PageRequest(0, 10);
		
		jpaCustomerRepository.findAll(topTen).forEach(item->result.append(item+"<br/>"));
		
		return "Top 10 customers are shown here: <br/>" + result.toString();
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/loaddb")
	@ResponseBody
	public String loadDB(@RequestParam(value = "amount", required = true) String amount) throws Exception {
		
		List<Customer> customers = new ArrayList<>();
		
		Integer num = Integer.parseInt(amount);
		
		for (int i=0; i<num; i++) {
			Person person = fairy.person();
			Customer customer = new Customer(person.passportNumber(), person.fullName(), person.email(), person.getAddress().toString(), person.dateOfBirth().toString());
			customers.add(customer);
		}
		
		jpaCustomerRepository.save(customers);
		
		return "New customers successfully saved into Database";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/cleardb")
	@ResponseBody
	public String clearDB() throws Exception {
		
		jpaCustomerRepository.deleteAll();
		
		return "Database cleared";
	}
	
	@RequestMapping(value = "/customerSearch", method = RequestMethod.GET)
	public String searchCustomerByEmail(@RequestParam(value = "email", required = true) String email) {
		
		long startTime = System.currentTimeMillis();
		Customer customer = customerSearchService.getCustomerByEmail(email);
		long elapsedTime = System.currentTimeMillis();
		Boolean isCacheMiss = customerSearchService.isCacheMiss();
		String sourceFrom = isCacheMiss ? "MySQL" : "PCC";

		return String.format("Result [<b>%1$s</b>] <br/>"
				+ "Cache Miss [<b>%2$s</b>] <br/>"
				+ "Read from [<b>%3$s</b>] <br/>"
				+ "Elapsed Time [<b>%4$s ms</b>]%n", customer, isCacheMiss, sourceFrom, (elapsedTime - startTime));
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/countdb")
	@ResponseBody
	public Long countDB() throws Exception {
		return jpaCustomerRepository.count();
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/countcache")
	@ResponseBody
	public Long countCache() throws Exception {
		return pccCustomerRepository.count();
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/registercq")
	@ResponseBody
	public String registerCQ(@Autowired ClientCacheFactoryBean ccf, @Autowired CustomerCQListener cqListener) throws Exception {
		
		cqContainer = new ContinuousQueryListenerContainer();
		cqContainer.setCache(ccf.getObject());
		cqContainer.setQueryService(ccf.getObject().getQueryService());
		cqContainer.addListener(new ContinuousQueryDefinition("SELECT * FROM /customer", new ContinuousQueryListenerAdapter(cqListener)));
		
		cqContainer.start();
		
		webSocket.convertAndSend("/topic/status", cqStatus());
		
		return "CQ registeration success!";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/unregistercq")
	@ResponseBody
	public String unregisterCQ() throws Exception {
		
		cqContainer.stop();
		cqContainer = null;
		
		webSocket.convertAndSend("/topic/status", cqStatus());
		
		return "CQ unregisteration success!";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/cqstatus")
	@ResponseBody
	public String cqStatus() throws Exception {
		return cqContainer != null && cqContainer.isRunning() ? "START" : "STOP";
	}
	
	
	
}
