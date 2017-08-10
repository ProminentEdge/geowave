package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.ServletContext;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Server;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.engine.Engine;
import org.restlet.engine.security.AuthenticatorHelper;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.security.ChallengeAuthenticator;
import org.restlet.security.MapVerifier;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.restlet.ext.apispark.internal.conversion.swagger.v1_2.model.ApiDeclaration;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.ext.swagger.SwaggerApplication;
import org.restlet.ext.oauth.*;
import org.restlet.ext.swagger.SwaggerSpecificationRestlet;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;

public class RestServer extends
		ServerResource
{
	private final ArrayList<RestRoute> availableRoutes;
	private final ArrayList<String> unavailableCommands;

	public static ChallengeScheme MySCHEME = new ChallengeScheme(
			"This is my own challenge scheme",
			"MySCHEME");

	/**
	 * Run the Restlet server (localhost:5152)
	 */
	public static void main(
			final String[] args ) {
		//Router r = new Router();
		final RestServer server = new RestServer();
		//server.run(5152);
		
		//ServletContext servCont = (ServletContext)server.getContext().getServerDispatcher().getContext().getAttributes().get("org.restlet.ext.servlet.ServletContext");
		ApplicationContext springContext = new ClassPathXmlApplicationContext(
				new String[] {
					"ApplicationContext-Server.xml"
				});

		// obtain the Restlet component from the Spring context and start it
		try {
			((Component) springContext.getBean("top")).start();
			//((Component) springContext.getBean("springSecurityFilterChain")).start();
		
			DelegatingFilterProxy p = new DelegatingFilterProxy((Filter) springContext.getBean("springSecurityFilterChain"));
			//p.setTargetBeanName("springSecurityFilterChain");
		}
		catch (BeansException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public RestServer() {
		
		availableRoutes = new ArrayList<RestRoute>();
		unavailableCommands = new ArrayList<String>();

		for (final Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(GeowaveOperation.class)) {
			if ((operation.getAnnotation(
					GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.GET)
					|| (((operation.getAnnotation(
							GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST)) && DefaultOperation.class
							.isAssignableFrom(operation)) || ServerResource.class.isAssignableFrom(operation)) {

				availableRoutes.add(new RestRoute(
						operation));
			}
			else {
				final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
				unavailableCommands.add(operation.getName() + " " + operationInfo.name());
			}
		}

		Collections.sort(availableRoutes);
	}

	// Show a simple 404 if the route is unknown to the server
	@Get("html")
	public String listResources() {
		final StringBuilder routeStringBuilder = new StringBuilder(
				"Available Routes:<br>");

		for (final RestRoute route : availableRoutes) {
			routeStringBuilder.append(route.getPath() + " --> " + route.getOperation() + "<br>");
		}
		routeStringBuilder.append("<br><br><span style='color:blue'>Unavailable Routes:</span><br>");
		for (final String command : unavailableCommands) {
			routeStringBuilder.append("<span style='color:blue'>" + command + "</span><br>");
		}
		return "<b>404</b>: Route not found<br><br>" + routeStringBuilder.toString();
	}

	public void run(
			final int port ) {

		// Add paths for each command
		final Router router = new Router();

		SwaggerApiParser apiParser = new SwaggerApiParser(
				"1.0.0",
				"GeoWave API",
				"REST API for GeoWave CLI commands");
		for (final RestRoute route : availableRoutes) {

			if (DefaultOperation.class.isAssignableFrom(route.getOperation())) {
				router.attach(
						route.getPath(),
						new GeoWaveOperationFinder(
								(Class<? extends DefaultOperation<?>>) route.getOperation()));

				apiParser.AddRoute(route);
			}
			else {
				router.attach(
						route.getPath(),
						(Class<? extends ServerResource>) route.getOperation());
			}
		}

		apiParser.SerializeSwaggerJson("swagger.json");

		List<AuthenticatorHelper> l = Engine.getInstance().getRegisteredAuthenticators();

		// Guard the restlet with BASIC authentication.
		ChallengeAuthenticator guard = new ChallengeAuthenticator(
				null,
				ChallengeScheme.HTTP_OAUTH,
				"testRealm");
		// Instantiates a Verifier of identifier/secret couples based on a
		// simple Map.
		MapVerifier mapVerifier = new MapVerifier();
		// Load a single static login/secret pair.
		mapVerifier.getLocalSecrets().put(
				"login",
				"secret".toCharArray());
		guard.setVerifier(mapVerifier);

		guard.setNext(RestServer.class);

		// Provide basic 404 error page for unknown route
		router.attachDefault(RestServer.class);

		// Setup router
		final Application myApp = new SwaggerApplication() {

			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());
			
				attachSwaggerSpecificationRestlet(
						router,
						"swagger.json");

				OAuthProxy googleProxy = new OAuthProxy(
						getContext(),
						false);
				googleProxy.setClientId("88903973904-5akeigppadcr5ge2sb8vq3811oshrd6h.apps.googleusercontent.com");
				googleProxy.setClientSecret("J2brEzLbp8GGlVLGTZPGwiyG");
				googleProxy.setRedirectURI("http://localhost:5152/google");
				googleProxy.setAuthorizationURI("https://accounts.google.com/o/oauth2/auth");
				googleProxy.setTokenURI("https://accounts.google.com/o/oauth2/token");
				googleProxy.setScope(new String[] {
					"https://www.google.com/m8/feeds/"
				});
				googleProxy.setNext(GoogleContactsServerResource.class);

				router.attach(
						"google",
						googleProxy);
				// router.attachDefault(RestServer.class);
				// router.attach("contacts",
				// GoogleContactsServerResource.class);

				return router;
			};

			@Override
			public String getName() {
				return "GeoWave API";
			}

			@Override
			public SwaggerSpecificationRestlet getSwaggerSpecificationRestlet(
					Context context ) {
				return new SwaggerSpecificationRestlet(
						getContext()) {
					@Override
					public Representation getApiDeclaration(
							String category ) {
						JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
								new FileRepresentation(
										"./swagger.json/" + category,
										MediaType.APPLICATION_JSON),
								ApiDeclaration.class);
						return result;
					}

					@Override
					public Representation getResourceListing() {
						JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
								new FileRepresentation(
										"./swagger.json",
										MediaType.APPLICATION_JSON),
								ApiDeclaration.class);
						return result;
					}
				};
			}

		};
		final Component component = new Component();
		component.getClients().add(
				Protocol.HTTP);
		component.getClients().add(
				Protocol.HTTPS);
		component.getDefaultHost().attach(
				"/",
				myApp);

		// Start server
		try {
			new Server(
					Protocol.HTTP,
					port,
					component).start();
		}
		catch (final Exception e) {
			e.printStackTrace();
			System.out.println("Could not create Restlet server - is the port already bound?");
		}
	}

	/**
	 * A simple ServerResource to show if the route's operation does not extend
	 * ServerResource
	 */
	public static class NonResourceCommand extends
			ServerResource
	{
		@Override
		@Get("html")
		public String toString() {
			return "The route exists, but the command does not extend ServerResource";
		}
	}
}
