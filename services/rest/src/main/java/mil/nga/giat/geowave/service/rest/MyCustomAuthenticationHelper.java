package mil.nga.giat.geowave.service.rest;

import java.io.IOException;

import org.restlet.Response;
import org.restlet.data.ChallengeRequest;
import org.restlet.data.ChallengeScheme;
import org.restlet.engine.header.ChallengeWriter;
import org.restlet.engine.security.AuthenticatorHelper;
import org.restlet.util.Series;

public class MyCustomAuthenticationHelper extends
		AuthenticatorHelper
{
	public MyCustomAuthenticationHelper() {
		super(
				ChallengeScheme.CUSTOM,
				false,
				true);
	}

	public void formatRawRequest(
			ChallengeWriter cw,
			ChallengeRequest challenge,
			Response response,
			Series httpHeaders )
			throws IOException {
		if (challenge.getRealm() != null) {
			cw.appendQuotedChallengeParameter(
					"realm",
					challenge.getRealm());
		}
	}
}
