import requests
import logging
from time import time

from auth_client.exceptions import AuthException

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

class ClientAuthToken:
    token: dict

    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.client_secret = client_secret
        self.client_id = client_id
        self.url = token_url
        self.token = {"expires_in": 0}

    def __call__(self):
        if time() > self.token["expires_in"]:
            self._refresh()
        return self.token["access_token"]

    def _refresh(self):
        # Tech1 API doesn't return a refresh token so re-auth with client_id/secret
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = requests.post(self.url, data=payload)
        if response.status_code != 200:
            logger.exception(f"Error in TechOne token generation - {str(response.status_code)}")
            raise AuthException(
                f"Received a {response.status_code} from token endpoint"
            )
        logger.info("TechOne token generation successful")
        self.token = response.json()
