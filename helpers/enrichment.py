import os

import requests

apollo_api_key = os.environ["APOLLO_API_KEY"]
enrichment_url = "https://api.apollo.io/v1/people/match"


def enrich_user(first_name, last_name, email):
    enrichment_request_headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "X-Api-Key": apollo_api_key,
    }
    enrichment_input = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "reveal_personal_emails": True,
    }
    enrichment_response = requests.request(
        "POST",
        url=enrichment_url,
        headers=enrichment_request_headers,
        json=enrichment_input,
    ).json()

    return enrichment_response
