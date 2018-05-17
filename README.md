# ElasticSearchPOC
This is a POC for Elastic Search using AWS 

![Screenshot](Pers.png)


# API example

EndPoint: /v1?q=<search_term> <br />
Method: GET <br />
Headers: x-api-key: <api_key> <br />
Status Code: 200 - Ok, 403 - Forbidden if api key not given <br />
search_term = could be any string, no results returned if empty or not present <br />

