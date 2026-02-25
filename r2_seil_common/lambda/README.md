## Creating a new lambda

Add an entry to `/pipelines/lambda-build.yaml` under the lambda parameter.

`poetry new demo/hello --name app` 

Where:

- demo is the name of the integration
- hello is the name of the lambda
- app is assumed by the Docker build as the entrypoint

Lambda entrypoint is `handler(event, context)` in `app/__init__.py`

## Testing the ECR docker image

### Build
The example given below builds the docker image for the *forecast-gam-service*.

From the *lambda* folder:

``` bash
mkvenv
pip3 install poetry==1.2.1 invoke==1.7.1 toml==0.10.2
inv build --fn api/forecast-gam-service --dockertag dev-api-forecast-gam-service --reportsdir test-reports
```

### Run

Ensure you have authenticated using ``aws-azure-login`` (see [AWS CLI Access](https://contino.atlassian.net/wiki/spaces/CLIENTS/pages/3155001458/AWS+CLI+Access))

``docker run -v $HOME/.aws/credentials:/root/.aws/credentials -p 9000:8080 dev-api-forecast-gam-service``

### Test

``curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'``

The parameter `-d '{}'` equates to the event object passed to the handler.  An empty event object such as this should
elicit and "400 badrequest" response.

## Testing existing lambda

### Build
The example given below builds the docker image for the *gam-client-service*.

From the *lambda* folder:

remove previous builds:
``` bash
rm -rf api/gam-client-service/dist
```

``` bash
inv build --fn api/gam-client-service --dockertag gam-client-service --reportsdir test-reports
```

### Run

Ensure you have authenticated using ``aws-azure-login`` (see [AWS CLI Access](https://contino.atlassian.net/wiki/spaces/CLIENTS/pages/3155001458/AWS+CLI+Access))
Authentication command with profile:
``` bash
aws-azure-login --profile <profile-name>
```

Windows:
``` bash
docker run -v $HOME/.aws/credentials:/root/.aws/credentials:~/.aws/credentials -p 9000:8080 gam-client-service
```
MacOS:
``` bash
docker run -v ~/.aws/credentials -p 9000:8080 gam-client-service
```

### Test

``curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'``

The parameter `-d '{}'` equates to the event object passed to the handler.  An empty event object such as this should
elicit and "400 badrequest" response.