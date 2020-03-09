#!/bin/bash
set -e

# Log in to the repostiory
eval $(docker run --rm -e AWS_ACCESS_KEY_ID=$DEPLOY_AWS_ACCESS_KEY -e AWS_SECRET_ACCESS_KEY=$DEPLOY_AWS_ACCESS_KEY_SECRET keboola/aws-cli:latest ecr get-login --region us-east-1 --no-include-email)

# Push to the repository
docker tag ${APP_IMAGE}:latest ${REPOSITORY}:${TRAVIS_TAG}
docker tag ${APP_IMAGE}:latest ${REPOSITORY}:latest
docker push ${REPOSITORY}:${TRAVIS_TAG}
docker push ${REPOSITORY}:latest

# Update the tag in Keboola Developer Portal -> Deploy to KBC
if echo ${TRAVIS_TAG} | grep -c '^v\?[0-9]\+\.[0-9]\+\.[0-9]\+$'
then
    docker run --rm \
        -e KBC_DEVELOPERPORTAL_USERNAME \
        -e KBC_DEVELOPERPORTAL_PASSWORD \
        quay.io/keboola/developer-portal-cli-v2:latest \
        update-app-repository ${KBC_DEVELOPERPORTAL_VENDOR} ${KBC_DEVELOPERPORTAL_APP} ${TRAVIS_TAG} ecr ${REPOSITORY}
else
    echo "Skipping deployment to KBC, tag ${TRAVIS_TAG} is not allowed."
fi
