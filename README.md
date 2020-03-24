# runner-workspace-test

[![Build Status](https://travis-ci.com/keboola/runner-workspace-test.svg?branch=master)](https://travis-ci.com/keboola/runner-workspace-test)

Component for testing runner input & output mapping with workspaces.

# Usage

Create a configuration with workspace authorization, input mapping and operation (only allowed value is `copy`). 

```
{     
    "authorization": {
         "workspace": {
            "host": "xxxx.snowflakecomputing.com",
            "port": "443",
            "warehouse": "xxx",
            "database": "xxx",
            "schema": "xxx",
            "user": "xxx",
            "password": "xxx"
         }
    },
    "parameters": {
        "operation": "copy"
    },
    "storage": {
        "input": [
            {
                "source": "in.c-test.test",
                "destination": "my-table"
            }
        ],
        "output": [
            {
                "source": "my-table-copy",
                "destination": "out.c-test.test"
            }
        ]
    }
}
```

Run the component `php src\run.php`.

## Development

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/runner-workspace-test
cd my-component
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Set environment variables in `.env` file (see `.env.template`) containing Snowflake credentials. Standard 
workspace credentials will do. Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```

# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 
