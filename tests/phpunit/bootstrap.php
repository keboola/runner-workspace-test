<?php

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

$environments = [
    'SNOWFLAKE_HOST',
    'SNOWFLAKE_PORT',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE',
    'SNOWFLAKE_SCHEMA',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
];

foreach ($environments as $environment) {
    if (empty(getenv($environment))) {
        throw new \Exception(sprintf('Missing environment "%s".', $environment));
    }
}
