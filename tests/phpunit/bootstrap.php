<?php

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

$environments = [
    'ABS_STORAGE_API_URL',
    'ABS_STORAGE_API_TOKEN',
    'SNOWFLAKE_STORAGE_API_URL',
    'SNOWFLAKE_STORAGE_API_TOKEN',
    'SYNAPSE_STORAGE_API_URL',
    'SYNAPSE_STORAGE_API_TOKEN',
];

foreach ($environments as $environment) {
    if (empty(getenv($environment))) {
        throw new \Exception(sprintf('Missing environment "%s".', $environment));
    }
}
