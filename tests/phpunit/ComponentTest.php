<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Keboola\RunnerWorkspaceTest\Component;

class ComponentTest extends TestCase
{
    public function testRun(): void
    {
        $dbOptions = [
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
            'database' => getenv('SNOWFLAKE_DATABASE'),
            'schema' => getenv('SNOWFLAKE_SCHEMA'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ];

        $config = [
            'authorization' => [
                'workspace' => $dbOptions,
            ],
            'parameters' => [
                'operation' => 'copy',
            ],
            'storage' => [
                'input' => [
                     [
                         'source' => 'in.c-test.test',
                         'destination' => 'my-table',
                     ],
                ],
                'output' => [
                     [
                         'source' => 'my-table-copy',
                         'destination' => 'out.c-test.test',
                     ],
                ],
            ],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        $schema = $dbOptions['schema'];
        unset($dbOptions['schema']);
        $connection = new Connection($dbOptions);
        $connection->query('USE SCHEMA ' . QueryBuilder::quoteIdentifier($schema));
        $connection->query('DROP TABLE IF EXISTS "my-table-copy"');
        $connection->query(
            'CREATE OR REPLACE TRANSIENT TABLE "my-table" (COLUMN1 VARCHAR(100), COLUMN2 NUMBER(38,0));'
        );
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $component = new Component(new NullLogger());
        $component->execute();
        $connection = new Connection($dbOptions);
        $connection->query('USE SCHEMA ' . QueryBuilder::quoteIdentifier($schema));
        self::assertCount(0, $connection->fetchAll('SELECT * FROM "my-table-copy"'));
    }
}
