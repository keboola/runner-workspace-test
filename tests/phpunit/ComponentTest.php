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
            'host' => (string) getenv('SNOWFLAKE_HOST'),
            'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
            'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            'schema' => (string) getenv('SNOWFLAKE_SCHEMA'),
            'user' => (string) getenv('SNOWFLAKE_USER'),
            'password' => (string) getenv('SNOWFLAKE_PASSWORD'),
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
                    'tables' => [
                         [
                             'source' => 'in.c-test.test',
                             'destination' => 'my-table',
                         ],
                    ],
                ],
                'output' => [
                    'tables' => [
                         [
                             'source' => 'my-table-copy',
                             'destination' => 'out.c-test.test',
                         ],
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
            'CREATE OR REPLACE TRANSIENT TABLE "my-table" ' .
            '(COLUMN1 VARCHAR(100), COLUMN2 NUMBER(38,0), "_timestamp" DATETIME);'
        );
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $component = new Component(new NullLogger());
        $component->execute();
        $connection = new Connection($dbOptions);
        $connection->query('USE SCHEMA ' . QueryBuilder::quoteIdentifier($schema));
        self::assertCount(0, $connection->fetchAll('SELECT * FROM "my-table-copy"'));
    }
}
