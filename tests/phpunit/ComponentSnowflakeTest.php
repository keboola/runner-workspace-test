<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest\Tests;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Keboola\RunnerWorkspaceTest\Component;

class ComponentSnowflakeTest extends TestCase
{
    private array $workspace;
    private Client $client;

    public function setUp(): void
    {
        parent::setUp();
        $this->client = new Client([
            'url' => (string) getenv('SNOWFLAKE_STORAGE_API_URL'),
            'token' => (string) getenv('SNOWFLAKE_STORAGE_API_TOKEN'),
        ]);
        $workspaces = new Workspaces($this->client);
        $this->workspace = $workspaces->createWorkspace(['backend' => 'snowflake']);
    }

    public function tearDown(): void
    {
        $workspaces = new Workspaces($this->client);
        $workspaces->deleteWorkspace($this->workspace['id']);
        parent::tearDown();
    }

    public function testRun(): void
    {
        $dbOptions = [
            'host' => (string) $this->workspace['connection']['host'],
            'warehouse' => (string) $this->workspace['connection']['warehouse'],
            'database' => (string) $this->workspace['connection']['database'],
            'schema' => (string) $this->workspace['connection']['schema'],
            'user' => (string) $this->workspace['connection']['user'],
            'password' => (string) $this->workspace['connection']['password'],
        ];

        $config = [
            'authorization' => [
                'workspace' => $dbOptions,
            ],
            'parameters' => [
                'operation' => 'copy-synapse',
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
