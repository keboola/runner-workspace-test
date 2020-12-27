<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest\Tests;

use Doctrine\DBAL\DriverManager;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Keboola\RunnerWorkspaceTest\Component;

class ComponentSynapseTest extends TestCase
{
    private array $workspace;
    private Client $client;

    public function setUp(): void
    {
        parent::setUp();
        $this->client = new Client([
            'url' => (string) getenv('SYNAPSE_STORAGE_API_URL'),
            'token' => (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),
        ]);
        $workspaces = new Workspaces($this->client);
        $this->workspace = $workspaces->createWorkspace(['backend' => 'synapse']);
    }

    public function tearDown(): void
    {
        $workspaces = new Workspaces($this->client);
        $workspaces->deleteWorkspace($this->workspace['id']);
        parent::tearDown();
    }

    public function testRun(): void
    {
        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlsrv',
            'host' => (string) $this->workspace['connection']['host'],
            'port' => 1433,
            'password' => (string) $this->workspace['connection']['password'],
            'user' => (string) $this->workspace['connection']['user'],
            'dbname' => (string) $this->workspace['connection']['database'],
        ]);
        $schema = $this->workspace['connection']['schema'];

        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
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
        $connection->executeQuery(
            'CREATE TABLE ' . QueryBuilder::quoteIdentifier($schema) . '."my-table"' .
            ' (COLUMN1 VARCHAR(100), COLUMN2 NUMBER(38,0))' .
            ' WITH (DISTRIBUTION = ROUND_ROBIN) '
        );
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $component = new Component(new NullLogger());
        $component->execute();
        self::assertCount(0, $connection->fetchAllAssociative('SELECT * FROM "my-table-copy"'));
    }
}
