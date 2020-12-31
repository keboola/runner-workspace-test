<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest\Tests;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use PHPUnit\Framework\TestCase;
use Keboola\RunnerWorkspaceTest\Component;
use Psr\Log\Test\TestLogger;

class ComponentAbsTest extends TestCase
{
    private array $workspace;
    private Client $client;

    public function setUp(): void
    {
        parent::setUp();
        $this->client = new Client([
            'url' => (string) getenv('ABS_STORAGE_API_URL'),
            'token' => (string) getenv('ABS_STORAGE_API_TOKEN'),
        ]);
        $workspaces = new Workspaces($this->client);
        $this->workspace = $workspaces->createWorkspace(['backend' => 'abs']);
    }

    public function tearDown(): void
    {
        $workspaces = new Workspaces($this->client);
        $workspaces->deleteWorkspace($this->workspace['id']);
        parent::tearDown();
    }

    public function testCopyAbs(): void
    {
        $authorization = $this->workspace['connection'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $content = '"first column","second column"' . "\n" . '"1","2"';
        $blobClient->createBlockBlob($authorization['container'], 'data/in/tables/my-table.csv', $content);
        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
            ],
            'parameters' => [
                'operation' => 'copy-abs',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-test.test',
                            'destination' => 'my-table.csv',
                        ],
                    ],
                ],
                'output' => [
                    'tables' => [
                        [
                            'source' => 'my-table-copy.csv',
                            'destination' => 'out.c-test.test',
                        ],
                    ],
                ],
            ],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $logger = new TestLogger();
        $component = new Component($logger);
        $component->execute();
        $blob = $blobClient->getBlob($authorization['container'], 'data/out/tables/my-table-copy.csv');
        $data = stream_get_contents($blob->getContentStream());
        self::assertEquals("\"first column\",\"second column\"\n\"1\",\"2\"", $data);
        $data = (string) file_get_contents($temp->getTmpFolder() . '/out/tables/my-table-copy.csv.manifest');
        self::assertEquals(
            [
                'columns' => [
                    'first column',
                    'second column',
                ],
            ],
            json_decode($data, true)
        );
    }

    public function testListAbs(): void
    {
        $authorization = $this->workspace['connection'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $blobClient->createAppendBlob($authorization['container'], 'data/out/tables/first');
        $blobClient->createAppendBlob($authorization['container'], 'data/out/files/second');
        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
            ],
            'parameters' => [
                'operation' => 'list-abs',
            ],
            'storage' => [],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $logger = new TestLogger();
        $component = new Component($logger);
        $component->execute();
        self::assertTrue($logger->hasInfoThatContains('data/out/tables/first'));
        self::assertTrue($logger->hasInfoThatContains('data/out/files/second'));
    }

    public function testDumpAbs(): void
    {
        $authorization = $this->workspace['connection'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $content = '"first column","second column"' . "\n" . '"1","2"';
        $blobClient->createBlockBlob($authorization['container'], 'data/in/tables/my-table.csv', $content);
        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
            ],
            'parameters' => [
                'operation' => 'dump-abs',
            ],
            'storage' => [],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $logger = new TestLogger();
        $component = new Component($logger);
        $component->execute();
        self::assertTrue($logger->hasInfoThatContains('data/in/tables/my-table.csv'));
        self::assertTrue($logger->hasInfoThatContains("Data: \"first column\",\"second column\"\n\"1\",\"2\""));
    }

    public function testCreateFileAbs(): void
    {
        $authorization = $this->workspace['connection'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $content = '"first column","second column"' . "\n" . '"1","2"';
        $blobClient->createBlockBlob($authorization['container'], 'data/in/tables/my-table.csv', $content);
        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
            ],
            'parameters' => [
                'operation' => 'create-abs-file',
            ],
            'storage' => [
                'output' => [
                    'files' => [
                        [
                            'source' => 'my-file',
                        ],
                    ],
                ],
            ],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $logger = new TestLogger();
        $component = new Component($logger);
        $component->execute();
        $blob = $blobClient->getBlob($authorization['container'], 'data/out/files/my-file');
        $data = stream_get_contents($blob->getContentStream());
        self::assertEquals('some-data', $data);
        $blob = $blobClient->getBlob($authorization['container'], 'data/out/files/my-file.manifest');
        $data = (string) stream_get_contents($blob->getContentStream());
        self::assertEquals(
            [
                'is_permanent' => true,
                'tags' => [
                    'foo',
                    'bar',
                ],
            ],
            json_decode($data, true)
        );
    }

    public function testCreateTableAbs(): void
    {
        $authorization = $this->workspace['connection'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $content = '"first column","second column"' . "\n" . '"1","2"';
        $blobClient->createBlockBlob($authorization['container'], 'data/in/tables/my-table.csv', $content);
        $config = [
            'authorization' => [
                'workspace' => $this->workspace['connection'],
            ],
            'parameters' => [
                'operation' => 'create-abs-table',
            ],
            'storage' => [
                'output' => [
                    'tables' => [
                        [
                            'source' => 'my-table.csv',
                            'destination' => 'out.c-test-bucket.test-table',
                        ],
                    ],
                ],
            ],
        ];
        $temp = new Temp();
        file_put_contents($temp->getTmpFolder() . '/config.json', json_encode($config));
        putenv('KBC_DATADIR=' . $temp->getTmpFolder());
        $logger = new TestLogger();
        $component = new Component($logger);
        $component->execute();
        $blob = $blobClient->getBlob($authorization['container'], 'data/out/tables/my-table.csv');
        $data = stream_get_contents($blob->getContentStream());
        self::assertEquals("first,second\n1a,2b", $data);
        $data = file_get_contents($temp->getTmpFolder() . '/out/tables/my-table.csv.manifest');
        self::assertEquals(
            [
                'primary_key' => ['first'],
            ],
            json_decode($data, true)
        );
    }
}
