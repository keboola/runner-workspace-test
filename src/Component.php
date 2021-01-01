<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\Csv\CsvReader;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\CopyBlobOptions;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

class Component extends BaseComponent
{
    private function getSnowflakeConnection(): Connection
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $authorization = $config->getAuthorization()['workspace'];
        unset($authorization['schema']);
        $connection = new Connection($authorization);
        $connection->query(
            'USE SCHEMA ' . QueryBuilder::quoteIdentifier($config->getAuthorization()['workspace']['schema'])
        );
        return $connection;
    }

    private function getSynapseConnection(): DBALConnection
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $authorization = $config->getAuthorization()['workspace'];
        $connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlsrv',
            'host' => $authorization['host'],
            'port' => 1433,
            'password' => $authorization['password'],
            'user' => $authorization['user'],
            'dbname' => $authorization['database'],
        ]);
        return $connection;
    }

    private function getAbsConnection(): BlobRestProxy
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $authorization = $config->getAuthorization()['workspace'];
        $blobClient = BlobRestProxy::createBlobService($authorization['connectionString']);
        $blobClient->pushMiddleware(RetryMiddlewareFactory::create());
        return $blobClient;
    }

    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $operation = $config->getOperation();
        switch ($operation) {
            case 'copy':
            case 'copy-snowflake':
                $connection = $this->getSnowflakeConnection();
                if (empty($config->getStorage()['output']['tables'][0]['source']) ||
                    empty($config->getStorage()['input']['tables'][0]['destination'])
                ) {
                    throw new UserException('One input and output mapping is required.');
                }
                $target = $config->getStorage()['output']['tables'][0]['source'];
                $source = $config->getStorage()['input']['tables'][0]['destination'];
                $connection->query(
                    'CREATE OR REPLACE TABLE ' . QueryBuilder::quoteIdentifier($target) .
                    ' AS SELECT * FROM ' . QueryBuilder::quoteIdentifier($source)
                );
                $connection->query(
                    'ALTER TABLE ' . QueryBuilder::quoteIdentifier($target) .
                    ' DROP COLUMN "_timestamp"'
                );
                $columns = $connection->fetchAll(
                    'SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = ' .
                    QueryBuilder::quote($target)
                );
                $columns = array_map(
                    function ($v) {
                        return $v['COLUMN_NAME'];
                    },
                    $columns
                );
                $manifestManager = new ManifestManager($this->getDataDir());
                $options = new OutTableManifestOptions();
                $options->setColumns($columns);
                $manifestManager->writeTableManifest($target  . '.manifest', $options);
                break;
            case 'copy-synapse':
                if (empty($config->getStorage()['output']['tables'][0]['source']) ||
                    empty($config->getStorage()['input']['tables'][0]['destination'])
                ) {
                    throw new UserException('One input and output mapping is required.');
                }
                $target = $config->getStorage()['output']['tables'][0]['source'];
                $source = $config->getStorage()['input']['tables'][0]['destination'];
                $connection = $this->getSynapseConnection();
                $config = $this->getConfig();
                $authorization = $config->getAuthorization()['workspace'];
                $connection->executeQuery(
                    'CREATE TABLE ' . QueryBuilder::quoteIdentifier($authorization['schema']) . '.'
                    . QueryBuilder::quoteIdentifier($target) .
                    ' WITH (DISTRIBUTION = ROUND_ROBIN) ' .
                    ' AS (SELECT * FROM ' . QueryBuilder::quoteIdentifier($authorization['schema']) . '.'
                    . QueryBuilder::quoteIdentifier($source) . ')'
                );
                $columns = $connection->fetchAllAssociative(
                    'SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = ' .
                    QueryBuilder::quote($target)
                );
                $columns = array_map(
                    function ($v) {
                        return $v['COLUMN_NAME'];
                    },
                    $columns
                );
                $manifestManager = new ManifestManager($this->getDataDir());
                $options = new OutTableManifestOptions();
                $options->setColumns($columns);
                $manifestManager->writeTableManifest($target  . '.manifest', $options);
                break;
            case 'copy-abs':
                if (empty($config->getStorage()['output']['tables'][0]['source']) ||
                    empty($config->getStorage()['input']['tables'][0]['destination'])
                ) {
                    throw new UserException('One input and output mapping is required.');
                }
                $target = 'data/out/tables/' . $config->getStorage()['output']['tables'][0]['source'];
                $source = 'data/in/tables/' . $config->getStorage()['input']['tables'][0]['destination'];
                $authorization = $config->getAuthorization()['workspace'];
                $blobClient = $this->getAbsConnection();
                $blob = $blobClient->getBlob($authorization['container'], $source);
                $blobClient->createBlockBlob($authorization['container'], $target, $blob->getContentStream());
                $blob = $blobClient->getBlob($authorization['container'], $source);
                $data = stream_get_contents($blob->getContentStream());
                file_put_contents($this->getDataDir() . '/tmp.csv', $data);
                $csvReader = new CsvFile($this->getDataDir() . '/tmp.csv');
                $columns = $csvReader->getHeader();
                $manifestManager = new ManifestManager($this->getDataDir());
                $options = new OutTableManifestOptions();
                $options->setColumns($columns);
                $manifestManager->writeTableManifest(
                    $config->getStorage()['output']['tables'][0]['source']  . '.manifest',
                    $options
                );
                break;
            case 'list-abs':
                $authorization = $config->getAuthorization()['workspace'];
                $blobClient = $this->getAbsConnection();
                $blobList = $blobClient->listBlobs($authorization['container']);
                foreach ($blobList->getBlobs() as $blob) {
                    $this->getLogger()->info($blob->getName());
                }
                break;
            case 'dump-abs':
                $authorization = $config->getAuthorization()['workspace'];
                $blobClient = $this->getAbsConnection();
                $blobList = $blobClient->listBlobs($authorization['container']);
                foreach ($blobList->getBlobs() as $blob) {
                    $this->getLogger()->info($blob->getName());
                    $blobData = $blobClient->getBlob($authorization['container'], $blob->getName());
                    $data = stream_get_contents($blobData->getContentStream());
                    $this->getLogger()->info('Data: ' . $data);
                }
                break;
            case 'create-abs-file':
                $fileName = $config->getStorage()['output']['files'][0]['source'] ?? 'my-file.dat';
                $authorization = $config->getAuthorization()['workspace'];
                $blobClient = $this->getAbsConnection();
                $blobClient->createBlockBlob($authorization['container'], 'data/out/files/' . $fileName, 'some-data');
                $manifestData = [
                    'is_permanent' => true,
                    'tags' => ['foo', 'bar'],
                ];
                $blobClient->createBlockBlob(
                    $authorization['container'],
                    'data/out/files/' . $fileName . '.manifest',
                    (string) json_encode($manifestData)
                );
                break;
            case 'create-abs-table':
                $fileName = $config->getStorage()['output']['tables'][0]['source'] ?? 'my-file.csv';
                $authorization = $config->getAuthorization()['workspace'];
                $blobClient = $this->getAbsConnection();
                $blobClient->createBlockBlob(
                    $authorization['container'],
                    'data/out/tables/' . $fileName,
                    "first,second\n1a,2b"
                );
                $options = new OutTableManifestOptions();
                $options->setPrimaryKeyColumns(['first']);
                $options->setColumns(['first', 'second']);
                $manifestManager = new ManifestManager($this->getDataDir());
                $manifestManager->writeTableManifest(
                    $config->getStorage()['output']['tables'][0]['source']  . '.manifest',
                    $options
                );
                break;
            default:
                throw new UserException('Invalid operation');
        }
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
