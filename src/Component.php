<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest;

use Doctrine\DBAL\Connection as DBALConnection;
use Doctrine\DBAL\DriverManager;
use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

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
                $connection->query(
                    'CREATE TABLE ' . QueryBuilder::quoteIdentifier($authorization['schema']) . '.'
                    . QueryBuilder::quoteIdentifier($target) .
                    ' WITH (DISTRIBUTION = ROUND_ROBIN) ' .
                    ' AS (SELECT * FROM ' . QueryBuilder::quoteIdentifier($authorization['schema']) . '.'
                    . QueryBuilder::quoteIdentifier($source) . ')'
                );
                $connection->query(
                    'ALTER TABLE ' . QueryBuilder::quoteIdentifier($authorization['schema']) .
                    '.' . QueryBuilder::quoteIdentifier($target) .
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
