<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest;

use Keboola\Component\BaseComponent;
use Keboola\Component\Manifest\ManifestManager;
use Keboola\Component\Manifest\ManifestManager\Options\OutTableManifestOptions;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Component extends BaseComponent
{
    protected function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $authorization = $config->getAuthorization()['workspace'];
        unset($authorization['schema']);
        $connection = new Connection($authorization);
        $connection->query(
            'USE SCHEMA ' . QueryBuilder::quoteIdentifier($config->getAuthorization()['workspace']['schema'])
        );
        $operation = $config->getOperation();
        switch ($operation) {
            case 'copy':
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
                $columns = $connection->fetchAll(
                    'SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = ' .
                    QueryBuilder::quote($source)
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
