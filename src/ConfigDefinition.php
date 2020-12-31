<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $operations = ['copy', 'copy-snowflake', 'copy-synapse', 'copy-abs', 'list-abs', 'dump-abs',
            'create-abs-file', 'create-abs-table'];
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('operation')
                    ->validate()
                        ->ifnotinarray($operations)
                        ->thenInvalid(sprintf('Allowed operations are: "%s".', implode($operations)))
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
