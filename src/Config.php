<?php

declare(strict_types=1);

namespace Keboola\RunnerWorkspaceTest;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    // @todo implement your custom getters
    public function getOperation(): string
    {
        return $this->getValue(['parameters', 'operation']);
    }
}
