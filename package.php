<?php

declare(strict_types=1);

return [
    'config' => [
        'framework_start' => [
            'nova\\plugin\\webdav\\WebdavManager',
        ],
    ],
    'require' => [
        'tpl', 'login',
    ],
];
