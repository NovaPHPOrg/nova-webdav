<?php

declare(strict_types=1);

namespace nova\plugin\webdav;

use nova\framework\core\StaticRegister;
use nova\framework\route\RouteTrait;
use nova\plugin\login\AdminPage;
use nova\plugin\login\route\Permission;

class WebdavManager extends StaticRegister
{
    use RouteTrait;

    public function __construct()
    {
        $this->controllerNamespace = 'nova\\plugin\\webdav\\controller\\';
        $this->registerRoutes();
    }

    private function registerRoutes(): void
    {
        $this->getOrPost('/webdav/api/config', $this->map('config', 'config'));
    }

    public static function registerInfo(): void
    {
        Permission::getInstance()->registerPermissions('WebDAV 配置', 'webdav_manage', [
            'GET /webdav/config',
            'ANY /webdav/api*',
        ]);

        self::getInstance()->bindPrefixDispatch('/webdav');
        AdminPage::bind(WebdavTpl::getInstance());
    }
}
