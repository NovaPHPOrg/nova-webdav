<?php

declare(strict_types=1);

namespace nova\plugin\webdav;

use nova\framework\core\Instance;
use nova\framework\http\Request;
use nova\framework\http\Response;

use function nova\framework\route;

use nova\framework\route\Route;
use nova\plugin\login\AdminPageInterface;
use nova\plugin\tpl\ViewResponse;

class WebdavTpl extends Instance implements AdminPageInterface
{
    public function registerRouter(string $model, string $controller): void
    {
        $default = route($model, $controller, 'init');
        Route::getInstance()
            ->get('/webdav/config', $default);
    }

    public function route(ViewResponse $view, Request $request): ?Response
    {
        if ($request->getPath() !== '/webdav/config') {
            return null;
        }

        return $view->asTpl(ROOT_PATH . DS . 'nova/plugin/webdav/tpl/config');
    }

    public function menu(): array
    {
        return [
            'title' => 'WebDAV 配置',
            'icon' => 'cloud_sync',
            'url' => '/webdav/config',
            'pjax' => true,
        ];
    }
}
