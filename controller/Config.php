<?php

declare(strict_types=1);

namespace nova\plugin\webdav\controller;

use function nova\framework\config;

use nova\framework\http\Response;
use nova\plugin\login\controller\BaseAPIController;
use nova\plugin\webdav\SimpleWebDAVClient;
use RuntimeException;
use Throwable;

class Config extends BaseAPIController
{
    public function config(): Response
    {
        if ($this->request->isGet()) {
            $deviceId = config('webdav.deviceId');
            if (empty($deviceId)) {
                config('webdav.deviceId', (string)(time() * 1000));
            }

            return Response::asJson([
                'code' => 200,
                'data' => config('webdav'),
            ]);
        }

        config('webdav.url', $this->request->post('url'));
        config('webdav.username', $this->request->post('username'));
        config('webdav.password', $this->request->post('password'));

        return Response::asJson([
            'code' => 200,
            'msg' => '操作成功',
        ]);
    }

    /**
     * 用当前表单值探测 WebDAV 是否可联通（不必先保存）
     */
    public function test(): Response
    {
        $url = trim((string)$this->request->post('url', config('webdav.url', '')));
        $username = (string)$this->request->post('username', config('webdav.username', ''));
        $password = (string)$this->request->post('password', config('webdav.password', ''));

        if ($url === '') {
            return Response::asJson(['code' => 400, 'msg' => '请填写主机域']);
        }

        try {
            $client = new SimpleWebDAVClient($url, $username !== '' ? $username : null, $password);
            // Depth:0 PROPFIND 根路径，只验证认证与可达性
            $info = $client->getResourceInfo('/');
            $name = is_array($info) ? ($info['name'] ?: '/') : '/';

            return Response::asJson([
                'code' => 200,
                'msg' => '连接成功：' . $name,
            ]);
        } catch (RuntimeException $e) {
            return Response::asJson([
                'code' => 400,
                'msg' => '连接失败：' . $e->getMessage(),
            ]);
        } catch (Throwable $e) {
            return Response::asJson([
                'code' => 400,
                'msg' => '连接失败：' . $e->getMessage(),
            ]);
        }
    }
}
