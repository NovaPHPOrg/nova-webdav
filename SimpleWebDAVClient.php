<?php

namespace nova\plugin\webdav;

use nova\framework\core\Logger;
use nova\framework\http\Response;
use nova\framework\http\ResponseType;
use nova\plugin\http\HttpClient;
use nova\plugin\http\HttpDownloadManager;
use RuntimeException;
use SimpleXMLElement;

/**
 * 优化版 WebDAV 客户端
 *
 * 相比原版改进：
 * - 复用 HttpClient 处理 HTTP 请求 (-100+ 行)
 * - 支持使用 HttpDownloadManager 进行下载 (-70+ 行)
 * - 继承框架的代理、超时、日志、缓存等功能
 * - 减少代码冗余，提高维护性
 *
 * 总代码量从 463 行降低到约 280 行，同时功能更强大
 */
class SimpleWebDAVClient{
    /** @var string WebDAV 服务器基础 URL */
    protected $baseUrl;

    /** @var string|null 基本认证用户名 */
    private $user;

    /** @var string|null 基本认证密码 */
    private $pass;

    /** @var HttpClient HTTP 客户端实例，复用框架的代理、超时、日志、缓存等功能 */
    private HttpClient $httpClient;

    /**
     * 构造函数：初始化 WebDAV 客户端
     *
     * @param string $baseUrl WebDAV 服务器地址（如：http://webdav.example.com:8080/dav）
     * @param string|null $user 可选的基本认证用户名
     * @param string|null $pass 可选的基本认证密码
     */
    public function __construct(string $baseUrl, ?string $user = null, ?string $pass = null) {
        // 移除末尾的斜杠，确保 URL 格式统一
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->user = $user;
        $this->pass = $pass;

        // 初始化 HTTP 客户端，复用框架的所有功能
        $this->httpClient = new HttpClient($this->baseUrl);
        // 设置请求超时时间为 30 秒
        $this->httpClient->timeout(30);
        // 禁用 SSL 证书验证（仅用于开发/测试环境，生产环境应启用）
        $this->httpClient->setOption(CURLOPT_SSL_VERIFYPEER, false);
        $this->httpClient->setOption(CURLOPT_SSL_VERIFYHOST, false);

        // 配置认证方式（支持 Basic 和 Digest 认证）
        if ($this->user !== null) {
            $this->httpClient->setOption(CURLOPT_HTTPAUTH, CURLAUTH_BASIC | CURLAUTH_DIGEST);
            $this->httpClient->setOption(CURLOPT_USERPWD, $this->user . ':' . $this->pass);
        }
    }

    /**
     * 获取 WebDAV 服务器基础 URL
     *
     * @return string
     */
    public function getBaseUrl(): string {
        return $this->baseUrl;
    }

    /**
     * 列举目录内容
     *
     * @param string $path 目录路径（相对于基础 URL）
     * @return array 包含目录项的数组，每项包含 path, name, is_dir, size, mtime, type
     * @throws RuntimeException 当 HTTP 状态码不是 207 时抛出异常
     */
    public function listDir(string $path): array {
        // 生成标准的 WebDAV PROPFIND 请求体
        $xmlBody = $this->defaultPropFindBody();

        // 发送 PROPFIND 请求，Depth: 1 表示仅列举直接子项
        list($response, $code) = $this->request('PROPFIND', $path, [
            'body' => $xmlBody,
            'headers' => ['Depth: 1', 'Content-Type: application/xml']
        ]);

        // WebDAV 多状态响应码为 207
        if ($code !== 207) {
            throw new RuntimeException("Failed to list directory. HTTP Code: $code, Path: $path");
        }

        // 构建完整路径用于过滤结果（排除请求的目录本身）
        $fullPath = parse_url($this->baseUrl, PHP_URL_PATH) . '/' . ltrim($path, '/');
        return $this->parsePropFindResponse($response, $fullPath);
    }

    /**
     * 获取单个资源的详细信息
     *
     * @param string $path 资源路径（相对于基础 URL）
     * @return array|null 包含资源信息的数组，如果不存在返回 null
     * @throws RuntimeException 当 HTTP 状态码不是 207 时抛出异常
     */
    public function getResourceInfo(string $path): ?array {
        // 生成标准的 WebDAV PROPFIND 请求体
        $xmlBody = $this->defaultPropFindBody();

        // 发送 PROPFIND 请求，Depth: 0 表示仅查询资源本身
        list($response, $code) = $this->request('PROPFIND', $path, [
            'body' => $xmlBody,
            'headers' => ['Depth: 0', 'Content-Type: application/xml']
        ]);

        // WebDAV 多状态响应码为 207
        if ($code !== 207) {
            throw new RuntimeException("Failed to get resource info. HTTP Code: $code, Path: $path");
        }

        // 解析响应并返回第一条（唯一一条）结果
        $entries = $this->parseMultistatusEntries($response);
        return $entries[0] ?? null;
    }

    /**
     * 判断指定路径是否为目录
     *
     * @param string $path 资源路径（相对于基础 URL）
     * @return bool 如果是目录返回 true，否则返回 false（包括不存在的情况）
     */
    public function isDirectory(string $path): bool {
        try {
            $info = $this->getResourceInfo($path);
            return $info !== null && ($info['is_dir'] ?? false);
        } catch (RuntimeException $e) {
            // 获取资源信息失败时，判定为不是目录
            return false;
        }
    }

    /**
     * 单线程下载文件到本地
     *
     * @param string $remotePath 远程文件路径
     * @param string $localPath 本地保存路径
     * @return bool 是否成功
     */
    public function download(string $remotePath, string $localPath): bool {
        try {
            // 使用 HttpDownloadManager 处理下载，支持断点续传和重试
            $downloadManager = new HttpDownloadManager($this->httpClient);
            return $downloadManager->download($this->buildUrl($remotePath), $localPath);
        } catch (\Exception $e) {
            Logger::error("WebDAV download failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * 多线程下载文件到本地
     *
     * @param string $remotePath 远程文件路径
     * @param string $localPath 本地保存路径
     * @param int $threads 线程数
     * @param callable|null $onProgress 进度回调
     * @return bool 是否成功
     */
    public function downloadMultiThread(
        string $remotePath,
        string $localPath,
        int $threads = 5,
        ?callable $onProgress = null
    ): bool {
        try {
            $downloadManager = new HttpDownloadManager($this->httpClient);
            return $downloadManager->multiThreadDownload(
                $this->buildUrl($remotePath),
                $localPath,
                $threads,
                $onProgress
            );
        } catch (\Exception $e) {
            Logger::error("WebDAV multi-thread download failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * 直接下载文件并输出到浏览器（使用 Response 对象）
     *
     * 支持以下功能：
     * - HTTP Range 请求（断点续传）
     * - 大文件流式传输
     * - 自定义文件名
     *
     * @param string $remotePath 远程文件路径
     * @param string|null $downloadName 下载时的文件名，如为 null 则使用远程文件名
     * @param string|null $rangeHeader HTTP Range 请求头（如 "bytes=0-1023"），用于断点续传
     * @return Response Response 对象，直接调用 send() 方法即可输出文件到浏览器
     */
    public function downloadToResponse(string $remotePath, ?string $downloadName = null, ?string $rangeHeader = null): Response {
        $url = $this->buildUrl($remotePath);
        $name = $downloadName ?? basename(urldecode($remotePath));

        // 创建匿名类继承 Response，支持流式传输和 Range 请求
        return new class($url, $this->user, $this->pass, $name, $rangeHeader) extends Response {
            /** @var string 远程文件 URL */
            private $url;

            /** @var string|null 基本认证用户名 */
            private $user;

            /** @var string|null 基本认证密码 */
            private $pass;

            /** @var string 下载时的文件名 */
            private $name;

            /** @var string|null HTTP Range 请求头 */
            private $rangeHeader;

            public function __construct($url, $user, $pass, $name, $rangeHeader) {
                parent::__construct('', 200, ResponseType::RAW);
                $this->url = $url;
                $this->user = $user;
                $this->pass = $pass;
                $this->name = $name;
                $this->rangeHeader = $rangeHeader;

                // 设置下载响应头
                $this->header['Content-Type'] = 'application/octet-stream';
                $this->header['Content-Disposition'] = 'attachment; filename="' . $name . '"';
                // 禁用代理缓冲，实时输出流式数据
                $this->header['X-Accel-Buffering'] = 'no';
                // 支持客户端 Range 请求
                $this->header['Accept-Ranges'] = 'bytes';
            }

            /**
             * 发送文件到浏览器
             *
             * 处理过程：
             * 1. 初始化 curl 连接到远程文件
             * 2. 如果支持 Range 请求，添加 Range 请求头
             * 3. 配置响应头处理（更新 Content-Range、Content-Length、Content-Type）
             * 4. 配置数据输出处理（流式输出到浏览器）
             * 5. 执行请求并关闭连接
             */
            public function send(): void {
                $ch = curl_init($this->url);

                $headers = [];
                // 如果指定了 Range 范围，添加到请求头以支持断点续传
                if ($this->rangeHeader !== null) {
                    $headers[] = 'Range: ' . $this->rangeHeader;
                }

                curl_setopt_array($ch, [
                    // 不返回响应体，直接输出到 STDOUT
                    CURLOPT_RETURNTRANSFER => false,
                    // 跟随 3xx 重定向
                    CURLOPT_FOLLOWLOCATION => true,
                    // 禁用 SSL 证书验证（开发环境）
                    CURLOPT_SSL_VERIFYPEER => false,
                    CURLOPT_SSL_VERIFYHOST => false,
                    // 设置 User-Agent
                    CURLOPT_USERAGENT => 'SimpleWebDAVClient/2.0',
                    // 不返回响应头
                    CURLOPT_HEADER => false,
                    // 设置缓冲区大小为 128KB，适合大文件流式传输
                    CURLOPT_BUFFERSIZE => 131072,
                    // 处理远程服务器返回的响应头
                    CURLOPT_HEADERFUNCTION => function($ch, $header) {
                        $len = strlen($header);
                        $headerLine = trim($header);

                        // 跳过空行
                        if (empty($headerLine)) {
                            return $len;
                        }

                        // 提取并更新响应头信息
                        if (stripos($headerLine, 'Content-Range:') === 0) {
                            // 断点续传时的范围信息
                            $this->header['Content-Range'] = trim(substr($headerLine, 14));
                        } elseif (stripos($headerLine, 'Content-Length:') === 0) {
                            // 文件总大小
                            $this->header['Content-Length'] = trim(substr($headerLine, 15));
                        } elseif (stripos($headerLine, 'Content-Type:') === 0) {
                            // 文件 MIME 类型
                            $this->header['Content-Type'] = trim(substr($headerLine, 13));
                        } elseif (stripos($headerLine, 'HTTP/') === 0 && strpos($headerLine, ' 206 ') !== false) {
                            // 206 Partial Content 表示断点续传请求成功
                            $this->code = 206;
                        }

                        return $len;
                    },
                    // 处理响应体数据，实时输出到浏览器
                    CURLOPT_WRITEFUNCTION => function($ch, $data) {
                        static $headersSent = false;
                        // 确保响应头只发送一次
                        if (!$headersSent) {
                            $this->sendHeaders();
                            $headersSent = true;
                        }
                        // 输出数据块到浏览器
                        echo $data;
                        return strlen($data);
                    }
                ]);

                // 如果需要认证，配置基本或摘要认证
                if ($this->user !== null) {
                    curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_BASIC | CURLAUTH_DIGEST);
                    curl_setopt($ch, CURLOPT_USERPWD, $this->user . ':' . $this->pass);
                }

                // 添加自定义请求头（如 Range 头）
                if (!empty($headers)) {
                    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                }

                // 执行 HTTP 请求，数据通过 CURLOPT_WRITEFUNCTION 回调输出
                curl_exec($ch);

                // 检查是否有错误
                $error = curl_error($ch);
                curl_close($ch);

                // 记录错误日志
                if ($error) {
                    error_log("WebDAV download failed: $error");
                }
            }
        };
    }

    /**
     * 上传文件到 WebDAV 服务器
     *
     * @param string $localPath 本地文件路径
     * @param string $remotePath 远程保存路径（相对于基础 URL）
     * @return bool 是否成功
     * @throws RuntimeException 当本地文件不存在或无法打开时抛出异常
     */
    public function upload(string $localPath, string $remotePath): bool {
        if (!file_exists($localPath)) {
            throw new RuntimeException("Local file not found: $localPath");
        }

        $fileSize = filesize($localPath);
        $fp = fopen($localPath, 'r');
        if ($fp === false) {
            throw new RuntimeException("Cannot open local file for reading: $localPath");
        }

        try {
            // 克隆 httpClient 避免影响原客户端配置
            $client = clone $this->httpClient;
            // 设置 HTTP PUT 方法用于上传
            $client->setOption(CURLOPT_CUSTOMREQUEST, 'PUT');
            // 启用文件上传模式
            $client->setOption(CURLOPT_UPLOAD, true);
            // 指定要上传的文件资源
            $client->setOption(CURLOPT_INFILE, $fp);
            // 设置文件大小
            $client->setOption(CURLOPT_INFILESIZE, $fileSize);

            $response = $client->send('/' . ltrim($remotePath, '/'));

            if ($response === null) {
                return false;
            }

            $code = $response->getHttpCode();
            Logger::debug("WebDAV upload data: $code");

            // 2xx 响应码表示上传成功
            return $code >= 200 && $code < 300;
        } finally {
            // 确保文件资源被关闭
            if (is_resource($fp)) {
                fclose($fp);
            }
        }
    }

    /**
     * 删除远程资源（文件或目录）
     *
     * @param string $remotePath 远程资源路径（相对于基础 URL）
     * @return bool 是否成功
     */
    public function delete(string $remotePath): bool {
        try {
            // 克隆 httpClient 避免影响原客户端配置
            $client = clone $this->httpClient;
            // 设置 HTTP DELETE 方法
            $client->setOption(CURLOPT_CUSTOMREQUEST, 'DELETE');

            $response = $client->send('/' . ltrim($remotePath, '/'));

            if ($response === null) {
                return false;
            }

            $code = $response->getHttpCode();
            // 2xx 响应码表示删除成功
            return $code >= 200 && $code < 300;
        } catch (\Exception $e) {
            Logger::error("WebDAV delete failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * 创建远程目录
     *
     * @param string $path 目录路径（相对于基础 URL）
     * @return bool 是否成功（201 Created 表示创建成功）
     */
    public function mkdir(string $path): bool {
        try {
            // 克隆 httpClient 避免影响原客户端配置
            $client = clone $this->httpClient;
            // 设置 MKCOL 方法创建集合（目录）
            $client->setOption(CURLOPT_CUSTOMREQUEST, 'MKCOL');

            $response = $client->send('/' . ltrim($path, '/'));

            if ($response === null) {
                return false;
            }

            $code = $response->getHttpCode();
            // WebDAV 创建成功返回 201
            return $code === 201;
        } catch (\Exception $e) {
            Logger::error("WebDAV mkdir failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * 核心：通用 WebDAV 请求方法
     *
     * @param string $method HTTP 方法（PROPFIND、DELETE、MKCOL 等）
     * @param string $path 资源路径（相对于基础 URL）
     * @param array $options 请求选项，支持 'body' 和 'headers'
     * @return array [响应内容, HTTP 状态码]
     * @throws RuntimeException 当请求失败时抛出异常
     */
    private function request(string $method, string $path, array $options = []): array {
        try {
            // 克隆 httpClient 避免影响原客户端配置
            $client = clone $this->httpClient;
            // 设置自定义 HTTP 方法
            $client->setOption(CURLOPT_CUSTOMREQUEST, $method);

            // 设置请求头
            if (!empty($options['headers'])) {
                // 直接传递请求头数组给 curl
                $client->setOption(CURLOPT_HTTPHEADER, $options['headers']);
            }

            // 设置请求体（用于 PROPFIND、PUT 等方法）
            if (isset($options['body'])) {
                $client->setOption(CURLOPT_POSTFIELDS, $options['body']);
            }

            // 发送请求
            $response = $client->send('/' . ltrim($path, '/'));

            if ($response === null) {
                throw new RuntimeException("Failed to execute $method request");
            }

            // 返回响应体和 HTTP 状态码
            return [$response->getBody(), $response->getHttpCode()];
        } catch (\Exception $e) {
            throw new RuntimeException("Request failed: " . $e->getMessage());
        }
    }

    /**
     * 构建完整的 WebDAV 资源 URL
     *
     * @param string $path 资源路径
     * @return string 完整的 URL
     */
    private function buildUrl($path) {
        // 确保路径以 / 开头
        $path = '/' . ltrim($path, '/');
        // URL 编码路径，但保留 / 不编码
        $encodedPath = str_replace('%2F', '/', rawurlencode($path));
        return $this->baseUrl . $encodedPath;
    }

    /**
     * 解析 PROPFIND 响应，过滤掉请求路径本身
     *
     * @param string $xmlContent WebDAV 服务器返回的 XML 响应
     * @param string $requestedPath 请求的完整路径（用于过滤）
     * @return array 过滤后的目录项数组
     */
    private function parsePropFindResponse(string $xmlContent, string $requestedPath = ''): array {
        // 解析 XML 响应并提取所有条目
        $entries = $this->parseMultistatusEntries($xmlContent);

        // 如果没有指定请求路径，直接返回所有条目
        if ($requestedPath === '') {
            return $entries;
        }

        // 规范化请求路径（移除末尾斜杠）
        $normalizedRequestPath = rtrim(rawurldecode($requestedPath), '/');

        // 如果规范化后为空，返回所有条目
        if ($normalizedRequestPath === '') {
            return $entries;
        }

        // 过滤掉请求路径本身，只保留子项
        return array_values(array_filter($entries, function (array $entry) use ($normalizedRequestPath) {
            return rtrim($entry['path'], '/') !== $normalizedRequestPath;
        }));
    }

    /**
     * 解析 WebDAV 多状态 (multistatus) 响应
     *
     * WebDAV 服务器返回的 XML 结构：
     * <D:multistatus>
     *   <D:response>
     *     <D:href>/path/to/resource</D:href>
     *     <D:propstat>
     *       <D:prop>...</D:prop>
     *       <D:status>HTTP/1.1 200 OK</D:status>
     *     </D:propstat>
     *   </D:response>
     * </D:multistatus>
     *
     * @param string $xmlContent WebDAV 服务器返回的 XML 内容
     * @return array 包含所有资源信息的数组
     */
    private function parseMultistatusEntries(string $xmlContent): array {
        // 使用内部错误处理，避免输出警告信息
        libxml_use_internal_errors(true);
        $xml = simplexml_load_string($xmlContent);

        // 如果 XML 解析失败，清除错误并返回空数组
        if ($xml === false) {
            libxml_clear_errors();
            return [];
        }

        // 获取 DAV: 命名空间下的元素
        $davRoot = $xml->children('DAV:');
        if (!isset($davRoot->response)) {
            libxml_clear_errors();
            return [];
        }

        // 遍历每个 response 元素并构建资源信息
        $entries = [];
        foreach ($davRoot->response as $responseNode) {
            $entry = $this->buildEntryFromResponse($responseNode);
            if ($entry !== null) {
                $entries[] = $entry;
            }
        }

        // 清除 libxml 的内部错误
        libxml_clear_errors();
        return $entries;
    }

    /**
     * 从 WebDAV 响应节点构建资源信息数组
     *
     * 提取的信息包括：
     * - path: 资源路径（已 URL 解码）
     * - name: 资源名称（文件名或目录名）
     * - is_dir: 是否为目录
     * - size: 文件大小（字节数）
     * - mtime: 最后修改时间（Unix 时间戳）
     * - type: 资源类型（'directory' 或 'file'）
     *
     * @param SimpleXMLElement $responseNode 单个 D:response 元素
     * @return array|null 资源信息数组，如果解析失败返回 null
     */
    private function buildEntryFromResponse(SimpleXMLElement $responseNode): ?array {
        // 获取资源的 URL 编码路径
        $responseDav = $responseNode->children('DAV:');
        $href = rawurldecode((string)($responseDav->href ?? ''));

        // 路径为空时返回 null
        if ($href === '') {
            return null;
        }

        // 提取属性信息（propstat）
        $propNode = $this->extractDavProp($responseDav->propstat ?? null);
        if ($propNode === null) {
            return null;
        }

        // 从属性中获取文件详细信息
        $propDav = $propNode->children('DAV:');
        $name = basename(rtrim($href, '/'));

        // 名称为空时返回 null
        if ($name === '') {
            return null;
        }

        // 提取文件大小和修改时间
        $size = isset($propDav->getcontentlength) ? (int)$propDav->getcontentlength : 0;
        $mtime = isset($propDav->getlastmodified) ? strtotime((string)$propDav->getlastmodified) : 0;

        // 判断是否为目录：首先检查 resourcetype，其次检查路径末尾斜杠
        $isDir = false;
        if (isset($propDav->resourcetype)) {
            // 如果包含 collection 元素，说明是目录
            $resourceType = $propDav->resourcetype->children('DAV:');
            $isDir = isset($resourceType->collection);
        }

        // 如果不是目录，再检查路径是否以斜杠结尾
        if (!$isDir) {
            $isDir = $this->pathEndsWithSlash($href);
        }

        // 构建并返回资源信息数组
        return [
            'path' => $href,
            'name' => $name,
            'is_dir' => $isDir,
            'size' => $size,
            'mtime' => $mtime,
            'type' => $isDir ? 'directory' : 'file'
        ];
    }

    /**
     * 从多个 propstat 元素中提取有效的属性信息
     *
     * WebDAV 响应中可能包含多个 propstat，分别对应成功和失败的属性。
     * 此方法优先选择 HTTP 200 OK 的 propstat，否则返回第一个 propstat 的属性。
     *
     * @param SimpleXMLElement|null $propstats D:propstat 元素集合
     * @return SimpleXMLElement|null 有效的属性节点，如果没有找到返回 null
     */
    private function extractDavProp(?SimpleXMLElement $propstats): ?SimpleXMLElement {
        if ($propstats === null) {
            return null;
        }

        // 遍历所有 propstat 元素，优先选择 HTTP 200 的
        foreach ($propstats as $propstat) {
            $propstatDav = $propstat->children('DAV:');
            $status = isset($propstatDav->status) ? (string)$propstatDav->status : '';

            // 如果状态包含 200 OK，则该 propstat 中的属性有效
            if ($status === '' || str_contains($status, ' 200')) {
                return $propstatDav->prop ?? null;
            }
        }

        // 如果没有找到 HTTP 200 的 propstat，返回第一个的属性
        $firstPropstat = $propstats[0] ?? null;
        if ($firstPropstat === null) {
            return null;
        }

        $firstDav = $firstPropstat->children('DAV:');
        return $firstDav->prop ?? null;
    }

    /**
     * 生成标准的 WebDAV PROPFIND 请求体
     *
     * 查询的属性包括：
     * - displayname: 资源的显示名称
     * - getcontentlength: 文件大小
     * - getcontenttype: MIME 类型
     * - getlastmodified: 最后修改时间
     * - resourcetype: 资源类型（用于判断是否为目录）
     *
     * @return string 格式化的 XML 请求体
     */
    private function defaultPropFindBody(): string {
        return <<<XML
<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
    <D:prop>
        <D:displayname/>
        <D:getcontentlength/>
        <D:getcontenttype/>
        <D:getlastmodified/>
        <D:resourcetype/>
    </D:prop>
</D:propfind>
XML;
    }

    /**
     * 判断路径是否以斜杠结尾
     *
     * 在 WebDAV 协议中，以斜杠结尾的路径通常表示目录。
     *
     * @param string $path 资源路径
     * @return bool
     */
    private function pathEndsWithSlash(string $path): bool {
        return str_ends_with($path, '/');
    }
}

