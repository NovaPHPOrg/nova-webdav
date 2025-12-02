<?php

namespace nova\plugin\webdav;

use nova\framework\http\Response;
use nova\framework\http\ResponseType;
use RuntimeException;
use SimpleXMLElement;
use function nova\framework\dump;

class SimpleWebDAVClient {
    protected $baseUrl;
    private $user;
    private $pass;

    public function __construct(string $baseUrl, ?string $user = null, ?string $pass = null) {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->user = $user;
        $this->pass = $pass;
    }
    
    /**
     * 获取 baseUrl
     */
    public function getBaseUrl(): string {
        return $this->baseUrl;
    }

    /**
     * 获取目录列表
     * @return array 包含文件信息的数组 ['path', 'name', 'is_dir', 'size', 'mtime', 'type']
     */
    public function listDir(string $path): array {
        $xmlBody = $this->defaultPropFindBody();

        list($response, $code) = $this->request('PROPFIND', $path, [
            'body' => $xmlBody,
            'headers' => ['Depth: 1', 'Content-Type: application/xml']
        ]);


        if ($code !== 207) {
            throw new RuntimeException("Failed to list directory. HTTP Code: $code, Path: $path");
        }

        // 构建完整的请求路径用于过滤
        $fullPath = parse_url($this->baseUrl, PHP_URL_PATH) . '/' . ltrim($path, '/');
        return $this->parsePropFindResponse($response, $fullPath);
    }

    /**
     * 获取指定路径的资源属性（Depth: 0）
     */
    public function getResourceInfo(string $path): ?array {
        $xmlBody = $this->defaultPropFindBody();

        list($response, $code) = $this->request('PROPFIND', $path, [
            'body' => $xmlBody,
            'headers' => ['Depth: 0', 'Content-Type: application/xml']
        ]);

        if ($code !== 207) {
            throw new RuntimeException("Failed to get resource info. HTTP Code: $code, Path: $path");
        }

        $entries = $this->parseMultistatusEntries($response);
        return $entries[0] ?? null;
    }

    /**
     * 判断路径是否为目录
     */
    public function isDirectory(string $path): bool {
        try {
            $info = $this->getResourceInfo($path);
            return $info !== null && ($info['is_dir'] ?? false);
        } catch (RuntimeException $e) {
            return false;
        }
    }

    /**
     * 下载文件并保存到本地
     */
    public function download(string $remotePath, string $localPath): bool {
        $fp = fopen($localPath, 'w+');
        if ($fp === false) {
            throw new RuntimeException("Cannot open local file for writing: $localPath");
        }

        try {
            list(, $code) = $this->request('GET', $remotePath, [
                'sink' => $fp
            ]);
            return $code >= 200 && $code < 300;
        } catch (\RuntimeException $e){
            return false;
        }finally {
            if (is_resource($fp)) {
                fclose($fp);
            }
        }
    }

    /**
     * 直接下载文件并输出到浏览器 (结合 Response)
     * 返回一个特殊的 Response 对象，该对象在发送时会流式传输内容
     * 支持 HTTP Range 请求（断点续传）
     * 
     * @param string $remotePath 远程文件路径
     * @param string|null $downloadName 下载保存的文件名，默认为远程文件名
     * @param string|null $rangeHeader 客户端的 Range 请求头，如 "bytes=0-1048575"
     * @return Response
     */
    public function downloadToResponse(string $remotePath, ?string $downloadName = null, ?string $rangeHeader = null): Response {
        $url = $this->buildUrl($remotePath);

        $name = $downloadName ?? basename(urldecode($remotePath));
        
        // 创建匿名类继承 Response，支持 Range 请求的流式传输
        return new class($url, $this->user, $this->pass, $name, $rangeHeader) extends Response {
            private $url;
            private $user;
            private $pass;
            private $name;
            private $rangeHeader;

            public function __construct($url, $user, $pass, $name, $rangeHeader) {
                // 初始化父类，默认 200，如果有 Range 会在 send() 中改为 206
                parent::__construct('', 200, ResponseType::RAW);
                $this->url = $url;
                $this->user = $user;
                $this->pass = $pass;
                $this->name = $name;
                $this->rangeHeader = $rangeHeader;
                
                // 基础头信息
                $this->header['Content-Type'] = 'application/octet-stream';
                $this->header['Content-Disposition'] = 'attachment; filename="' . $name . '"';
                $this->header['X-Accel-Buffering'] = 'no';
                // 声明支持 Range 请求
                $this->header['Accept-Ranges'] = 'bytes';
            }

            public function send(): void {
                $ch = curl_init($this->url);
                
                $headers = [];
                
                // 如果客户端发送了 Range 请求，转发给 WebDAV 服务器
                if ($this->rangeHeader !== null) {
                    $headers[] = 'Range: ' . $this->rangeHeader;
                }
                
                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => false,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_SSL_VERIFYPEER => false,
                    CURLOPT_SSL_VERIFYHOST => false,
                    CURLOPT_USERAGENT => 'SimpleWebDAVClient/2.0',
                    CURLOPT_HEADER => false,
                    CURLOPT_BUFFERSIZE => 131072, // 128KB，减少回调次数
                    // 透传响应头：只捕获关键头部，不做任何修改判断
                    CURLOPT_HEADERFUNCTION => function($ch, $header) {
                        $len = strlen($header);
                        $headerLine = trim($header);
                        
                        if (empty($headerLine)) {
                            return $len;
                        }
                        
                        // 只透传这几个关键头，原样设置
                        if (stripos($headerLine, 'Content-Range:') === 0) {
                            $this->header['Content-Range'] = trim(substr($headerLine, 14));
                        } elseif (stripos($headerLine, 'Content-Length:') === 0) {
                            $this->header['Content-Length'] = trim(substr($headerLine, 15));
                        } elseif (stripos($headerLine, 'Content-Type:') === 0) {
                            $this->header['Content-Type'] = trim(substr($headerLine, 13));
                        } elseif (stripos($headerLine, 'HTTP/') === 0 && strpos($headerLine, ' 206 ') !== false) {
                            $this->code = 206;
                        }
                        
                        return $len;
                    },
                    // 流式输出数据
                    CURLOPT_WRITEFUNCTION => function($ch, $data) {
                        static $headersSent = false;
                        if (!$headersSent) {
                            $this->sendHeaders();
                            $headersSent = true;
                        }
                        echo $data;
                        return strlen($data);
                    }
                ]);

                if ($this->user !== null) {
                    curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_BASIC | CURLAUTH_DIGEST);
                    curl_setopt($ch, CURLOPT_USERPWD, $this->user . ':' . $this->pass);
                }
                
                if (!empty($headers)) {
                    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                }

                curl_exec($ch);
                
                $error = curl_error($ch);
                curl_close($ch);
                
                if ($error) {
                    error_log("WebDAV download failed: $error");
                }
            }
        };
    }

    /**
     * 上传文件
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
            list(, $code) = $this->request('PUT', $remotePath, [
                'upload_source' => $fp,
                'upload_size' => $fileSize
            ]);

            return $code >= 200 && $code < 300;
        } finally {
            if (is_resource($fp)) {
                fclose($fp);
            }
        }
    }

    public function delete(string $remotePath): bool {
        list(, $code) = $this->request('DELETE', $remotePath);
        return $code >= 200 && $code < 300;
    }

    public function mkdir(string $path): bool {
        list(, $code) = $this->request('MKCOL', $path);
        return $code === 201;
    }

    private function buildUrl($path) {
        $path = '/' . ltrim($path, '/');
        $encodedPath = str_replace('%2F', '/', rawurlencode($path));
        return $this->baseUrl . $encodedPath;
    }

    private function request(string $method, string $path, array $options = []): array {
        $url = $this->buildUrl($path);
        $ch = curl_init($url);

        // 是否是带有请求体的上传（如 PUT）
        $isUpload = isset($options['upload_source']) && is_resource($options['upload_source']);

        // 基础 cURL 选项
        $curlOptions = [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_RETURNTRANSFER => true,
            // 注意：上传场景禁用自动重定向，避免 cURL 需要重放请求体而触发 rewind 错误
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => false,
            CURLOPT_USERAGENT      => 'SimpleWebDAVClient/2.0',
        ];

        // 非上传场景才允许自动跟随重定向
        if (!$isUpload) {
            $curlOptions[CURLOPT_FOLLOWLOCATION] = true;
        }

        curl_setopt_array($ch, $curlOptions);

        if ($this->user !== null) {
            /**
             * 认证策略说明：
             * - 对上传（PUT 等带请求体）只使用 BASIC，避免 Digest 认证握手导致请求被重放，从而触发
             *   "necessary data rewind wasn't possible" 错误
             * - 对其他请求保持原有 BASIC | DIGEST 行为
             */
            $authMode = $isUpload ? CURLAUTH_BASIC : (CURLAUTH_BASIC | CURLAUTH_DIGEST);
            curl_setopt($ch, CURLOPT_HTTPAUTH, $authMode);
            curl_setopt($ch, CURLOPT_USERPWD, $this->user . ':' . $this->pass);
        }

        $headers = $options['headers'] ?? [];

        if ($isUpload) {
            curl_setopt($ch, CURLOPT_UPLOAD, true);
            curl_setopt($ch, CURLOPT_INFILE, $options['upload_source']);
            curl_setopt($ch, CURLOPT_INFILESIZE, $options['upload_size']);
        }
        elseif (isset($options['body'])) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $options['body']);
        }

        if (isset($options['sink']) && is_resource($options['sink'])) {
            curl_setopt($ch, CURLOPT_FILE, $options['sink']);
        }

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $response = curl_exec($ch);
        $info = curl_getinfo($ch);
        $error = curl_error($ch);
        
        curl_close($ch);

        if ($error) {
            throw new RuntimeException("cURL Error: $error");
        }

        return [$response, $info['http_code']];
    }

    private function parsePropFindResponse(string $xmlContent, string $requestedPath = ''): array {
        $entries = $this->parseMultistatusEntries($xmlContent);

        if ($requestedPath === '') {
            return $entries;
        }

        $normalizedRequestPath = rtrim(rawurldecode($requestedPath), '/');

        if ($normalizedRequestPath === '') {
            return $entries;
        }

        return array_values(array_filter($entries, function (array $entry) use ($normalizedRequestPath) {
            return rtrim($entry['path'], '/') !== $normalizedRequestPath;
        }));
    }

    private function parseMultistatusEntries(string $xmlContent): array {
        libxml_use_internal_errors(true);
        $xml = simplexml_load_string($xmlContent);

        if ($xml === false) {
            libxml_clear_errors();
            return [];
        }

        $davRoot = $xml->children('DAV:');
        if (!isset($davRoot->response)) {
            libxml_clear_errors();
            return [];
        }

        $entries = [];
        foreach ($davRoot->response as $responseNode) {
            $entry = $this->buildEntryFromResponse($responseNode);
            if ($entry !== null) {
                $entries[] = $entry;
            }
        }

        libxml_clear_errors();
        return $entries;
    }

    private function buildEntryFromResponse(SimpleXMLElement $responseNode): ?array {
        $responseDav = $responseNode->children('DAV:');
        $href = rawurldecode((string)($responseDav->href ?? ''));

        if ($href === '') {
            return null;
        }

        $propNode = $this->extractDavProp($responseDav->propstat ?? null);
        if ($propNode === null) {
            return null;
        }

        $propDav = $propNode->children('DAV:');
        $name = basename(rtrim($href, '/'));

        if ($name === '') {
            return null;
        }

        $size = isset($propDav->getcontentlength) ? (int)$propDav->getcontentlength : 0;
        $mtime = isset($propDav->getlastmodified) ? strtotime((string)$propDav->getlastmodified) : 0;

        $isDir = false;
        if (isset($propDav->resourcetype)) {
            $resourceType = $propDav->resourcetype->children('DAV:');
            $isDir = isset($resourceType->collection);
        }

        if (!$isDir) {
            $isDir = $this->pathEndsWithSlash($href);
        }

        return [
            'path' => $href,
            'name' => $name,
            'is_dir' => $isDir,
            'size' => $size,
            'mtime' => $mtime,
            'type' => $isDir ? 'directory' : 'file'
        ];
    }

    private function extractDavProp(?SimpleXMLElement $propstats): ?SimpleXMLElement {
        if ($propstats === null) {
            return null;
        }

        foreach ($propstats as $propstat) {
            $propstatDav = $propstat->children('DAV:');
            $status = isset($propstatDav->status) ? (string)$propstatDav->status : '';

            if ($status === '' || str_contains($status, ' 200')) {
                return $propstatDav->prop ?? null;
            }
        }

        $firstPropstat = $propstats[0] ?? null;
        if ($firstPropstat === null) {
            return null;
        }

        $firstDav = $firstPropstat->children('DAV:');
        return $firstDav->prop ?? null;
    }

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

    private function pathEndsWithSlash(string $path): bool {
        return str_ends_with($path, '/');
    }
}
