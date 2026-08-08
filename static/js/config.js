window.pageLoadFiles = [
    'Form',
];

window.pageOnLoad = function () {
    $.form.manage('/webdav/api/config', '#form');

    $('#test_webdav').on('click', function () {
        const $btn = $(this);
        const data = $.form.val('#form');
        if (!String(data.url || '').trim()) {
            $.toaster.warn('请填写主机域');
            return;
        }

        $btn.prop('disabled', true);
        $.request.postForm('/webdav/api/test', data, (res) => {
            $btn.prop('disabled', false);
            if (res.code === 200) {
                $.toaster.success(res.msg || '连接成功');
            } else {
                $.toaster.error(res.msg || '连接失败');
            }
        }, () => {
            $btn.prop('disabled', false);
            $.toaster.error('请求失败');
        });
    });

    window.pageOnUnLoad = function () {
    };
};
