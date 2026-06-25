window.pageLoadFiles = [
    'Form',
];

window.pageOnLoad = function () {
    $.form.manage('/webdav/api/config', '#form');

    window.pageOnUnLoad = function () {
    };
};
