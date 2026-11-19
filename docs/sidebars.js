const sidebars = {
  sidebar: [
    "index",
    "getting-started",
    {
      type: "category",
      label: "Reference",
      collapsed: false,
      items: [
        "s3-operations",
        "settings",
        "options",
        "errors",
        "models"
      ]
    },
    "tutorial",
    {
      type: "category",
      label: "How-to Guides",
      collapsed: true,
      items: [
        "credentials",
        "connect-to-an-s3-compatible-store",
        "manage-buckets",
        "upload-a-large-file-in-parts",
        "download-an-object",
        "list-objects-page-by-page",
        "set-and-read-object-metadata",
        "handle-errors",
        "run-s3-calls-with-the-raw-client",
        "testing"
      ]
    }
  ]
};

module.exports = sidebars;
