const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "Getting Started",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        "tutorial"
      ]
    },
    {
      type: "category",
      label: "How-to Guides",
      collapsed: false,
      items: [
        "credentials",
        "s3-compatible-endpoints",
        "multipart-upload",
        "list-and-paginate-objects",
        "object-metadata",
        "testing"
      ]
    },
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
    }
  ]
};

module.exports = sidebars;
