# Third-Party Components

## SortableJS 1.15.7

- Used for dashboard widget sorting, pointer-following card previews and touch support.
- Source: https://github.com/SortableJS/Sortable/releases/tag/1.15.7
- Local bundle: `frontend/assets/vendor/Sortable-1.15.7.min.js`
- License: MIT, retained in `frontend/assets/vendor/Sortable.LICENSE.txt` and the bundle header.
- Bundle SHA-256: `bf4241bc73fef7f11c59a283a69fe8051cdd31c6d8ff5a2b9ba219e7831fcf76`
- The upstream bundle is unmodified. No CDN requests, package install scripts or additional services run in production.

Homepage and Homarr were considered as dashboard-pattern references, not copied or installed. GridStack was considered for free-form resizing, but this pass retains the existing responsive layout and persistence rather than replacing them with a coordinate-based grid engine.
