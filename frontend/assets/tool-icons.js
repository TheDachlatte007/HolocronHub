// Curated choices use the existing icon_url field; custom URLs stay editable.
const TOOL_ICON_LIBRARY = [
  { id: 'auto', name: 'Automatic', url: '' },
  ...[
    ['homeassistant', 'Home Assistant'], ['jellyfin', 'Jellyfin'],
    ['proxmox', 'Proxmox'], ['pihole', 'Pi-hole'], ['truenas', 'TrueNAS'],
    ['portainer', 'Portainer'], ['grafana', 'Grafana'], ['uptimekuma', 'Kuma'],
    ['googlegemini', 'Gemini'], ['anthropic', 'Claude'], ['ollama', 'Ollama'],
    ['huggingface', 'Hugging Face'],
  ].map(([id, name]) => ({ id, name, url: `https://cdn.simpleicons.org/${id}/d8f5ff` })),
  ...[
    ['home', 'Home', '<path d="m3 10 9-7 9 7v10H15v-7H9v7H3z"/>'],
    ['server', 'Server', '<rect x="4" y="3" width="16" height="7" rx="2"/><rect x="4" y="14" width="16" height="7" rx="2"/><path d="M8 6.5h.01M8 17.5h.01M12 6.5h4M12 17.5h4"/>'],
    ['storage', 'Storage', '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0"/>'],
    ['network', 'Network', '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c5 5 5 13 0 18-5-5-5-13 0-18Z"/>'],
    ['media', 'Media', '<rect x="3" y="4" width="18" height="16" rx="3"/><path d="m10 8 6 4-6 4z"/>'],
    ['monitor', 'Monitor', '<path d="M2 12h5l2-7 5 14 2-7h6"/>'],
    ['automation', 'Automation', '<path d="m13 2-9 12h7l-1 8 10-13h-7z"/>'],
    ['code', 'Code', '<path d="m7 6-5 6 5 6m10-12 5 6-5 6M14 3l-4 18"/>'],
  ].map(([id, name, glyph]) => ({ id, name, url: 'data:image/svg+xml,' + encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#85e9f5" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">${glyph}</svg>`
  ) })),
];

function iconLibraryMarkup(inputId, expanded = false) {
  return `<details class="icon-picker" ${expanded ? 'open' : ''}><summary>Choose an icon</summary>
    <div class="icon-picker-grid" data-icon-picker="${esc(inputId)}">${TOOL_ICON_LIBRARY.map(icon => `
      <button type="button" class="icon-choice" data-icon-id="${icon.id}" aria-pressed="false" onclick="chooseToolIcon('${esc(inputId)}','${icon.id}')">
        ${icon.url ? `<img src="${esc(icon.url)}" alt="" />` : '<span aria-hidden="true">A</span>'}<span>${esc(icon.name)}</span>
      </button>`).join('')}</div>
    <div class="icon-picker-current" data-icon-preview="${esc(inputId)}"></div></details>`;
}

function chooseToolIcon(inputId, iconId) {
  const input = document.getElementById(inputId);
  const icon = TOOL_ICON_LIBRARY.find(choice => choice.id === iconId);
  if (!input || !icon) return;
  input.value = icon.url;
  syncIconPicker(inputId);
}

function syncIconPicker(inputId) {
  const value = document.getElementById(inputId)?.value.trim() || '';
  document.querySelectorAll(`[data-icon-picker="${inputId}"] .icon-choice`).forEach(button => {
    const selected = TOOL_ICON_LIBRARY.find(icon => icon.id === button.dataset.iconId)?.url === value;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-pressed', String(selected));
  });
  const preview = document.querySelector(`[data-icon-preview="${inputId}"]`);
  if (preview) preview.innerHTML = value ? `<img src="${esc(value)}" alt="Selected icon" onerror="this.hidden=true"/><span>Selected icon</span>` : '<span>Automatic: detected from the service name or URL.</span>';
}
