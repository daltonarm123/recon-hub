from fastapi import FastAPI, Request
from fastapi.responses import Response


_SCRIPT = r'''
(() => {
  if (window.__reconResetLinksLoaded) return;
  window.__reconResetLinksLoaded = true;

  async function createResetLink(userId, username, button) {
    if (!confirm(`Create a one-time password reset link for ${username || userId}?`)) return;
    const oldText = button.textContent;
    button.disabled = true;
    button.textContent = 'Creating...';
    try {
      const response = await fetch('/api/admin/users/create-reset-link', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        credentials: 'include',
        body: JSON.stringify({user_id: userId})
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data?.detail || `HTTP ${response.status}`);
      const url = data.reset_url || '';
      await navigator.clipboard?.writeText(url).catch(() => {});
      prompt('Reset link created. It expires in 30 minutes and can only be used once. Copy and send this link to the user:', url);
    } catch (error) {
      alert(error?.message || 'Could not create reset link.');
    } finally {
      button.disabled = false;
      button.textContent = oldText;
    }
  }

  function enhanceUserTable() {
    const headings = [...document.querySelectorAll('div')].filter(
      (node) => node.textContent?.trim() === 'Alliance Access Control'
    );
    if (!headings.length) return;

    let panel = headings[0].parentElement;
    while (panel && !panel.querySelector('table')) panel = panel.parentElement;
    const table = panel?.querySelector('table');
    if (!table || table.dataset.resetLinksReady === '1') return;

    const header = table.querySelector('thead tr');
    if (header) {
      const th = document.createElement('th');
      th.textContent = 'Account Reset';
      th.style.cssText = 'text-align:left;padding:8px 6px;border-bottom:1px solid rgba(255,255,255,.12);color:rgba(231,236,255,.75)';
      header.appendChild(th);
    }

    table.querySelectorAll('tbody tr').forEach((row) => {
      const cells = row.querySelectorAll('td');
      const username = cells[0]?.textContent?.trim() || '';
      const userId = cells[1]?.textContent?.trim() || '';
      if (!userId) return;

      const td = document.createElement('td');
      td.dataset.label = 'Account Reset';
      td.style.cssText = 'padding:8px 6px;border-bottom:1px solid rgba(255,255,255,.08)';
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Create Reset Link';
      button.style.cssText = 'background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.15);color:#e7ecff;border-radius:10px;padding:8px 10px;cursor:pointer;font-size:12px';
      button.addEventListener('click', () => createResetLink(userId, username, button));
      td.appendChild(button);
      row.appendChild(td);
    });

    table.dataset.resetLinksReady = '1';
  }

  new MutationObserver(enhanceUserTable).observe(document.documentElement, {subtree: true, childList: true});
  enhanceUserTable();
})();
'''


def install_admin_reset_ui(app: FastAPI) -> None:
    @app.get('/admin-reset-ui.js', include_in_schema=False)
    def admin_reset_ui_script():
        return Response(content=_SCRIPT, media_type='application/javascript', headers={'Cache-Control': 'no-store'})

    @app.middleware('http')
    async def add_admin_reset_script(request: Request, call_next):
        response = await call_next(request)
        content_type = str(response.headers.get('content-type') or '').lower()
        if response.status_code != 200 or 'text/html' not in content_type:
            return response

        body = b''
        async for chunk in response.body_iterator:
            body += chunk
        text = body.decode('utf-8', errors='replace')
        tag = '<script src="/admin-reset-ui.js"></script>'
        if tag not in text:
            text = text.replace('</body>', f'{tag}</body>', 1) if '</body>' in text else text + tag

        headers = dict(response.headers)
        headers.pop('content-length', None)
        return Response(content=text, status_code=response.status_code, headers=headers, media_type='text/html')
