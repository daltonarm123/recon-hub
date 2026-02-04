Recon-Hub NWOT Frontend Update (SOURCE files)

This zip contains updated SOURCE files:
- App.jsx (adds NWOT nav + route + NWOT page)
- NWChart.jsx (renders /api/nw/history/{kingdom} points array)

Where to copy these:
Your project may store these files in ONE of these places:
1) frontend/src/App.jsx and frontend/src/NWChart.jsx
2) frontend/App.jsx and frontend/NWChart.jsx

Copy the files into the correct folder (overwrite existing).

Then rebuild the frontend bundle and copy it to backend/static:

Mac/Linux:
  cd frontend
  npm install
  npm run build
  rm -rf ../backend/static/*
  cp -R dist/* ../backend/static/
  cd ..
  git add backend/static
  git commit -m "Update frontend build (NWOT)"
  git push

Windows PowerShell:
  cd frontend
  npm install
  npm run build
  Remove-Item -Recurse -Force ..\backend\static\*
  Copy-Item -Recurse dist\* ..\backend\static\
  cd ..
  git add backend/static
  git commit -m "Update frontend build (NWOT)"
  git push

After Render deploy finishes, open:
  https://recon-hub.onrender.com/nwot
