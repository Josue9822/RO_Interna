# 🏗️ Sistema SGC – Batalla de Junín S.A.C.
**Sistema de Gestión de Reportes de Incidencias (RI)**

---

## 📁 Estructura del Proyecto

```
batalla_junin/
├── app.py                        ← App principal (Streamlit)
├── requirements.txt              ← Dependencias Python
├── .gitignore                    ← Excluye secrets y DB
├── assets/
│   ├── BJ_PNG.png                ← Logo empresa
│   └── sellos/                   ← Sellos por área (PNG)
│       ├── S_ADMIN.png
│       ├── S_LOGISTICA.png
│       └── ... (todos los sellos)
└── .streamlit/
    └── secrets.toml              ← ⚠️ NO subir a GitHub
```

---

## 🚀 Pasos para desplegar en Streamlit Cloud

### 1. Subir a GitHub
```bash
git init
git add app.py requirements.txt assets/ .gitignore README.md
# ⚠️ NO hacer git add .streamlit/secrets.toml
git commit -m "Sistema SGC BJ - Producción"
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
git push -u origin main
```

### 2. Crear app en Streamlit Cloud
1. Ve a [share.streamlit.io](https://share.streamlit.io)
2. Conecta tu cuenta GitHub
3. Selecciona el repositorio y rama `main`
4. **Main file:** `app.py`
5. Click en **Deploy**

### 3. Configurar Secrets en Streamlit Cloud
En tu app: **Settings → Secrets** → pega el contenido de `.streamlit/secrets.toml` con tus datos reales.

Variables requeridas:
- `APP_URL` → URL pública de tu app (ej: `https://miapp.streamlit.app`)
- `LOGIN_ADMIN_USER` / `LOGIN_ADMIN_PASS`
- `LOGIN_STAFF_USER` / `LOGIN_STAFF_PASS`
- `[gcp_service_account]` → Contenido de tu JSON de Google

---

## 📌 Notas importantes

- **SQLite en producción:** Streamlit Cloud reinicia periódicamente el servidor. La base de datos SQLite se usa como caché temporal. La fuente de verdad es **Google Sheets**.
- **Sellos:** Sube todos los archivos PNG de sellos a la carpeta `assets/sellos/`.
- **Logo:** Coloca `BJ_PNG.png` en la carpeta `assets/`.
- Los PDFs se generan en carpeta temporal del servidor y se descargan al instante.

---

## 🔐 Seguridad
- Nunca subas `secrets.toml` ni archivos `.json` de Google a GitHub.
- El `.gitignore` ya los excluye.
- Cambia las contraseñas de login en los Secrets de Streamlit Cloud.
