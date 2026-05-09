# SISTEMA SGC - BATALLA DE JUNIN S.A.C.
# VERSION PRODUCCION - STREAMLIT CLOUD
# Gestión RI: Login + PDF + Google Sheets (2 Fases) + 6M/5W

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import os
import urllib.parse
from fpdf import FPDF
import gspread
from google.oauth2.service_account import Credentials
import json
from zoneinfo import ZoneInfo

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="RI - Batalla de Junin", page_icon="🏗️", layout="centered")

URL_SHEETS = "https://docs.google.com/spreadsheets/d/1zFug8ZcmhNzZ24LX8oEu-sKqfUenpbIJs8DB6t_0Ch8/edit?usp=sharing"
SPREADSHEET_ID = "1zFug8ZcmhNzZ24LX8oEu-sKqfUenpbIJs8DB6t_0Ch8"

# Diccionario de IDs de Google Sheets por Área
# Reemplaza los ejemplos con los IDs reales de tus nuevos archivos
IDS_POR_AREA = {
    "PLANEAMIENTO ESTRATEGICO": "1D0aOTXOMeEgqJdJAJ0vtEQ0rRdh5eyjDUXX0BSXRBkE",
    "ADMINISTRACION": "1k0mjAYv6MRFpGVejPfR1FCnpRDwpqQiAZFceiQGjPqo",
    "RECURSOS HUMANOS": "1qXTCHeRviUK7ViJWsCNcDuQ7p409QijljJUmt9fZcHs",
    "CONTABILIDAD": "1b5u3k3WMrEWZC-PmcuQf1JUA6gy-m1uGtXCAGWK_AIQ",
    "G.P. CONSTRUCCION": "1fcLm3me-fLHwUj_ugfzwYT2XMBwTQRNgjNJHrvIvij4",
    "SSOMA": "1s28ZbfklZZ9q7rnsL1JE1-JavosreQkJzrPyfBJVoL0",
    "EQUIPOS": "1hh0rSIi8uQX0Egkf5xJDw_GisHpA9WpVGCeN8nF2BNw",
    "LOGISTICA": "1E-giuxgI2VuJXmQCwhLqlgud7KLdsnDiorC4v5Ypuv0",
    "COMERCIAL": "1gIvIQcAJoSdJrhwgaxL0m2ffha-T5AuaON6E2L_tia8",
    "INGENIERIA": "1-6LjDUhd48fx7FO-YYd3SjDLwQWM2I9dyxlI5dF2__0",
    "COMEX": "1bdTBvFXTxKW1pXDF3MTG4G7LnyCNcs2RB6WyOzB-cpM",
    "OBRAS CIVILES": "12uaFomgVzDSwegTd27KR7sN20xt9qqCvyfpxpBz-wlk",
    "GESTION DE PROCESOS": "1fMV0yRo84-P4mtmzk95qm2gRZrrLrBwifFFdF7ThGog"
    
}
# El SPREADSHEET_ID original se usará como respaldo (General)

# Rutas relativas al proyecto (producción)
ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
PATH_SELLOS = os.path.join(ASSETS_DIR, "sellos")
LOGO_PATH = os.path.join(ASSETS_DIR, "BJ_PNG.png")
DB_NAME = os.path.join(os.path.dirname(__file__), "sistema_bj.db")

SHEETS_AVAILABLE = False

# --- CREDENCIALES GOOGLE DESDE st.secrets ---
def get_google_credentials():
    """Carga credenciales desde st.secrets (Streamlit Cloud) o variable de entorno."""
    try:
        # En Streamlit Cloud: secrets.toml o Secrets UI
        creds_dict = dict(st.secrets["gcp_service_account"])
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        return Credentials.from_service_account_info(creds_dict, scopes=scopes)
    except Exception as e:
        st.error(f"❌ Error de credenciales Google: {e}")
        return None

# --- FUNCIONES DE GOOGLE SHEETS ---
def guardar_en_sheets(fila: list, area_empleado):
    """Fase 1: Crea una nueva fila en Sheets cuando el Jefe emite el reporte."""
    try:
        creds = get_google_credentials()
        if not creds: return False
        cliente = gspread.authorize(creds)
        
        # 1. IDENTIFICAR EL ARCHIVO DESTINO
        nombre_area = str(area_empleado).strip().upper()
        target_id = IDS_POR_AREA.get(nombre_area, SPREADSHEET_ID)
        
        # 2. ABRIR EL ARCHIVO CORRESPONDIENTE
        spreadsheet = cliente.open_by_key(target_id)
        
        # 3. SELECCIONAR PESTAÑA
        try:
            hoja_destino = spreadsheet.worksheet("Reportes")
        except:
            hoja_destino = spreadsheet.get_worksheet(0)
        
        # --- CORRECCIÓN DEL ERROR 400 ---
        # Contamos cuántos IDs hay para ir a la siguiente fila
        columna_a = hoja_destino.col_values(1)
        siguiente_fila = len(columna_a) + 1
        
        # Cambiamos G por M, porque tu lista 'fila_fase_1' tiene 13 elementos
        rango_destino = f"A{siguiente_fila}:M{siguiente_fila}"
        
        # 4. GUARDAR
        hoja_destino.update(
            range_name=rango_destino,
            values=[fila],
            value_input_option="USER_ENTERED"
        )
        return True
        
    except Exception as e:
        st.error(f"❌ Error al guardar en el archivo de {area_empleado}: {e}")
        return False

def actualizar_en_sheets(ro_id, datos_actualizar: list, nombre_area: str):
    """Fase 2: Busca la fila por ID y la actualiza cuando el Colaborador cierra el reporte."""
    try:
        creds = get_google_credentials()
        if not creds: return False
        cliente = gspread.authorize(creds)
        
        area_key = str(nombre_area).strip().upper() 
        target_id = IDS_POR_AREA.get(area_key, SPREADSHEET_ID)
        spreadsheet = cliente.open_by_key(target_id)
        
        try:
            hoja = spreadsheet.worksheet("Reportes")
        except:
            hoja = spreadsheet.get_worksheet(0)

        # ID exacto a buscar
        id_buscar = f"RI-{int(ro_id):03d}"
        
        # CAMBIO CLAVE: Buscamos coincidencia EXACTA para evitar sobreescribir otros
        import re
        # Esto busca el ID exacto, no algo que "contenga" el ID
        fmt = re.compile(r'^' + id_buscar + r'$')
        celda = hoja.find(fmt, in_column=1)

        if celda:
            fila_idx = celda.row
            # Verificamos si la celda de "Estado" (Columna K / 11) ya está como "Resuelto"
            # Esto evita que alguien vuelva a enviar un reporte ya cerrado
            estado_actual = hoja.cell(fila_idx, 11).value
            if estado_actual == "Resuelto":
                st.warning("⚠️ Este reporte ya fue cerrado anteriormente.")
                return False

            hoja.update(
                values=[datos_actualizar],
                range_name=f"H{fila_idx}:M{fila_idx}",
                value_input_option="USER_ENTERED"
            )
            return True
        else:
            st.error(f"No se encontró el ID {id_buscar} en {nombre_area}")
            return False
    except Exception as e:
        st.error(f"Error técnico: {e}")
        return False

# --- 2. SEGURIDAD / LOGIN ---
def login_screen():
    if 'auth' not in st.session_state:
        st.session_state.auth = False
        st.session_state.user_role = None
        st.session_state.user_data = None # Para guardar info del jefe logueado

    if not st.session_state.auth:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_container_width=True)
            st.markdown("<h3 style='text-align:center; color:#990000;'>CONTROL DE ACCESO</h3>", unsafe_allow_html=True)
            
            with st.form("login_bj"):
                u = st.text_input("Correo Institucional")
                p = st.text_input("Contraseña", type="password")
                
                if st.form_submit_button("INGRESAR AL SISTEMA"):
                    # Buscamos al usuario en el DataFrame de empleados
                    # Usamos .strip() para evitar errores por espacios invisibles
                    user_match = df_empleados[
                        (df_empleados['CORREO'].str.strip() == u.strip()) & 
                        (df_empleados['WHATSAPP'].str.strip() == p.strip())
                    ]

                    if not user_match.empty:
                        datos = user_match.iloc[0]
                        st.session_state.auth = True
                        # Si es Jefe, le damos su rol; si no, queda como staff
                        st.session_state.user_role = "jefe" if datos['ROL'].strip().capitalize() == "Jefe" else "staff"
                        st.session_state.user_data = datos # Guardamos toda su fila
                        st.rerun()
                    else:
                        st.error("❌ Credenciales incorrectas o usuario no registrado.")
        st.stop()

# --- 3. ESTILOS VISUALES ---
st.markdown("""
<style>
    .stApp, .stMain, .main, .block-container, [data-testid="stHeader"] { background-color: #FFFFFF !important; }
    :root { --bj-red: #990000; --bj-grey: #f0f2f6; }
    .stApp { background-color: #FFFFFF; font-family: 'Segoe UI', sans-serif; }
    h1, h2, h3, h4, h5, h6 { color: var(--bj-red) !important; font-weight: 700 !important; }
    .stTabs [data-baseweb="tab"] { background-color: var(--bj-grey); color: #333; font-weight: 600; border: 1px solid #ddd; }
    .stTabs [aria-selected="true"] { background-color: var(--bj-red) !important; color: white !important; border: none; }
    .stSelectbox label, .stTextArea label, .stTextInput label, .stCheckbox label { color: #333 !important; font-size: 14px !important; font-weight: bold !important; }
    div.stButton > button { background-color: var(--bj-red) !important; color: white !important; font-weight: bold !important; border-radius: 6px !important; border: none !important; width: 100% !important; transition: 0.2s !important; height: 45px !important;}
    div.stButton > button:hover { background-color: #cc0000 !important; transform: scale(1.01) !important; }
    .bj-report-box { background-color: #f9f9f9; border: 1px solid #ddd; border-left: 6px solid var(--bj-red); padding: 25px; border-radius: 8px; margin-bottom: 25px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .bj-report-box p { color: #333 !important; font-size: 15px; margin-bottom: 8px; }
    .alerta-roja { background-color: #ffe6e6; color: #990000; border: 2px solid #ffcccc; padding: 15px; border-radius: 6px; margin-bottom: 20px; font-size: 15px; font-weight: 500; }
    .memo-alert { background-color: #ffe6e6; border: 2px solid #cc0000; color: #990000; padding: 15px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px; font-size: 16px; }
    .form-header-box { margin-bottom: 20px; border-bottom: 2px solid var(--bj-red); padding-bottom: 5px; }
    .legend-box { background-color: #fff8e1; border: 1px dashed #ffb300; padding: 10px; border-radius: 5px; margin-bottom: 15px; font-size: 13px; color: #5d4037; }
    .btn-gmail { display: inline-block; background-color: #DB4437; color: white !important; padding: 10px; border-radius: 5px; text-decoration: none; font-weight: bold; width: 100%; text-align: center; margin-bottom: 5px; }
    .btn-wa { display: inline-block; background-color: #25D366; color: white !important; padding: 10px; border-radius: 5px; text-decoration: none; font-weight: bold; width: 100%; text-align: center; margin-bottom: 5px; }
    .bj-footer { font-size: 12px; color: #888; text-align: center; margin-top: 50px; border-top: 1px solid #eee; padding-top: 10px; }
    .m-title { color: #990000 !important; font-weight: bold !important; font-size: 15px; margin-bottom: 5px; margin-top: 10px; }
</style>
""", unsafe_allow_html=True)

# --- 4. BASE DE DATOS LOCAL ---
def get_connection():
    return sqlite3.connect(DB_NAME)

@st.cache_data(ttl=600)
def obtener_empleados():
    try:
        creds = get_google_credentials()
        cliente = gspread.authorize(creds)
        hoja = cliente.open_by_key(SPREADSHEET_ID).worksheet("Empleados")
        
        valores = hoja.get_all_values()
        
        if not valores or len(valores) < 2: # Si está vacío o solo tiene títulos
            return pd.DataFrame()

        # --- LA SOLUCIÓN ---
        # valores[0] son los títulos del Excel (los ignoramos)
        # valores[1:] son los empleados reales (Andrea, Juan, Fernando...)
        columnas_fijas = ["NOMBRE", "ÁREA", "ROL", "CORREO", "WHATSAPP"]
        
        df = pd.DataFrame(valores[1:], columns=columnas_fijas)
        
        # Limpiamos espacios
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            
        return df
    except Exception as e:
        st.error(f"❌ Error al leer empleados: {e}")
        return pd.DataFrame()

def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS reportes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empleado_nombre TEXT,
        empleado_area TEXT,
        empleado_correo TEXT,
        empleado_wa TEXT,
        emisor TEXT,
        descripcion_falta TEXT,
        fecha_emision TIMESTAMP,
        estado TEXT DEFAULT 'Pendiente',
        analisis_causa TEXT,
        plan_accion TEXT,
        fecha_cierre TIMESTAMP
    )""")
    conn.commit()
    conn.close()

init_db()
df_empleados = obtener_empleados()

# =========================================================
# 5. MOTOR PDF
# =========================================================
MAPEO_SELLOS = {
    "ADMINISTRACIÓN": "S_ADMIN.PNG", "COMERCIAL": "S_COMERCIAL.PNG",
    "COMERCIO EXTERIOR": "S_COMERCIOEXTERIOR.PNG", "CONTABILIDAD": "S_CONTA.PNG",
    "EQUIPOS": "S_EQUIPOS.PNG", "GERENCIA": "S_GERENCIA.PNG",
    "GESTIÓN DE PROCESOS": "S_GESTIONDEPROCESOS.PNG", "INGENIERÍA": "S_INGENIERIA.PNG",
    "LOGÍSTICA": "S_LOGISTICA.PNG", "OBRAS CIVILES": "S_OBRASCIVILES.PNG",
    "PRODUCCIÓN": "S_PRODU.PNG", "RECURSOS HUMANOS": "S_RRHH.PNG", "SSOMA": "S_SSOMA.PNG"
}

MAPEO_CODIGOS = {
    "ADMINISTRACIÓN": "ADM", "COMERCIAL": "COM", "CONTABILIDAD": "CON",
    "EQUIPOS": "EQ", "INGENIERÍA": "ING", "LOGÍSTICA": "LOG",
    "PRODUCCIÓN": "PROD", "GESTIÓN DE PROCESOS": "GP", "RECURSOS HUMANOS": "RRHH"
}

class PDF_BJ(FPDF):
    def __init__(self, area_nombre):
        super().__init__(orientation='P', unit='mm', format='A4')
        if not hasattr(self, 'unifontsubset'):
            self.unifontsubset = False
        self.area_nombre = area_nombre
        self.set_auto_page_break(auto=True, margin=65)

    def header(self):
        self.set_font('Arial', '', 9)
        self.set_margins(30, 25, 30)
        self.set_xy(30, 25)
        self.cell(40, 25, "", border=1, align='C')
        if os.path.exists(LOGO_PATH):
            self.image(LOGO_PATH, x=32, y=27, w=36)

        self.set_xy(70, 25)
        self.set_font('Arial', 'B', 11)
        self.cell(70, 12.5, "REPORTE DE INCIDENCIA", border=1, align='C')

        area_upper = str(self.area_nombre).strip().upper()
        codigo_area = MAPEO_CODIGOS.get(area_upper, "GP")
        codigo_doc = f"BJ-REG-{codigo_area}-SGC-01"
        version_doc = f"01-{datetime.now(ZoneInfo("America/Lima")).year}"

        self.set_xy(140, 25)
        self.set_font('Arial', '', 8)
        self.cell(40, 6.25, f"Código: {codigo_doc}", border=1, align='L')
        self.set_xy(140, 31.25)
        self.cell(40, 6.25, f"Versión: {version_doc}", border=1, align='L')

        self.set_xy(70, 37.5)
        self.set_font('Arial', '', 9)
        self.cell(70, 12.5, f"Área: {self.area_nombre}", border=1, align='C')

        self.set_xy(140, 37.5)
        self.set_font('Arial', '', 8)
        self.cell(40, 6.25, f"Fecha: {datetime.now(ZoneInfo("America/Lima")).strftime('%d/%m/%Y')}", border=1, align='L')
        self.set_xy(140, 43.75)
        self.cell(40, 6.25, f"Página: {self.page_no()}", border=1, align='L')
        self.ln(12)

    def footer(self):
        self.set_y(-55)
        w_col = 37.5

        self.set_font('Arial', 'B', 7)
        self.set_x(30)
        self.cell(w_col, 5, "Elaborado por:", border=1, align='C')
        self.cell(w_col, 5, "Revisado por:", border=1, align='C')
        self.cell(w_col, 5, "Aprobado por:", border=1, align='C')
        self.cell(w_col, 5, "Fecha de aprobación:", border=1, align='C')
        self.ln(5)

        y_sellos = self.get_y()
        self.set_x(30)
        self.cell(w_col, 20, "", border=1)
        self.cell(w_col, 20, "", border=1)
        self.cell(w_col, 20, "", border=1)

        self.set_font('Arial', '', 8)
        self.cell(w_col, 20, f"{datetime.now(ZoneInfo("America/Lima")).strftime('%d/%m/%Y')}", border=1, align='C')
        self.ln(20)

        area_upper = str(self.area_nombre).strip().upper()
        sello_file = MAPEO_SELLOS.get(area_upper, "S_GESTIONDEPROCESOS.PNG")
        sello_path = os.path.join(PATH_SELLOS, sello_file)
        s_gerencia = os.path.join(PATH_SELLOS, "S_GERENCIA.PNG")

        if os.path.exists(sello_path):
            self.image(sello_path, 30 + 2, y_sellos + 1, 33.5)
            self.image(sello_path, 30 + w_col + 2, y_sellos + 1, 33.5)
        if os.path.exists(s_gerencia):
            self.image(s_gerencia, 30 + (w_col * 2) + 2, y_sellos + 1, 33.5)

        self.set_font('Arial', '', 7)
        self.set_x(30)
        self.cell(w_col, 5, "Jefe de área", border=1, align='C')
        self.cell(w_col, 5, "Jefe de área", border=1, align='C')
        self.cell(w_col, 5, "Gerente general", border=1, align='C')
        self.cell(w_col, 5, "", border=1, align='C')

def generar_pdf_oficial(rep):
    def clean(txt): return str(txt).encode('latin-1', 'replace').decode('latin-1')

    pdf = PDF_BJ(rep['empleado_area'])
    pdf.alias_nb_pages()
    pdf.add_page()

    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 8, "1. INFORMACIÓN DEL REPORTE", ln=1)
    pdf.set_font('Arial', '', 9)

    info = (f"Colaborador: {rep['empleado_nombre']}\n"
            f"Emitido por: {rep['emisor']}\n"
            f"Fecha de Emisión: {rep['fecha_emision']}\n"
            f"Descripción de la falta: {rep['descripcion_falta']}")
    pdf.multi_cell(0, 6, clean(info), border=1, align='J')

    pdf.ln(4)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 8, "2. ANÁLISIS DE CAUSA (UNIFICADO 6M + 5W)", ln=1)
    pdf.set_font('Arial', '', 9)
    pdf.multi_cell(0, 5, clean(rep['analisis_causa']).replace("|", "\n"), border=1, align='J')

    pdf.ln(4)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 8, "3. PLAN DE ACCIÓN", ln=1)
    pdf.set_font('Arial', '', 9)
    pdf.multi_cell(0, 5, clean(rep['plan_accion']), border=1, align='J')

    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf", prefix=f"Reporte_BJ_{rep['id']}_")
    pdf.output(tmp.name)
    return tmp.name

def link_gmail(dest, asunto, cuerpo):
    return f"https://mail.google.com/mail/?view=cm&fs=1&to={dest}&su={urllib.parse.quote(asunto)}&body={urllib.parse.quote(cuerpo)}"

def link_wa(num, msg):
    return f"https://wa.me/51{num}?text={urllib.parse.quote(msg)}"

# =========================================================
# LÓGICA PRINCIPAL
# =========================================================
query_params = st.query_params
ro_id = query_params.get("ro_id", None)
area_reporte = query_params.get("area", "GENERAL")

if ro_id:
    # --- FASE 2: VISTA DEL COLABORADOR ---
    col1, col2 = st.columns([1, 5])
    with col1:
        if os.path.exists(LOGO_PATH): st.image(LOGO_PATH, width=120)
    with col2:
        st.markdown("### BATALLA DE JUNIN S.A.C.")
        st.caption("Sistema de Gestión de Incidencias (RI)")

    conn = get_connection()
    df = pd.read_sql_query(f"SELECT * FROM reportes WHERE id = {ro_id}", conn)
    conn.close()

    if df.empty:
        st.error("Reporte no encontrado.")
    elif df.iloc[0]['estado'] == 'Resuelto':
        rep = df.iloc[0]
        st.success("✅ Reporte Cerrado Exitosamente.")
        
        # 1. IDENTIFICAR AL JEFE DEL ÁREA DEL COLABORADOR
        area_del_reportado = rep['empleado_area']
        
        try:
            # Buscamos en el DataFrame de empleados al Jefe de esa área
            info_jefe = df_empleados[
                (df_empleados['ÁREA'] == area_del_reportado) & 
                (df_empleados['ROL'].astype(str).str.strip().str.capitalize() == 'Jefe')
            ].iloc[0]
            
            correo_jefe = info_jefe['CORREO']
            nombre_jefe = info_jefe['NOMBRE']
        except:
            # Si no hay jefe configurado, enviamos al correo del colaborador como respaldo
            correo_jefe = "reportedeincidenciasinternas@gmail.com" # O podrías dejarlo vacío
            nombre_jefe = "Jefe de Área"

        pdf_path = generar_pdf_oficial(rep)
        with open(pdf_path, "rb") as f:
            st.download_button("📥 Descargar Reporte PDF (ISO BJ)", f, file_name=f"Reporte_BJ_{rep['id']}.pdf")
        
        # 2. CONFIGURAR EL CORREO DINÁMICO
        asunto_g = f"REPORTE DE INCIDENCIA FINALIZADO - #{ro_id} - {rep['empleado_nombre']}"
        cuerpo_g = f"Hola {nombre_jefe},\n\nSe informa que el colaborador {rep['empleado_nombre']} ha finalizado el análisis de causa raíz para el reporte RI-{ro_id}.\n\nAtentamente,\nSistema de Gestión SGC"
        
        col_g, _ = st.columns(2)
        # Aquí se reemplaza el correo estático por la variable 'correo_jefe'
        col_g.markdown(f'<a href="{link_gmail(correo_jefe, asunto_g, cuerpo_g)}" target="_blank" class="btn-gmail">📧 NOTIFICAR A JEFE ({area_del_reportado})</a>', unsafe_allow_html=True)
   
    # 3. SI ESTÁ PENDIENTE: MOSTRAR FORMULARIO
    else:
        rep = df.iloc[0]
        fecha_emision = pd.to_datetime(rep['fecha_emision']).replace(tzinfo=ZoneInfo("America/Lima"))
        fecha_actual = datetime.now(ZoneInfo("America/Lima"))
        diferencia = fecha_actual - fecha_emision

        # Si han pasado más de 3 días (72 horas)
        if diferencia.days >= 3:
            st.error(f"⚠️ **ACCESO BLOQUEADO:** El plazo para responder este reporte (3 días) ha vencido.")
            st.warning(f"Este reporte fue emitido el {fecha_emision.strftime('%d/%m/%Y %H:%M')}. Por favor, comuníquese con su jefe directo para regularizar su situación.")
            st.stop() # Detiene la ejecución para que no vea el formulario

        fecha_display = fecha_emision.strftime("%Y-%m-%d Hora: %H:%M")
        
        st.markdown(f"""
        <div class="bj-report-box">
            <h3 style="margin-top:0;">⚠️ REPORTE DE INCIDENCIA #{ro_id}</h3>
            <p><strong>Falta Reportada:</strong> {rep['descripcion_falta']}</p>
            <p><strong>Emitido por:</strong> {rep['emisor']}</p>
            <p><strong>Fecha: {fecha_display}</strong></p>
            <div style="background-color: #fff3cd; padding: 10px; border-radius: 5px; margin-top: 15px; border: 1px solid #ffeeba;">
                <small style="color: #856404; font-weight: bold;">Estimado colaborador: Es obligatorio completar el Diagrama 6M y minimo 3 Porqués para cerrar el caso.</small>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("##### 1. Análisis de Causas – Diagrama de Ishikawa (6M)")
        st.markdown("""
        <div class="legend-box">
            <strong>📌 GUÍA DE LLENADO:</strong> Marque con una <strong>[X]</strong> las categorías que <strong>APLICAN</strong> al incidente.
        </div>
        """, unsafe_allow_html=True)

        def m_unified_selector(label, guide, key):
            st.markdown(f"<div class='m-title'>{label}</div>", unsafe_allow_html=True)
            aplica = st.checkbox("¿Aplica?", key=f"aplica_{key}")
            if aplica:
                c1, c2 = st.columns([1, 1.2])
                with c1:
                    desc = st.text_area(f"Detalle {label}", placeholder=guide, key=f"txt_{key}", height=220)
                with c2:
                    st.markdown("<div style='color:#333; font-weight:bold; margin-bottom:10px;'>- Análisis de los 5 Porqués</div>", unsafe_allow_html=True)
                    p1 = st.text_area("P1. ¿Por qué ocurrió el problema?", key=f"p1_{key}", placeholder="Causa directa...", height=100)
                    p2 = st.text_area("P2. ¿Por qué ocurrió lo descrito en el punto 1?", key=f"p2_{key}", height=100)
                    p3 = st.text_area("P3. ¿Por qué ocurrió lo descrito en el punto 2?", key=f"p3_{key}", height=100)
                    p4 = st.text_area("P4. ¿Por qué ocurrió lo descrito en el punto 3?", key=f"p4_{key}", height=100)
                    st.markdown("<div style='color:#990000; font-weight:bold; font-size:12px;'>🔻 POSIBLE CAUSA RAÍZ</div>", unsafe_allow_html=True)
                    p5 = st.text_area("P5. ¿Por qué? (Causa Raíz)", key=f"p5_{key}", placeholder="Defina raíz...", height=100)
                return {"desc": desc, "w": f"{p1}|{p2}|{p3}|{p4}|{p5}"}
            return None

        m1 = m_unified_selector("Mano de Obra", "¿Fatiga, capacitación, error humano?", "mo")
        m2 = m_unified_selector("Maquinaria", "¿Falla equipos, mantenimiento?", "mq")
        m3 = m_unified_selector("Materiales", "¿Insumos defectuosos, stock?", "mat")
        m4 = m_unified_selector("Método", "¿Procedimiento incorrecto/inexistente?", "met")
        m5 = m_unified_selector("Medición", "¿Datos erróneos, indicadores?", "med")
        m6 = m_unified_selector("Medio Ambiente", "¿Clima, ruido, espacio, luz?", "amb")

        st.markdown("---")
        st.markdown("##### 2. Plan de Acción")
        accion = st.text_area("COMPROMISO DE CORRECCIÓN", placeholder="Describa acciones correctivas y preventivas (Mín. 40 caracteres).", height=150)

        if st.button("REGISTRAR Y CERRAR REPORTE", key="btn_close"):
            resultados = [r for r in [m1, m2, m3, m4, m5, m6] if r is not None]
            if not resultados:
                st.error("❌ Seleccione al menos una categoría.")
            elif len(accion) < 40:
                st.error("❌ Plan de acción muy corto.")
            else:
                anal_db = ""
                lbls = ["MO", "MQ", "MAT", "MET", "MED", "AMB"]
                nombres_6m = ["Mano de Obra", "Maquinaria", "Materiales", "Método", "Medición", "Medio Ambiente"]
                categorias_afectadas = []
                causas_raices = []

                for i, r in enumerate([m1, m2, m3, m4, m5, m6]):
                    if r:
                        anal_db += f"[{lbls[i]}]: {r['desc']} | 5W: {r['w']} || "
                        categorias_afectadas.append(nombres_6m[i])
                        quinto_porque = r['w'].split('|')[-1].strip()
                        if quinto_porque:
                            causas_raices.append(f"{lbls[i]}: {quinto_porque}")

                dt_cierre = datetime.now(ZoneInfo("America/Lima"))
                fecha_cierre_str = dt_cierre.strftime('%d/%m/%Y')
                hora_cierre_str = dt_cierre.strftime('%H:%M:%S')
                fecha_cierre_full = str(dt_cierre)

                conn = get_connection()
                conn.execute(
                    "UPDATE reportes SET estado='Resuelto', analisis_causa=?, plan_accion=?, fecha_cierre=? WHERE id=?",
                    (anal_db, accion, fecha_cierre_full, ro_id)
                )
                conn.commit()
                conn.close()

                cat_str = ", ".join(categorias_afectadas)
                raiz_str = " / ".join(causas_raices)

                datos_fase_2 = [
                    cat_str,        # H: Categoría de Falla (6M)
                    raiz_str,       # I: Causa Raíz
                    str(accion),    # J: Plan de Acción
                    "Resuelto",     # K: Estado
                    fecha_cierre_str,  # L: Fecha de Cierre
                    hora_cierre_str    # M: Hora de Cierre
                ]

                ok = actualizar_en_sheets(ro_id, datos_fase_2, area_reporte)
                if ok:
                    st.success("✅ Guardado en Sheets")
                else:
                    st.error("❌ Falló Sheets")
                st.balloons()
                import time; time.sleep(3)
                st.rerun()

else:
    login_screen()

    # --- FASE 1: VISTA DEL JEFE (MODIFICADA) ---
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if os.path.exists(LOGO_PATH): st.image(LOGO_PATH, width=300)
        else: st.title("BATALLA DE JUNIN")

    st.markdown("<h2 style='text-align:center'>RI – Reporte de Incidencias Internas</h2>", unsafe_allow_html=True)

    if st.session_state.user_role == "jefe":
        t_emitir, t_stats = st.tabs(["📄 PAPELETA RI", "📊 ESTADÍSTICAS"])
    else:
        t_stats = st.tabs(["📊 ESTADÍSTICAS"])[0]
        t_emitir = None

    if t_emitir:
        with t_emitir:
            # 1. Recuperamos automáticamente los datos del jefe que inició sesión
            jefe_actual = st.session_state.user_data
            emisor = jefe_actual['NOMBRE']
            area_jefe = jefe_actual['ÁREA']

            st.markdown(f'<div class="form-header-box"><h4>Generar Reporte - Área: {area_jefe}</h4></div>', unsafe_allow_html=True)
            st.info(f"Sesión iniciada como: **{emisor}**")
            
            # --- PREPARACIÓN DE DATOS ---
            df_empleados.columns = [str(c).strip().upper() for c in df_empleados.columns]

            # 2. FILTRADO DINÁMICO: Solo integrantes del equipo de SU misma área
            equipo = df_empleados[
                (df_empleados['ROL'].astype(str).str.strip().str.capitalize() == 'Equipo') & 
                (df_empleados['ÁREA'] == area_jefe)
            ]['NOMBRE'].tolist()

            # Selector de receptor (solo su equipo)
            receptor = st.selectbox("¿A quién se reporta? (Personal de su área)", equipo if equipo else ["Sin personal"])

            # FORMULARIO PARA LA ACCIÓN DE GUARDAR
            with st.form("emision_final"):
                desc = st.text_area("Descripción de la Incidencia:", height=120)
                submit = st.form_submit_button("GENERAR PAPELETA")

                if submit:
                    if not equipo or receptor == "Sin personal":
                        st.error("❌ No se puede generar el reporte sin un receptor válido en su área.")
                    elif len(desc) >= 20:
                        # Buscamos los datos del receptor seleccionado
                        row_rec = df_empleados[df_empleados['NOMBRE'] == receptor].iloc[0]
                        area_receptor = row_rec.get('ÁREA', area_jefe)
                        dt_emision = datetime.now(ZoneInfo("America/Lima"))

                        # --- GUARDADO EN BASE DE DATOS LOCAL ---
                        conn = get_connection()
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO reportes (empleado_nombre, empleado_area, empleado_correo, empleado_wa, emisor, descripcion_falta, fecha_emision) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (receptor, area_receptor, 
                            row_rec.get('CORREO', 'N/A'), 
                            row_rec.get('WHATSAPP', 'N/A'), 
                            emisor, desc, dt_emision)
                        )
                        conn.commit()
                        last_id = cur.lastrowid
                        conn.close()

                        # --- GUARDADO EN GOOGLE SHEETS ---
                        fila_fase_1 = [f"RI-{int(last_id):03d}", dt_emision.strftime('%d/%m/%Y'), dt_emision.strftime('%H:%M:%S'), str(area_receptor), str(receptor), str(emisor), str(desc), "", "", "", "Pendiente", "", ""]
                        guardar_en_sheets(fila_fase_1, area_receptor)

                        # --- GENERACIÓN DE LINK Y NOTIFICACIONES ---
                        app_url = st.secrets.get("APP_URL", "http://localhost:8501")
                        link = f"{app_url}/?ro_id={last_id}&area={urllib.parse.quote(str(area_receptor))}"
                        
                        st.success(f"✅ Papeleta RI Generada Exitosamente")
                        st.code(link)
                        
                        col_g, col_w = st.columns(2)
                        col_g.markdown(f'<a href="{link_gmail(row_rec.get("CORREO",""), f"RI #{last_id}", f"Hola, se ha generado un reporte. Completa el análisis aquí: {link}")}" target="_blank" class="btn-gmail">📧 Enviar por Gmail</a>', unsafe_allow_html=True)
                        col_w.markdown(f'<a href="{link_wa(row_rec.get("WHATSAPP",""), f"Hola, tienes un RI pendiente por completar: {link}")}" target="_blank" class="btn-wa">💬 Enviar por WhatsApp</a>', unsafe_allow_html=True)
                    else:
                        st.error("❌ Descripción muy corta (mínimo 20 caracteres).")

    with t_stats:
        st.markdown('<div class="form-header-box"><h4>Panel de Estadísticas por Área</h4></div>', unsafe_allow_html=True)
        try:
            creds = get_google_credentials()
            cliente = gspread.authorize(creds)
            
            # 1. Obtener y Normalizar el área del Jefe logueado
            import unicodedata
            def limpiar_texto(t):
                if not t: return ""
                return ''.join(c for c in unicodedata.normalize('NFD', str(t)) 
                               if unicodedata.category(c) != 'Mn').upper().strip()

            jefe_actual = st.session_state.user_data
            area_buscada = limpiar_texto(jefe_actual['ÁREA'])
            
            st.info(f"📊 Mostrando reportes para el área: **{jefe_actual['ÁREA']}**")

            # 2. Buscar el ID del Sheet en tu diccionario IDS_POR_AREA
            sheet_id = None
            for nombre_area_dict, id_fijo in IDS_POR_AREA.items():
                if limpiar_texto(nombre_area_dict) == area_buscada:
                    sheet_id = id_fijo
                    break

            if sheet_id:
                ss = cliente.open_by_key(sheet_id)
                hoja = ss.get_worksheet(0)
                datos_raw = hoja.get_all_records()
                
                if datos_raw:
                    # CONVERSIÓN CRÍTICA: Forzamos DataFrame desde el inicio
                    df_stats = pd.DataFrame(datos_raw)
                    # Limpiamos los nombres de las columnas (quita espacios y tildes en los encabezados)
                    df_stats.columns = [limpiar_texto(c) for c in df_stats.columns]

                    # 3. IDENTIFICAR COLUMNAS POR PALABRAS CLAVE (No por nombre exacto)
                    def buscar_col(lista, palabra):
                        return next((c for c in lista if palabra in c), None)

                    c_colab = buscar_col(df_stats.columns, "COLABORADOR")
                    c_estado = buscar_col(df_stats.columns, "ESTADO")
                    c_id = buscar_col(df_stats.columns, "ID")
                    c_area = buscar_col(df_stats.columns, "AREA")

                    if c_colab and c_estado:
                        # Limpiar filas donde el colaborador esté vacío
                        df_stats = df_stats[df_stats[c_colab].astype(str).str.strip() != ""]
                        
                        # 4. AGRUPACIÓN SEGURA (Usando .agg para evitar errores de listas)
                        resumen = df_stats.groupby([c_colab, c_area if c_area else c_colab]).agg(
                            Total_RI=(c_id if c_id else c_colab, 'count'),
                            Resueltos=(c_estado, lambda x: x.astype(str).str.upper().str.contains("RESUELTO").sum())
                        ).reset_index()

                        # Renombrar columnas para la vista del usuario
                        resumen.columns = ["Colaborador", "Área/Cargo", "Total RI", "RI Respondidas"]
                        resumen = resumen.sort_values(by="Total RI", ascending=False)

                        # 5. APLICAR ESTILOS Y MOSTRAR
                        def resaltar_criticos(s):
                            is_critico = s["Total RI"] >= 3
                            return ['background-color: #ffe6e6; color: #990000; font-weight: bold' if is_critico else '' for _ in s]

                        st.dataframe(resumen.style.apply(resaltar_criticos, axis=1), use_container_width=True)

                        # Alertas Legales
                        for _, fila in resumen[resumen["Total RI"] >= 3].iterrows():
                            st.error(f"🚨 **MEMORÁNDUM REQUERIDO:** {fila['Colaborador']} alcanzó {fila['Total RI']} incidencias.")
                    else:
                        st.warning(f"⚠️ El archivo de {jefe_actual['ÁREA']} no tiene las columnas 'COLABORADOR' o 'ESTADO'.")
                else:
                    st.info("Aún no hay reportes registrados en esta área.")
            else:
                st.error(f"❌ Error de Configuración: El área '{jefe_actual['ÁREA']}' no se encontró en la lista de IDs del sistema.")

        except Exception as e:
            st.error(f"Error técnico al cargar estadísticas: {str(e)}")

st.markdown("<div class='bj-footer'>Batalla de Junin S.A.C. © 2026</div>", unsafe_allow_html=True)
