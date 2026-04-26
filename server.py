import os
import json
import math
import time
import concurrent.futures
import hashlib
import requests
from flask import Flask, request, jsonify, render_template_string
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta, timezone


app = Flask(__name__)

# Cache-Control Headers for all responses (or specific ones)
@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

# --- CONFIGURACIÓN Y VARIABLES DE ENTORNO ---
# La variable de entorno GOOGLE_MAPS_API_KEY ya no es crítica
# porque la clave se usa directamente en la función get_google_distance.
GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')


# --- FIREBASE SETUP ---
cred_val = os.environ.get('FIREBASE_CREDENTIALS')
db = None

# Lista de rutas posibles para buscar las credenciales si no hay variable de entorno
POSSIBLE_CRED_PATHS = [
    '/root/bot-indrive/serviceAccountKey.json',
    './serviceAccountKey.json',
    'serviceAccountKey.json',
]

# Si no hay variable, buscar en rutas
if not cred_val:
    for path in POSSIBLE_CRED_PATHS:
        if os.path.exists(path):
            print(f"🔍 Credenciales encontradas automáticamente en: {path}")
            cred_val = path
            break

if cred_val:
    try:
        # Detectar si es una ruta de archivo o un JSON en texto
        if os.path.exists(cred_val):
            # Es una ruta de archivo
            cred = credentials.Certificate(cred_val)
            print(f"📂 Cargando credenciales desde archivo: {cred_val}")
        else:
            # Es JSON en texto (fallback)
            cred_dict = json.loads(cred_val)
            cred = credentials.Certificate(cred_dict)
            print("📝 Cargando credenciales desde texto JSON")

        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase Conectado Exitosamente")
    except Exception as e:
        print(f"❌ Error crítico Firebase: {e}")
        db = None
else:
    print("⚠️ WARNING: No se encontraron credenciales de Firebase (Variable ni Archivos). Usando memoria local.")


# --- SEGURIDAD ---
# Secret Key compartida con la App para HMAC-SHA256
SECRET_KEY = "InDrive_Secure_Auth_v2_2026"

# --- CONFIGURACIÓN POR DEFECTO ---
DEFAULT_CONFIG = {
    "max_pickup_dist": 1.2,
    "max_dest_dist": 4.0,
    "min_dest_dist": 0.2,
    "min_price": 0,
    "max_price": 0,
    "base_price_km": 0,
    "fare_tiers": [],
    "min_rating": 4.5,
    "min_rides": 5,
    "reject_tags": False,
    "accept_stops": False,
    "accept_airport": False,
    "pickup_time_minutes": 3,
    "blocked_zones_enabled": True,
    "favorite_zones_enabled": False,
    "max_repeats": 2,
    "banned_words": [],
    "offer_max_if_exceeds": False,
    "audio_enabled": True,
    "show_info_card": True,
    "hourly_rate": 0,
    "time_selection": "travel",  # 'travel' (default) or 'total'
    "pricing_strategy": "km",    # 'km', 'time', 'mixed'
    "ui_mode": "basic",          # 'basic' or 'advanced'
    "price_rounding_factor": 1,  # Default to 1 (integer rounding)
    "whatsapp_url": "https://wa.me/573142999526" # URL por defecto para redirección
}

PROCESSED_ORDERS = {}
ORDER_TTL = 1  # 5 minutos

# --- SUGGESTED PROFILES DATA ---
SUGGESTED_PROFILES = [
    {
        "id": "rain_mode",
        "name": "Modo Lluvia 🌧️",
        "desc": "Tarifas más altas y distancias cortas.",
        "config": {
            "min_price": 5000,
            "max_pickup_dist": 1.5,
            "max_dest_dist": 5.0,
            "base_price_km": 1500, # Agresivo
            "offer_max_if_exceeds": True
        }
    },
    {
        "id": "short_trips",
        "name": "Solo Cortos ⚡",
        "desc": "Viajes rápidos, ideales para completar bonos.",
        "config": {
            "max_pickup_dist": 1.0,
            "max_dest_dist": 3.0,
            "min_rating": 4.8
        }
    },
    {
        "id": "god_mode",
        "name": "Modo Dios ⚡💵",
        "desc": "Solo las mejores tarifas y usuarios top.",
        "config": {
            "min_price": 8000,
            "min_rating": 4.9,
            "min_rides": 20,
            "reject_tags": True, # Estricto
            "base_price_km": 2000
        }
    }
]

def seed_suggested_configs():
    """Siembra las configuraciones sugeridas en Firestore si no existen"""
    if not db: return

    try:
        ref = db.collection('suggested_configs')
        # Check if empty (limit 1)
        if not next(ref.limit(1).stream(), None):
            print("🌱 Seeding suggested configs...", flush=True)
            batch = db.batch()
            for profile in SUGGESTED_PROFILES:
                doc_ref = ref.document(profile['id'])
                # Mezclar con default para tener estructura completa
                full_config = DEFAULT_CONFIG.copy()
                full_config.update(profile['config'])

                # Guardar metadatos de presentación separados o dentro?
                # Guardemos todo junto para facilitar el fetch
                save_data = full_config.copy()
                save_data['metadata'] = {
                    "name": profile['name'],
                    "desc": profile['desc']
                }
                batch.set(doc_ref, save_data)
            batch.commit()
            print("✅ Suggested configs seeded.")
    except Exception as e:
        print(f"⚠️ Error seeding suggestions: {e}")

# Llamar al inicio (o lazily, pero mejor al inicio si no bloquea mucho)
# Lo llamamos en el main block o antes de run


# --- CONSTANTS ---
TRIAL_DURATION_DAYS = 3

# --- CACHE EN MEMORIA (Optimización de Velocidad) ---
CACHE = {}
CACHE_TTL = 86400  # 2 Horas (Para actualizar rápido entre múltiples workers)


# --- FUNCIONES DE UTILIDAD GEOGRÁFICA ---

def distance_km(lat1, lon1, lat2, lon2):
    """Calcula distancia Haversine (línea recta) como fallback"""
    R = 6371  # Radio Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def point_in_circle(lat, lon, circle):
    """Verifica si punto está dentro de círculo"""
    try:
        center_lat = circle['center']['lat']
        center_lon = circle['center']['lon']
        radius_km = float(circle['radius_km'])
        dist = distance_km(lat, lon, center_lat, center_lon)
        return dist <= radius_km
    except Exception as e:
        return False

def get_osrm_distance(lat1, lon1, lat2, lon2):
    """Calcula distancia de ruta usando OSRM (OpenStreetMap)"""
    try:
        # OSRM expects lon,lat
        url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
        params = {"overview": "false"}

        # Disable retries for speed
        with requests.Session() as s:
            s.mount('http://', requests.adapters.HTTPAdapter(max_retries=0))
            response = s.get(url, params=params, timeout=1.0)

        data = response.json()

        if data.get("code") == "Ok":
            # distance is in meters
            dist_m = data['routes'][0]['distance']
            return dist_m / 1000.0, "OSRM"

        print(f"⚠️ OSRM Error: {data}")
        return None, "OSRM_ERR"
    except Exception as e:
        print(f"❌ Error calling OSRM: {e}")
        return None, f"OSRM_EXC_{str(e)[:15]}"

def point_in_polygon(lat, lon, polygon_points):
    """
    Ray-casting algorithm to validation if a point is inside a polygon.
    polygon_points: List of dicts [{'lat': x, 'lon': y}, ...] or lists [[lat, lon], ...]
    """
    try:
        inside = False
        n = len(polygon_points)
        p1x, p1y = 0, 0
        p2x, p2y = 0, 0

        # Normalize first point
        pt = polygon_points[0]
        if isinstance(pt, dict):
            p1x, p1y = float(pt['lat']), float(pt['lon'])
        else:
            p1x, p1y = float(pt[0]), float(pt[1])

        for i in range(n + 1):
            # Loop supports closing the polygon implicitly
            pt2 = polygon_points[i % n]
            if isinstance(pt2, dict):
                p2x, p2y = float(pt2['lat']), float(pt2['lon'])
            else:
                p2x, p2y = float(pt2[0]), float(pt2[1])

            if lon > min(p1y, p2y):
                if lon <= max(p1y, p2y):
                    if lat <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (lon - p1y) * (p1x - p2x) / (p1y - p2y) + p1x
                        if p1x == p2x or lat <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside
    except Exception as e:
        print(f"⚠️ Error in point_in_polygon: {e}")
        return False

def get_google_distance(lat1, lon1, lat2, lon2):
    """
    Calcula distancia real por calles usando Google Maps API.
    Utiliza la clave hardcodeada.
    """
    # 🛑 CLAVE API HARDCODEADA DIRECTAMENTE AQUÍ 🛑
    API_KEY = "AIzaSyAf_PYN1RDSft1TSOPQQC9qh0AeG5eJTrY"

    if not API_KEY:
        print("⚠️ Google Maps API Key is hardcoded but empty. Falling back to OSRM.")
        return get_osrm_distance(lat1, lon1, lat2, lon2)

    try:
        url = "https://maps.googleapis.com/maps/api/distancematrix/json"
        params = {
            "origins": f"{lat1},{lon1}",
            "destinations": f"{lat2},{lon2}",
            "key": API_KEY,  # <--- ¡CLAVE APLICADA!
            "mode": "driving"
        }

        # Timeout corto para no bloquear el bot
        # REDUCIDO A 0.5s para Fail-Fast
        response = requests.get(url, params=params, timeout=0.5)
        data = response.json()

        if data['status'] == 'OK':
            element = data['rows'][0]['elements'][0]
            if element['status'] == 'OK':
                val = element['distance']['value'] / 1000.0
                return val, "GOOGLE"

        # Si la respuesta de Google no es 'OK' (ej. REQUEST_DENIED)
        status = data.get('status', 'UNKNOWN')
        return get_osrm_distance(lat1, lon1, lat2, lon2)

    except Exception as e:
        # print(f"❌ Google Maps Fail/Timeout: {e}")
        return get_osrm_distance(lat1, lon1, lat2, lon2)

# --- GESTIÓN DE CONFIGURACIÓN Y ZONAS ---

def get_signal_file(user_id):
    return f"config_signal_{user_id}.txt"

def bump_signal(user_id):
    """Marca que la configuración ha cambiado para este usuario"""
    try:
        with open(get_signal_file(user_id), 'w') as f:
            f.write(str(time.time()))
    except Exception as e:
        print(f"⚠️ Error updating signal file: {e}")

def get_signal_ts(user_id):
    """Obtiene la última vez que se modificó la configuración (timestamp)"""
    try:
        return os.path.getmtime(get_signal_file(user_id))
    except:
        return 0

def cleanup_processed_orders():
    current_time = time.time()
    to_remove = [oid for oid, data in PROCESSED_ORDERS.items() if current_time - data['ts'] > ORDER_TTL]
    for oid in to_remove:
        del PROCESSED_ORDERS[oid]


def invalidate_cache(user_id):
    """Fuerza la invalidación de caché para recargar configuración y zonas"""
    if user_id in CACHE:
        del CACHE[user_id]
        # print(f"🧹 Cache invalidated for {user_id}", flush=True)

# Lista de campos que SON metadatos y DEBEN quedarse en el root
ROOT_METADATA_FIELDS = [
    'active_profile_id',
    'membership_status',
    'membership_expires_at',
    'first_seen_at',
    'email', # A veces se guarda
    'phone'  # A veces se guarda
]

# Función clean_root_document eliminada por optimización (ya no es necesaria)


def load_config(user_id, force_remote=False):
    # 0. Check Signal (Sync Workers)
    signal_ts = get_signal_ts(user_id)

    if user_id in CACHE:
        if signal_ts > CACHE[user_id]['ts']:
            force_remote = True

    # 1. Check Cache
    now = time.time()
    if not force_remote and user_id in CACHE and (now - CACHE[user_id]['ts'] < CACHE_TTL):
        return CACHE[user_id]['config']

    # 2. Load from DB (Subcollections Strategy)
    config = DEFAULT_CONFIG.copy()

    # Valores por defecto de metadatos
    active_profile_id = 'Mi Perfil'
    root_data = {}

    if db:
        try:
            # A. Leer Root Document (Solo Metadatos)
            root_ref = db.collection('bot_configs').document(user_id)
            root_doc = root_ref.get()

            if root_doc.exists:
                root_data = root_doc.to_dict()
                active_profile_id = root_data.get('active_profile_id', 'Mi Perfil')



            # C. Cargar Configuración del Perfil Activo
            profile_doc = root_ref.collection('profile_config').document(active_profile_id).get()

            if profile_doc.exists:
                config.update(profile_doc.to_dict())
            else:
                # Si el perfil activo apunta a algo que no existe, fallback a default o crear
                if active_profile_id != 'Mi Perfil':
                    print(f"⚠️ Active profile '{active_profile_id}' not found. Fallback to default.")
                    active_profile_id = 'Mi Perfil'
                    # Intentar cargar default
                    profile_doc = root_ref.collection('profile_config').document('Mi Perfil').get()
                    if profile_doc.exists:
                        config.update(profile_doc.to_dict())

            # D. Mezclar con Metadatos del Root (Membresía, etc explícitamente sobreescribe config)
            # Esto es importante porque check_membership escribe en config, pero queremos que persista
            # la info de membresía. AUNQUE, check_membership guarda en config.
            # Idealmente membership data vive en root.
            if 'membership_status' in root_data: config['membership_status'] = root_data['membership_status']
            if 'membership_expires_at' in root_data: config['membership_expires_at'] = root_data['membership_expires_at']
            if 'first_seen_at' in root_data: config['first_seen_at'] = root_data['first_seen_at']

            # Inyectar ID de perfil activo para el frontend
            config['active_profile_id'] = active_profile_id

        except Exception as e:
            print(f"❌ Error loading config: {e}", flush=True)

    # 3. Save to Cache
    if user_id not in CACHE: CACHE[user_id] = {'ts': 0, 'config': {}, 'zones': []}
    
    # Si estamos forzando recarga remota, invalidar también las zonas para que se recarguen
    if force_remote:
        CACHE[user_id].pop('zones_ts', None)
        CACHE[user_id].pop('fav_zones_ts', None)

    CACHE[user_id]['config'] = config
    CACHE[user_id]['ts'] = now if now > signal_ts else signal_ts

    return config

def save_config(user_id, config):
    # 1. Update DB (Persistent)
    saved_to_db = False
    if db:
        try:
            print(f"Attemping to save config for {user_id}...", flush=True)

            root_ref = db.collection('bot_configs').document(user_id)

            # Separar Metadatos (Root) de Configuración (Subcolección)
            root_updates = {}
            profile_updates = config.copy()

            # Extraer active_profile_id
            active_profile_id = config.get('active_profile_id', 'Mi Perfil')

            # Extraer campos de metadatos para actualizar en root SOLO si están presentes
            # (No borrarlos de profile_updates porque el frontend los envía y no daña tenerlos duplicados en profile,
            # pero el source of truth de membresía debería ser root. Por limpieza, los quitamos de profile_updates)
            for meta_field in ROOT_METADATA_FIELDS:
                if meta_field in config:
                    root_updates[meta_field] = config[meta_field]
                    # Opcional: quitarlos del perfil para no duplicar data
                    # if meta_field != 'active_profile_id':
                    #    del profile_updates[meta_field]

            # A. Guardar en Subcolección
            print(f"  💾 Saving to profile: {active_profile_id}")
            root_ref.collection('profile_config').document(active_profile_id).set(profile_updates, merge=True)

            # B. Actualizar Metadatos en Root (si hay cambios)
            if root_updates:
                 root_ref.set(root_updates, merge=True)


            print(f"✅ Config saved for {user_id}", flush=True)
            saved_to_db = True
        except Exception as e:
            print(f"❌ Error saving config: {e}", flush=True)
    else:
        print("⚠️ Firebase DB not initialized (db is None). Saving only to memory.", flush=True)

    # 2. SIGNAL OTHER WORKERS
    # Esto crea/toca un archivo. TODOS los workers verán esto y recargarán.
    bump_signal(user_id)

    # 3. Update Cache Strategy (Local)
    if saved_to_db:
        invalidate_cache(user_id)
        # Recargar para asegurar que tenemos la version fusionada correcta en memoria
        load_config(user_id, force_remote=True)
    else:
        # Memory fallback
        now = time.time()
        if user_id not in CACHE:
            CACHE[user_id] = {'ts': 0, 'config': DEFAULT_CONFIG.copy(), 'zones': []}

        current_cfg = CACHE[user_id].get('config', {})
        current_cfg.update(config)
        CACHE[user_id]['config'] = current_cfg
        CACHE[user_id]['ts'] = now

def load_blocked_zones(user_id):
    # 1. Check Cache (Zones are updated along with config or lazily)
    # Since we don't track zone cache TS separately, we use the same entry but check if 'zones_loaded' timestamp exists?
    # Simpler: Just cache it when loaded.

    # We rely on load_config to set the TS. If TS is valid, we check if zones are cached.
    # Actually, let's just create a separate load logic but share the CACHE dict.

    # 0. Check Signal (Sync Workers)
    signal_ts = get_signal_ts(user_id)
    
    if user_id in CACHE and 'zones' in CACHE[user_id]:
        # Si la señal es más reciente que cuando cargamos las zonas, forzar recarga
        if signal_ts > CACHE[user_id].get('zones_ts', 0):
            pass # Proceder a recargar
        else:
            now = time.time()
            if now - CACHE[user_id].get('zones_ts', 0) < CACHE_TTL:
                return CACHE[user_id]['zones']

    zones = []
    zones = []
    if db:
        try:
            # DETERMINE ACTIVE PROFILE
            active_profile_id = 'Mi Perfil'
            # Try to get from cache first
            if user_id in CACHE and 'config' in CACHE[user_id] and 'active_profile_id' in CACHE[user_id]['config']:
                 active_profile_id = CACHE[user_id]['config']['active_profile_id']
            else:
                 # Fetch metadata from root
                 root = db.collection('bot_configs').document(user_id).get()
                 if root.exists:
                     active_profile_id = root.to_dict().get('active_profile_id', 'Mi Perfil')

            # Query Subcollection
            zones_ref = db.collection('bot_configs').document(user_id).collection('profile_config').document(active_profile_id).collection('blocked_zones')
            query = zones_ref.where('active', '==', True)
            docs = query.stream()
            for doc in docs:
                z = doc.to_dict()
                z['id'] = doc.id
                zones.append(z)
        except Exception as e:
            print(f"❌ Error loading zones: {e}")

    # Update Cache
    now = time.time()
    if user_id not in CACHE: CACHE[user_id] = {'ts': now, 'config': DEFAULT_CONFIG.copy()}
    CACHE[user_id]['zones'] = zones
    CACHE[user_id]['zones_ts'] = now

    return zones

def load_favorite_zones(user_id, force_refresh=False):
    # 0. Check Signal (Sync Workers)
    signal_ts = get_signal_ts(user_id)

    if not force_refresh and user_id in CACHE and 'favorite_zones' in CACHE[user_id]:
        if signal_ts > CACHE[user_id].get('fav_zones_ts', 0):
            pass # Proceder a recargar
        else:
            now = time.time()
            if now - CACHE[user_id].get('fav_zones_ts', 0) < CACHE_TTL:
                return CACHE[user_id]['favorite_zones']

    zones = []
    zones = []
    if db:
        try:
            # DETERMINE ACTIVE PROFILE
            active_profile_id = 'Mi Perfil'
            if user_id in CACHE and 'config' in CACHE[user_id] and 'active_profile_id' in CACHE[user_id]['config']:
                 active_profile_id = CACHE[user_id]['config']['active_profile_id']
            else:
                 root = db.collection('bot_configs').document(user_id).get()
                 if root.exists:
                     active_profile_id = root.to_dict().get('active_profile_id', 'Mi Perfil')

            # Query Subcollection
            zones_ref = db.collection('bot_configs').document(user_id).collection('profile_config').document(active_profile_id).collection('favorite_zones')
            query = zones_ref.where('active', '==', True)
            docs = query.stream()
            for doc in docs:
                z = doc.to_dict()
                z['id'] = doc.id
                zones.append(z)
        except Exception as e:
            print(f"❌ Error loading favorite zones: {e}")

    now = time.time()
    if user_id not in CACHE: CACHE[user_id] = {'ts': now, 'config': DEFAULT_CONFIG.copy()}
    CACHE[user_id]['favorite_zones'] = zones
    CACHE[user_id]['fav_zones_ts'] = now
    return zones

# --- MEMBRESIA & SEGURIDAD ---

def get_readable_time(seconds):
    days = int(seconds // (24 * 3600))
    hours = int((seconds % (24 * 3600)) // 3600)
    minutes = int((seconds % 3600) // 60)
    if days > 0: return f"{days}d {hours}h"
    return f"{hours}h {minutes}m"

def check_membership(user_id, config):
    """
    Verifica el estado de la membresía usando DATETIME (Firestore Timestamp).
    """
    now = datetime.now(timezone.utc)

    # Recuperar valor (puede ser None, float/int viejo, o datetime nuevo)
    expires_val = config.get('membership_expires_at')
    expires_at = None

    # Normalización a datetime
    if isinstance(expires_val, (int, float)):
        # Migración: Convertir timestamp unix a datetime utc
        expires_at = datetime.fromtimestamp(expires_val, timezone.utc)
        config['membership_expires_at'] = expires_at
        save_config(user_id, config) # Persistir conversión
    elif isinstance(expires_val, datetime):
        expires_at = expires_val

    # 1. Inicialización de usuario nuevo (TRIAL)
    if not expires_at:
        expires_at = now + timedelta(days=TRIAL_DURATION_DAYS)
        config['membership_expires_at'] = expires_at
        config['membership_status'] = 'TRIAL'
        config['first_seen_at'] = now
        save_config(user_id, config)

        remaining = (expires_at - now).total_seconds()
        return {
            "status": "TRIAL",
            "can_operate": True,
            "expires_at": expires_at.isoformat(),
            "message": f"Trial Activo: {get_readable_time(remaining)} restantes"
        }

    # 2. Verificación de Expiración
    status = config.get('membership_status', 'TRIAL')

    # Asegurar que expire_at tenga timezone para comparar con now
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if now > expires_at:
        if status != 'EXPIRED':
            config['membership_status'] = 'EXPIRED'
            save_config(user_id, config)

        return {
            "status": "EXPIRED",
            "can_operate": False,
            "expires_at": expires_at.isoformat(),
            "message": "TU PLAN HA VENCIDO. Renueva para continuar."
        }

    # 3. Usuario Activo
    remaining = (expires_at - now).total_seconds()
    return {
        "status": status,
        "can_operate": True,
        "expires_at": expires_at.isoformat(),
        "message": f"{status}: {get_readable_time(remaining)} restantes"
    }

# --- MOTOR LÓGICO PRINCIPAL ---

def evaluate_order(order, config):
    reasons = []
    analysis = []
    dist_data = {"a": 0, "b": 0}

    # 1. VALIDACIÓN DE COORDENADAS (CRÍTICO)
    # Ignoramos order['dist_a'] y order['dist_b'] intencionalmente
    coords_driver = order.get('coords_driver', {})
    coords_pickup = order.get('coords_a', {})
    coords_dest = order.get('coords_b', {})

    has_pickup = coords_pickup and coords_pickup.get('lat') is not None and coords_pickup.get('lon') is not None
    has_dest = coords_dest and coords_dest.get('lat') is not None and coords_dest.get('lon') is not None

    if not (has_pickup and has_dest):
        # print("⚠️ Coordenadas incompletas. Ignorando.")
        return {"action": "IGNORE", "reasons": ["Faltan coordenadas GPS"], "analysis": ["No GPS Data"]}

    # 2. CÁLCULO DE DISTANCIAS REALES
    lat_pickup = float(coords_pickup['lat'])
    lon_pickup = float(coords_pickup['lon'])
    lat_dest = float(coords_dest['lat'])
    lon_dest = float(coords_dest['lon'])

    # 2.1 VERIFICACION DE COORDENADAS ZERO
    if (lat_pickup == 0 and lon_pickup == 0) or (lat_dest == 0 and lon_dest == 0):
        return {"action": "IGNORE", "reasons": ["Coordenadas 0.0"], "analysis": ["GPS Zero - Ignorado"]}

    # --- APP DATA INJECTION (Cost Saving) ---
    app_dist_a = order.get('app_dist_a', 0)
    app_dist_b = order.get('app_dist_b', 0)
    
    # Check for our new injected cached data
    cached_dist_a = order.get('cached_dist_a', 0)
    cached_dist_b = order.get('cached_dist_b', 0)

    dist_a_km = 0
    dist_b_km = 0
    source_a = "NONE"
    source_b = "NONE"

    # Use Cached Data if available (Priority 1)
    if cached_dist_b > 0:
        dist_a_km = cached_dist_a / 1000.0
        dist_b_km = cached_dist_b / 1000.0
        source_a = "CACHED_DATA"
        source_b = "CACHED_DATA"
    # Use App Data if available (Priority 2)
    elif app_dist_b > 0:
        # App sends meters, convert to km
        dist_a_km = app_dist_a / 1000.0
        dist_b_km = app_dist_b / 1000.0
        source_a = "APP_DATA"
        source_b = "APP_DATA"
        # print(f"  📲 Using App Data: A={dist_a_km}km, B={dist_b_km}km")
    else:
        # STRICT MODE: NO EXTERNAL APIS
        # print("⚠️ No App Data available. IGNORED (Strict Mode).")
        return {"action": "IGNORE", "reasons": ["Faltan datos de App (Distancia)"], "analysis": ["No App Data"]}

    # (Removed Parallel Calculation Block)

    # Redondear a 1 decimal
    dist_a_km = math.floor(dist_a_km * 10) / 10.0
    dist_b_km = math.floor(dist_b_km * 10) / 10.0

    dist_data = {"a": dist_a_km, "b": dist_b_km}

    analysis.append(f"Src: {source_b}")
    analysis.append(f"Pickup: {dist_a_km}km (Max {config['max_pickup_dist']})")
    analysis.append(f"Viaje: {dist_b_km}km (Max {config['max_dest_dist']})")

    # print(f"  🗺️ Geo ({source_b}): A={dist_a_km}km, B={dist_b_km}km")

    # 3. VERIFICAR ZONAS BLOQUEADAS Y FAVORITAS
    user_id = order.get('user_id', 'default_user')

    # helper for zone checking
    def is_in_zone(lat, lon, zone):
        # 1. Check Polygon (Priority)
        if zone.get('points') and len(zone['points']) >= 3:
            return point_in_polygon(lat, lon, zone['points'])
        # 2. Check Circle (Fallback)
        if zone.get('radius_km'):
            return point_in_circle(lat, lon, zone)
        return False

    # 3.A BLOQUEO (Prioridad Alta)
    blocked_found = False
    if config.get('blocked_zones_enabled', True):
        zones = load_blocked_zones(user_id)
        # Check Pickup
        for zone in zones:
            if zone.get('applies_to') in ['pickup', 'both']:
                if is_in_zone(lat_pickup, lon_pickup, zone):
                    reasons.append(f"🚫 Zona Bloq. (Recojo): {zone.get('name')}")
                    blocked_found = True
        # Check Destination
        for zone in zones:
            if zone.get('applies_to') in ['destination', 'both']:
                if is_in_zone(lat_dest, lon_dest, zone):
                    reasons.append(f"🚫 Zona Bloq. (Destino): {zone.get('name')}")
                    blocked_found = True

    # Si hay bloqueo, no procesar favoritas para evitar mensajes contradictorios
    if blocked_found:
        return {"action": "REJECT", "reasons": reasons, "analysis": analysis, "dist_data": dist_data, "config": config_data if 'config_data' in locals() else {}}

    # 3.B FAVORITAS (Whitelist / Allowlist)
    if config.get('favorite_zones_enabled', False):
        fav_zones = load_favorite_zones(user_id)

        # 🛡️ ROBUSTES: Si no hay zonas cargadas pero está activo, intentar recarga forzada (Evitar   falso negativo)
        if not fav_zones:
             print(f"⚠️ Favorite zones enabled but empty for {user_id}. Forcing refresh...")
             fav_zones = load_favorite_zones(user_id, force_refresh=True)

        pickup_in_fav = False
        dest_in_fav = False
        pickup_zone_name = ""
        dest_zone_name = ""

        # Check Pickup
        for zone in fav_zones:
            if zone.get('applies_to') in ['pickup', 'both']:
                if is_in_zone(lat_pickup, lon_pickup, zone):
                    pickup_in_fav = True
                    pickup_zone_name = zone.get('name')
                    break

        # Check Destination
        for zone in fav_zones:
            if zone.get('applies_to') in ['destination', 'both']:
                if is_in_zone(lat_dest, lon_dest, zone):
                    dest_in_fav = True
                    dest_zone_name = zone.get('name')
                    break

        if pickup_in_fav and dest_in_fav:
            analysis.append(f"✅ Favorita: Pickup[{pickup_zone_name}] -> Dest[{dest_zone_name}]")
            analysis.append("Oferta habilitada: el viaje está dentro de las zonas configuradas")
        else:
            # Si el modo favoritas está activo y NO cumple ambas, RECHAZAR.
            if not reasons: # Solo si no hay otras razones de peso aun
                msg = []
                if not pickup_in_fav: msg.append("Recojo fuera")
                if not dest_in_fav: msg.append("Destino fuera")
                reasons.append(f"Fuera de zona favorita ({', '.join(msg)})")

    # 4. REGLAS DE DISTANCIA (Usando lo calculado, no la app)
    if dist_a_km > config['max_pickup_dist']:
        reasons.append(f"Recojo lejos: {dist_a_km}km > {config['max_pickup_dist']}")

    if dist_b_km > config['max_dest_dist']:
        reasons.append(f"Viaje largo: {dist_b_km}km > {config['max_dest_dist']}")

    if dist_b_km < config['min_dest_dist']:
        reasons.append(f"Viaje corto: {dist_b_km}km < {config['min_dest_dist']}")

    # 5. REGLAS DE CLIENTE
    rating = order.get('rating', 5.0)
    rides = order.get('rides', 0)

    analysis.append(f"Rating: {rating} (Min {config['min_rating']})")

    if rating < config['min_rating']:
        reasons.append(f"Rating bajo: {rating}")

    if rides < config['min_rides']:
        reasons.append(f"Usuario nuevo: {rides} viajes")

    # 5.1 VERIFICAR PRECIO MÁXIMO DEL CLIENTE (RECHAZO DIRECTO)
    client_offer = order.get('price', 0)
    max_p = config.get('max_price', 0)
    if max_p > 0 and client_offer > max_p:
        reasons.append(f"Precio muy alto: {client_offer} > {max_p}")

    # 6. FILTROS EXTRA
    has_stops = order.get('has_stops', False)
    if not config['accept_stops'] and has_stops:
        reasons.append("Tiene paradas")

    if config['reject_tags']:
        # Concatenar campos de texto (Descripción y Notas)
        d_val = order.get('description')
        desc = str(d_val).strip() if d_val is not None else ""

        n_val = order.get('notes')
        notes = str(n_val).strip() if n_val is not None else ""

        # Sanitizar valores "None" o "null"
        if desc.lower() in ['none', 'null']: desc = ""
        if notes.lower() in ['none', 'null']: notes = ""

        # Obtener Tags (Labels)
        l_val = order.get('labels')
        if isinstance(l_val, list):
            labels_str = " ".join([str(x) for x in l_val])
        else:
            labels_str = str(l_val).strip() if l_val is not None else ""
            if labels_str.lower() in ['none', 'null', '[]']: labels_str = ""

        # Lista de palabras seguras (Métodos de pago permitidos en tags)
        # Estas palabras harán que el tag sea IGNORADO por el bloqueo total.
        SAFE_PAYMENT_KEYWORDS = [
            "nequi", "daviplata", "bancolombia", "ahorro a la mano",
            "yape", "plin", "tigo", "mercado pago", "mercadopago",
            "santander", "mach", "tenpo", "mio", "billet", "qr", "efectivo"
        ]

        banned = [w for w in config.get('banned_words', []) if w.strip()]

        if not banned:
            # LÓGICA BLOQUEO TOTAL (Sin palabras configuradas):
            # 1. Descripción y Notas -> SIEMPRE RECHAZAR si existen.
            reject = False
            if desc or notes:
                 # print(f"  🛑 REJECT_TAGS (Block All): Desc='{desc}' Notes='{notes}'")
                 reasons.append("Tiene comentarios/notas")
                 reject = True

            # 2. Tabs/Etiquetas -> RECHAZAR si NO son métodos de pago conocidos.
            if not reject and isinstance(l_val, list):
                for tag in l_val:
                    tag_lower = str(tag).lower()
                    # Verificar si el tag es "seguro" (es un método de pago)
                    is_safe = False
                    for safe_word in SAFE_PAYMENT_KEYWORDS:
                        if safe_word in tag_lower:
                            is_safe = True
                            break

                    if not is_safe:
                        # print(f"  🛑 REJECT_TAGS (Block All): Tag desconocido='{tag}'")
                        reasons.append(f"Tiene tags no permitidos ({tag})")
                        break
            elif not reject and labels_str:
                 # Fallback para cuando labels no es lista
                 is_safe = False
                 for safe_word in SAFE_PAYMENT_KEYWORDS:
                     if safe_word in labels_str.lower():
                         is_safe = True
                         break
                 if not is_safe:
                     reasons.append("Tiene tags no permitidos")

        else:
            # LÓGICA BLOQUEO ESPECÍFICO (Con palabras configuradas):
            # Buscar en Descripción, Notas y Tags
            text_to_search = (desc + " " + notes + " " + labels_str).lower()
            found_words = [w for w in banned if w.lower().strip() in text_to_search]
            if found_words:
                reasons.append(f"Palabra prohibida detectada: {', '.join(found_words)}")

    # INJECT CONFIG INTO DECISION (Piggyback Sync)
    # Ensure membership is defined/calculated before this
    # It is calculated at start of decide()

    config_data = {
        "max_pickup_dist": config.get('max_pickup_dist', 3.0),
        "max_repeats": config.get('max_repeats', 0),
        "membership_message": config.get('membership_message', '')
    }

    if reasons:
        return {"action": "REJECT", "reasons": reasons, "analysis": analysis, "dist_data": dist_data, "config": config_data}

    return {"action": "PASS", "analysis": analysis, "dist_data": dist_data, "config": config_data}

# --- FUNCIONES DE SEGURIDAD ---

def validate_hmac_auth(req, user_id):
    """
    Valida el token HMAC y el timestamp para peticiones críticas.
    Retorna (True, None) si es válido, o (False, (error_dict, status_code)) si falla.
    """
    client_ts_str = req.headers.get('X-App-Timestamp', '0')
    client_auth = req.headers.get('X-App-Auth', '').lower().strip()

    try:
        client_ts = int(client_ts_str)
        server_ts = int(time.time())

        # 1. Validar Tiempo (Replay Attack Prevention - Max 300s skew)
        if abs(server_ts - client_ts) > 300:
            print(f"⛔ SECURITY ALERT: Timestamp expired/invalid for user {user_id}. Server: {server_ts}, Client: {client_ts}")
            return False, ({
                "action": "BLOCK",
                "message": "Security Violation: Request Expired. Check device time.",
                "reason": "INVALID_TIMESTAMP"
            }, 403)

        # 2. Validar Hash (HMAC Check)
        payload_to_hash = f"{SECRET_KEY}{client_ts}".encode('utf-8')
        expected_hash = hashlib.sha256(payload_to_hash).hexdigest()

        if client_auth != expected_hash:
            print(f"⛔ SECURITY ALERT: Invalid Auth Token for user {user_id}: {client_auth} != {expected_hash}")
            return False, ({
                "action": "BLOCK",
                "message": "Security Violation: Unauthorized App Modification.",
                "reason": "INVALID_AUTH"
            }, 403)

        return True, None

    except ValueError:
         print(f"⛔ SECURITY ALERT: Malformed timestamp for user {user_id}: {client_ts_str}")
         return False, ({"action": "BLOCK", "reason": "MALFORMED_TIMESTAMP"}, 403)


# Función migrate_user_data eliminada (Sistema de migración desactivado)



# --- RUTAS FLASK ---

@app.errorhandler(404)
def resource_not_found(e):
    return jsonify({"status": "error", "message": "Not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"status": "error", "message": "Internal server error"}), 500

@app.route('/ping', methods=['GET', 'POST'])
def ping():
    try:
        user_id = request.args.get('user_id', 'default_user')
        old_device_id = request.args.get('old_device_id', None)
        phone = request.args.get('phone', None)
        if request.method == 'POST':
            data = request.json or {}
            user_id = data.get('user_id', user_id)

        # --- VALIDACIÓN DE SEGURIDAD (HMAC + TIMESTAMP) ---
        is_valid, error_response = validate_hmac_auth(request, user_id)
        if not is_valid:
            # Para /ping, devolvemos un formato que la app entienda para bloquear el encendido
            # BotClient$Starter.smali espera un json con "membership": {"can_operate": boolean, "message": string}
            err_dict, status = error_response
            return jsonify({
                "membership": {
                    "can_operate": False,
                    "message": err_dict["message"]
                }
            }), 200 # Usamos 200 porque la app cliente actual asume errores en la respuesta parsing, no en HTTP status para el Ping

        # Si pasa la seguridad, cargamos config (forzando lectura remota para refrescar DB -> Cache)
        cfg = load_config(user_id, force_remote=True)
        if phone and str(cfg.get('phone')) != str(phone):
            cfg['phone'] = phone
            save_config(user_id, cfg)

        membership = check_membership(user_id, cfg)

        print(f"🔔 PING/START received for {user_id}. Config Reloaded. Membership: {membership['status']}")

        return jsonify({
            "status": "pong",
            "timestamp": time.time(),
            "config_reloaded": True,
            "membership": membership
        })
    except Exception as e:
        print(f"❌ Error en /ping: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/decide', methods=['POST'])
def decide():
    try:
        order_data = request.json
        # print(f"\n📥 INCOMING REQUEST (/decide): {json.dumps(order_data, indent=2, ensure_ascii=False)}") # LOG INPUT

        user_id = order_data.get('user_id', 'default_user')
        order_id = order_data.get('order_id') or order_data.get('id')

        # --- VALIDACIÓN DE SEGURIDAD (HMAC + TIMESTAMP) ---
        is_valid, error_response = validate_hmac_auth(request, user_id)
        if not is_valid:
            err_dict, status = error_response
            return err_dict, status

        # Cargar Config (Solo UNA VEZ)
        cfg = load_config(user_id)



        # --- VERIFICACIÓN DE MEMBRESÍA ---
        membership = check_membership(user_id, cfg)
        if not membership['can_operate']:
            # print(f"⛔ USER BLOCKED ({user_id}): {membership['message']}")
            resp = {
                "action": "EXPIRED",
                "message": membership['message'],
                "expires_at": membership['expires_at'],
                "whatsapp_url": cfg.get('whatsapp_url', '')
            }
            # print(f"📤 RESPONSE: {json.dumps(resp, indent=2, ensure_ascii=False)}") # LOG OUTPUT
            return resp

        # Inyectar mensaje en config para que evaluate_order lo vea
        cfg['membership_message'] = membership['message']

        # Gestión de Duplicados
        if order_id:
            cleanup_processed_orders()
            # cfg ya está cargada
            max_repeats = cfg.get('max_repeats', 0)

            if order_id in PROCESSED_ORDERS:
                entry = PROCESSED_ORDERS[order_id]
                if entry['count'] > max_repeats:
                    # print(f"⚠️ Orden duplicada ignorada: {order_id}")
                    resp = {"action": "IGNORE", "reason": "Duplicate"}
                    # print(f"📤 RESPONSE: {json.dumps(resp, indent=2, ensure_ascii=False)}") # LOG OUTPUT
                    return resp
                entry['count'] += 1
                entry['ts'] = time.time()
            else:
                PROCESSED_ORDERS[order_id] = {'ts': time.time(), 'count': 1}

        # Evaluar
        decision = evaluate_order(order_data, cfg)

        # print(f"🤖 DECISIÓN: {decision['action']}")
        if 'reasons' in decision:
            # print(f"❌ RAZONES: {decision['reasons']}")
            pass

        # Si rechazamos, retornamos inmediatamente
        if decision['action'] != 'PASS':
            decision['order_id'] = order_id
            # print(f"📤 RESPONSE: {json.dumps(decision, indent=2, ensure_ascii=False)}") # LOG OUTPUT
            return decision

        # LOGICA DE BIDDING (Oferta)
        # Usamos EXCLUSIVAMENTE la distancia calculada por nosotros
        dist_data = decision.get('dist_data', {})
        dist_b_km = dist_data.get('b', 0)

        min_price = cfg.get('min_price', 9000)
        base_price_km = cfg.get('base_price_km', 4000)
        client_price = order_data.get('price', 0)

        # Extract analysis early for use in reject logic
        analysis = decision.get('analysis', [])

        # Precio base calculado (Distancia)
        calc_price_km = dist_b_km * base_price_km

        # Revisar Tiers (Tarifa dinámica - Aplica solo a calc_price_km)
        tiers = cfg.get('fare_tiers', [])

        # Ordenar tiers por min_dist para asegurar evaluación secuencial correcta
        try:
             tiers.sort(key=lambda x: float(x.get('min_dist', 0)))
        except Exception as e:
             print(f"⚠️ Error sorting tiers: {e}")

        for tier in tiers:
            try:
                # Conversión explícita a float para evitar errores de comparación string vs float
                t_min = float(tier.get('min_dist', 0))
                t_max = float(tier.get('max_dist', 1000))

                # print(f"  🔍 Checking Tier: {t_min}-{t_max} km for Dist: {dist_b_km}", flush=True)

                # CAMBIO CRÍTICO: Usar <= para cerrar brechas (ej. 5.0 entra en 0-5)
                if dist_b_km >= t_min and dist_b_km <= t_max:
                    t_rate = float(tier.get('rate', base_price_km))
                    calc_price_km = dist_b_km * t_rate
                    print(f"  ⚡ Tier Aplicado [{t_min}-{t_max}]: {t_rate}/km -> Base Price: {int(calc_price_km)}", flush=True)
                    break
            except Exception as e:
                print(f"⚠️ Error processing tier {tier}: {e}", flush=True)

        # --- NUEVO CÁLCULO: PRECIO POR TIEMPO ---
        app_time_a = order_data.get('cached_time_a') or order_data.get('app_time_a', 0) # Segundos a pickup
        app_time_b = order_data.get('cached_time_b') or order_data.get('app_time_b', 0) # Segundos de viaje

        hourly_rate = cfg.get('hourly_rate', 0)
        time_selection = cfg.get('time_selection', 'travel')

        # Determinar tiempo total en segundos a cobrar
        seconds_to_charge = 0
        if time_selection == 'total':
             seconds_to_charge = app_time_a + app_time_b
        else:
             seconds_to_charge = app_time_b # Default 'travel'

        # Regla de 3: (Segundos * TarifaHora) / 3600
        hours_to_charge = seconds_to_charge / 3600.0
        calc_price_time = hours_to_charge * hourly_rate

        print(f"  ⏱️ Time Calc: {int(app_time_a)}s + {int(app_time_b)}s -> {int(seconds_to_charge)}s ({hours_to_charge:.2f}h) * {hourly_rate}/h = {int(calc_price_time)}")

        # --- ESTRATEGIA DE PRECIO ---
        pricing_strategy = cfg.get('pricing_strategy', 'km')
        calc_price = 0

        if pricing_strategy == 'time':
            calc_price = calc_price_time
            print(f"  🎯 Estrategia: Solo Tiempo -> {int(calc_price)}")
        elif pricing_strategy == 'mixed':
            calc_price = max(calc_price_km, calc_price_time)
            print(f"  🎯 Estrategia: Mixto (Max) -> Km:{int(calc_price_km)} vs Time:{int(calc_price_time)} -> {int(calc_price)}")
        else:
            # Default 'km'
            calc_price = calc_price_km
            print(f"  🎯 Estrategia: Solo Km -> {int(calc_price)}")


        # Ensure values are numbers
        try:
            calc_price = float(calc_price)
            client_price = float(client_price)
            min_price = float(min_price)
        except ValueError:
            print(f"⚠️ Error converting prices to float: Calc={calc_price}, Cli={client_price}, Min={min_price}")

        print(f"  🧮 MAX CALC: Calc={calc_price} | Client={client_price} | Min={min_price} => Target  ={max(calc_price, client_price, min_price)}")

        # Ofertar el MAXIMO entre lo calculado, lo que ofrece el cliente y el mínimo configurado
        raw_offer_price = max(calc_price, client_price, min_price)
        offer_price = raw_offer_price

        # LOGICA DE REDONDEO
        rounding_factor = int(cfg.get('price_rounding_factor', 1))
        if rounding_factor > 0:
            offer_price = math.ceil(raw_offer_price / rounding_factor) * rounding_factor
            if offer_price != raw_offer_price:
                 print(f"  🍡 Rounding: {raw_offer_price:.2f} -> {offer_price:.0f} (Factor: {rounding_factor})")

        # Respetar topes globales
        # Respetar topes globales (solo si max_price > 0)
        max_p = cfg.get('max_price', 0)
        if max_p > 0 and offer_price > max_p:
            offer_price = max_p

        if offer_price < cfg['min_price']: offer_price = cfg['min_price']

        # --- LOGICA DE CLIENT MAX PRICE ---
        client_max_price = order_data.get('max_price', 0)
        secondary_max_price = order_data.get('secondary_max_price', 0)

        # El precio máximo efectivo es el mayor entre el configurado manualmente y el extraído de los botones
        effective_max_price = max(client_max_price, secondary_max_price)

        if effective_max_price > 0 and offer_price > effective_max_price:
            if cfg.get('offer_max_if_exceeds', False):
                # print(f"  ⚠️ Oferta {offer_price} > Max {effective_max_price}. Ajustando a Max.")
                offer_price = effective_max_price
            else:
                # print(f"  🛑 Oferta {offer_price} > Max {effective_max_price} (Client:{client_max_price}, 2nd:{secondary_max_price}). REJECTING.")
                resp = {
                    "action": "REJECT",
                    "order_id": order_id,
                    "reasons": [f"Precio muy alto (Max: {effective_max_price})"],
                    "analysis": analysis,
                    "dist_data": decision.get('dist_data'),
                    "config": decision.get('config')
                }
                # print(f"📤 RESPONSE: {json.dumps(resp, indent=2, ensure_ascii=False)}") # LOG OUTPUT
                return resp


        # print(f"  💰 Bidding: Dist={dist_b_km}km | Calc={calc_price:.0f} | Offer={offer_price:.0f}")

        # Add price debug information to analysis
        analysis.append(f"Price: {int(offer_price)} (Raw:{int(raw_offer_price)}/Calc:{int(calc_price)}/Cli:{int(client_price)})")

        resp = {
            "action": "BID",
            "price": offer_price,
            "pickup_time": cfg.get('pickup_time_minutes', 5),
            "analysis": analysis,
            "config": {
                "max_pickup_dist": cfg.get('max_pickup_dist', 3.0),
                "max_repeats": cfg.get('max_repeats', 0)
            }
        }
        # print(f"📤 RESPONSE: {json.dumps(resp, indent=2, ensure_ascii=False)}") # LOG OUTPUT
        return resp

    except Exception as e:
        # print(f"❌ INTERNAL SERVER ERROR: {e}")
        # Fail safe: Return IGNORE or PASS to avoid crashing client
        resp = {"action": "PASS", "reason": f"Server Error: {str(e)[:50]}"}
        # print(f"📤 RESPONSE: {json.dumps(resp, indent=2, ensure_ascii=False)}") # LOG OUTPUT
        return resp

@app.route('/profiles/switch', methods=['POST'])
def switch_profile():
    try:
        data = request.json
        user_id = data.get('user_id')
        profile_id = data.get('profile_id')

        if not user_id or not profile_id:
            return jsonify({"status": "error", "message": "Faltan datos"}), 400

        print(f"🔄 Switching profile for {user_id} to {profile_id}", flush=True)

        if db:
            # Actualizar active_profile_id en root
            db.collection('bot_configs').document(user_id).update({
                'active_profile_id': profile_id
            })
            # Invalidate cache to force reload next time
            invalidate_cache(user_id)
            bump_signal(user_id)

        return jsonify({"status": "success", "active_profile_id": profile_id})
    except Exception as e:
        print(f"❌ Error switching profile: {e}", flush=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/profiles/create', methods=['POST'])
def create_profile():
    try:
        data = request.json
        user_id = data.get('user_id')
        new_profile_name = data.get('profile_name')
        source_profile_id = data.get('source_profile_id', 'default') # Copiar desde aquí

        if not user_id or not new_profile_name:
            return jsonify({"status": "error", "message": "Faltan datos"}), 400

        # Generar ID seguro (slugify simple)
        new_profile_id = "".join(x for x in new_profile_name if x.isalnum()).lower()
        if not new_profile_id: new_profile_id = f"profile_{int(time.time())}"

        print(f"✨ Creating profile '{new_profile_name}' ({new_profile_id}) for {user_id}", flush=True)

        if db:
            root_ref = db.collection('bot_configs').document(user_id)
            profiles_ref = root_ref.collection('profile_config')

            # Verificar si ya existe
            if profiles_ref.document(new_profile_id).get().exists:
                 return jsonify({"status": "error", "message": "El perfil ya existe"}), 400

            # Cargar config base para copiar
            base_config = DEFAULT_CONFIG.copy()

            source_type = data.get('source_type', 'user') # 'user' | 'suggestion'

            if source_type == 'suggestion':
                # Copiar desde colección de sugerencias
                suggestion_doc = db.collection('suggested_configs').document(source_profile_id).get()
                if suggestion_doc.exists:
                    # La data interna ya tiene la config mezclada
                    # pero removemos 'metadata' si existe para limpiza
                    s_data = suggestion_doc.to_dict()
                    if 'metadata' in s_data: del s_data['metadata']
                    base_config.update(s_data)
                else:
                    # Fallback a local dict si la DB falla o no existe documento
                    local_sug = next((p for p in SUGGESTED_PROFILES if p['id'] == source_profile_id), None)
                    if local_sug:
                        base_config.update(local_sug['config'])

            else:
                # Copiar desde perfil de usuario (Legacy/Normal behavior)
                source_doc = profiles_ref.document(source_profile_id).get()
                if source_doc.exists:
                    base_config.update(source_doc.to_dict())

            # Ajustar nombre
            base_config['profile_name'] = new_profile_name
            # Limpiar metadatos root de la copia si se colaron
            for meta in ROOT_METADATA_FIELDS:
                if meta in base_config: del base_config[meta]

            # Guardar nuevo perfil
            profiles_ref.document(new_profile_id).set(base_config)

            return jsonify({"status": "success", "profile_id": new_profile_id, "name": new_profile_name})

        return jsonify({"status": "error", "message": "DB no conectada"}), 500
    except Exception as e:
        print(f"❌ Error creating profile: {e}", flush=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/profiles/delete', methods=['POST'])
def delete_profile():
    try:
        data = request.json
        user_id = data.get('user_id')
        profile_id = data.get('profile_id')

        if not user_id or not profile_id:
             return jsonify({"status": "error", "message": "Faltan datos"}), 400

        if profile_id == 'Mi Perfil':
            return jsonify({"status": "error", "message": "No se puede borrar el perfil Mi Perfil"}), 400

        print(f"🗑️ Deleting profile {profile_id} for {user_id}", flush=True)

        if db:
            root_ref = db.collection('bot_configs').document(user_id)
            # Borrar doc de subcolección
            root_ref.collection('profile_config').document(profile_id).delete()

            # Verificar si era el activo
            root_doc = root_ref.get()
            if root_doc.exists:
                current_active = root_doc.to_dict().get('active_profile_id')
                if current_active == profile_id:
                    print(f"⚠️ Deleted profile was active. Switching to default.", flush=True)
                    root_ref.update({'active_profile_id': 'Mi Perfil'})
                    invalidate_cache(user_id)
                    bump_signal(user_id)

        return jsonify({"status": "success"})
    except Exception as e:
         print(f"❌ Error deleting profile: {e}", flush=True)
         return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/profiles/suggested', methods=['GET'])
def list_suggested_profiles():
    try:
        suggestions = []
        if db:
            docs = db.collection('suggested_configs').stream()
            for doc in docs:
                d = doc.to_dict()
                meta = d.get('metadata', {})
                # Clean config part (remove metadata from it)
                cfg = d.copy()
                if 'metadata' in cfg: del cfg['metadata']

                suggestions.append({
                    "id": doc.id,
                    "name": meta.get('name', doc.id),
                    "desc": meta.get('desc', ''),
                    "config": cfg # Include config for the manager!
                })

        # Fallback si DB vacío o desconectado
        if not suggestions:
             for p in SUGGESTED_PROFILES:
                 suggestions.append({
                     "id": p['id'],
                     "name": p['name'],
                     "desc": p['desc'],
                     "config": p['config']
                 })

        return jsonify(suggestions)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/profiles/list', methods=['GET'])
def list_profiles():
    try:
        user_id = request.args.get('user_id')
        if not user_id: return jsonify([]), 400

        profiles = []
        if db:
            docs = db.collection('bot_configs').document(user_id).collection('profile_config').stream()
            for doc in docs:
                d = doc.to_dict()
                profiles.append({
                    "id": doc.id,
                    "name": d.get('profile_name', doc.id)
                })

        # Si no hay perfiles (aun no migrado o vacio), devolver default simulado
        if not profiles:
            profiles.append({"id": "Mi Perfil", "name": "Mi Perfil"})

        return jsonify(profiles)
    except Exception as e:
        print(f"❌ Error listing profiles: {e}", flush=True)
        return jsonify([]), 500

@app.route('/membership_status', methods=['GET'])
def get_membership_status():
    user_id = request.args.get('user_id', 'default_user')
    version_key = request.args.get('version', 'v1')
    
    cfg = load_config(user_id, force_remote=True)
    membership = check_membership(user_id, cfg)
    
    VERSION_MAP = {
        'v1': {'name': 'Original', 'phone': '573142999526'},
        'v2': {'name': 'Ñato Bot', 'phone': '18298869919'},
        'v3': {'name': 'Dalia Bot','phone': '18298533482'},
        'v4': {'name': 'Chris Bot','phone': '18293664411'},
        'v5': {'name': 'Danny Bot','phone': '18495132638'},
        'v6': {'name': 'Musan Bot','phone': '18293376904'},
        'v7': {'name': 'Conejo Draiver','phone': '50589417950'},
        'v8': {'name': 'Dominican Bot','phone': '18098403817'},
        'v9': {'name': 'Original','phone': '51975159676'},
        'v10': {'name': 'Carlos Bot','phone': '18097823602'},
        'v11': {'name': 'Bot Furia','phone': '18296987188'},
        'v12': {'name': 'Juan Carlos Bot','phone': '18293645306'},
        'v13': {'name': 'Original','phone': '573228824169'},
        'v14': {'name': 'Original','phone': '18493797972'},
        'v15': {'name': 'ROBOCOP','phone': '18293572555'},
        'v16': {'name': 'MVP BOT','phone': '18492094162'},
        'v17': {'name': 'Super Flash Bot','phone': '18296838484'},
        'v18': {'name': 'Flash Bot','phone': '18098486477'},
    }
    
    if version_key not in VERSION_MAP:
        version_key = 'v1'
        
    contact_phone = VERSION_MAP[version_key]['phone']
    whatsapp_url = f"https://wa.me/{contact_phone}?text=Hola,%20quiero%20renovar%20mi%20membresía%20(Mi%20ID:%20{user_id})"
    
    return jsonify({
        "membership_status": membership.get('status', 'EXPIRED'),
        "has_membership": membership.get('can_operate', True),
        "whatsapp_url": whatsapp_url
    })

@app.route('/config', methods=['GET', 'POST']) 
def config_ui():
    user_id = request.args.get('user_id', 'default_user')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')

    # -------------------------------------------------------------
    # 🔧 CONFIGURACIÓN DE VERSIONES Y CONTACTO
    # -------------------------------------------------------------
    # GUÍA: Para agregar nuevas versiones (v2, v3, etc.), copia la línea
    # de abajo y cambia 'v1' por la versión que quieras y el número.
    VERSION_MAP = {
        'v1': {'name': 'Original', 'phone': '573142999526', 'logo': 'https://vidcache.net:8161/static/85da964fa796b810f504b3cf81b89b36be6cb4fb/indrive.jpg', 'show_distributor': True, 'title': 'InDrive Automático'},
        'v2': {'name': 'Ñato Bot', 'phone': '18298869919', 'logo': 'https://vidcache.net:8161/static/313c6a1836041cdc196969418e17aab181f33041/%C3%91ato%20logo.jpg', 'show_distributor': False, 'title': 'Ñato Bot'},
        'v3': {'name': 'Dalia Bot','phone': '18298533482', 'logo': 'https://vidcache.net:8161/static/dc4ff1267692d8b7ec216bc3a4dcce4d91e75b44/dalia%20bot.jpg', 'show_distributor': False, 'title': 'Dalia Bot'},
        'v4': {'name': 'Chris Bot','phone': '18293664411', 'logo': 'https://vidcache.net:8161/static/20c4e501dbe389b6e3380173e183afe581d3589f/chirs%20bot.jpg', 'show_distributor': False, 'title': 'Chris Bot'},
        'v5': {'name': 'Danny Bot','phone': '18495132638', 'logo': 'https://vidcache.net:8161/static/1135b5ff27a2bee59281d26cf8a9e609de7fae6c/danny.jpeg', 'show_distributor': False, 'title': 'Danny Bot'},
        'v6': {'name': 'Musan Bot','phone': '18293376904', 'logo': 'https://vidcache.net:8161/static/c926c05703e41f62ccdca876af64b703215fae16/musan.jpeg', 'show_distributor': False, 'title': 'Musan_Bot'},
        'v7': {'name': 'Conejo Draiver','phone': '50589417950', 'logo': 'https://vidcache.net:8161/static/96c82eda1a6e86ab3edaf02118606c416852abea/logo nicaragua.jpeg', 'show_distributor': False, 'title': 'Conejo Driver'},
        'v8': {'name': 'Dominican Bot','phone': '18098403817', 'logo': 'https://vidcache.net:8161/static/fd9b05f1849bd5e72736ca02b39b96f996e6156b/de2dcd3e-3860-4fef-8dfa-cf898c563c3f.jpg', 'show_distributor': False, 'title': 'Dominican Bot'},
        'v9': {'name': 'Original','phone': '51975159676', 'logo': 'https://vidcache.net:8161/static/85da964fa796b810f504b3cf81b89b36be6cb4fb/indrive.jpg', 'show_distributor': False, 'title': 'InDrive Automático'},
        'v10': {'name': 'Carlos BOT','phone': '18097823602', 'logo': 'https://vidcache.net:8161/static/5a53af257456729e5e73accb2caf735c683b16d2/Carlos bot.jpeg', 'show_distributor': False, 'title': 'Carlos BOT'},
        'v11': {'name': 'Bot Furia','phone': '18296987188', 'logo': 'https://vidcache.net:8161/static/daa8f2e25e373b821b0997acc4d175e4ff4b9353/furia bot.jpeg', 'show_distributor': False, 'title': 'Bot Furia'},
        'v12': {'name': 'Juan Carlos Bot','phone': '18293645306', 'logo': 'https://vidcache.net:8161/static/041dd6298048b8c796aed0deb382a7df1e9c228b/logo juancarlos.jpeg', 'show_distributor': False, 'title': 'Juan Carlos Bot'},
        'v13': {'name': 'Original','phone': '573228824169', 'logo': 'https://vidcache.net:8161/static/85da964fa796b810f504b3cf81b89b36be6cb4fb/indrive.jpg', 'show_distributor': False, 'title': 'InDrive Automático'},
        'v14': {'name': 'Original','phone': '18493797972', 'logo': 'https://vidcache.net:8161/static/85da964fa796b810f504b3cf81b89b36be6cb4fb/indrive.jpg', 'show_distributor': False, 'title': 'InDrive Automático'},
        'v15': {'name': 'ROBOCOP','phone': '18293572555', 'logo': 'https://vidcache.net:8161/static/94c1b891c3250a92dbb8d3c961e7d65497838fc2/ROBOCOP LOGO.jpeg', 'show_distributor': False, 'title': 'ROBOCOP'},
        'v16': {'name': 'MVP BOT','phone': '18492094162', 'logo': 'https://vidcache.net:8161/static/be4bb1c26e0969329d33d2510ade898bff1bf5f1/mvp bot.jpeg', 'show_distributor': False, 'title': 'MVP BOT'},
        'v17': {'name': 'Super Flash Bot','phone': '18296838484', 'logo': 'https://vidcache.net:8161/static/d1444349ebd7ea336882552ca8a007bd06a6dbc5/Super Flash Bot.jpeg', 'show_distributor': False, 'title': 'Super Flash Bot'},
        'v18': {'name': 'Flash Bot','phone': '18098486477', 'logo': 'https://vidcache.net:8161/static/7dee91143e75e8265d95fde84597ae753c96f24a/flash bot 2.jpeg', 'show_distributor': False, 'title': 'Flash Bot'},

    }

    # 1. Detectar versión (por URL parameter ?version=vX)
    version_key = request.args.get('version', 'v1')

    # 2. Validar (Si no existe, usar v1 por defecto)
    if version_key not in VERSION_MAP:
        version_key = 'v1'

    # 3. Extraer datos
    version_data = VERSION_MAP[version_key]
    contact_phone = version_data['phone']
    logo_url = version_data.get('logo')
    page_title = version_data.get('title', 'InDrive Auto')
    show_distributor = version_data.get('show_distributor', True)
    version_display = f"{version_key.upper()} ({version_data['name']})"

    # Generar la URL de whatsapp dinámica para inyectarla en la config del usuario
    whatsapp_url = f"https://api.whatsapp.com/send?phone={contact_phone}&text=Hola,%20necesito%20renovar%20mi%20membres%C3%ADa%20({version_display})"

    # print(f"🔧 CONFIG UI LOADED: User={user_id} | VersionDetected={version_key} | Phone={contact_phone}")
    # -------------------------------------------------------------

    if request.method == 'POST':
        data = request.json
        user_id = data.get('user_id', user_id)


        config_data = {k: v for k, v in data.items() if k not in ['user_id', 'old_device_id']}
        config_data['whatsapp_url'] = whatsapp_url # <-- Guardar la URL dinámica basada en la versión
        save_config(user_id, config_data)
        return jsonify({"status": "saved", "user_id": user_id})



    cfg = load_config(user_id)

    # Check membership for UI display
    membership = check_membership(user_id, cfg)
    remaining_days = 0
    if membership and 'expires_at' in membership:
        try:
            expires = datetime.fromisoformat(membership['expires_at'])
            diff = expires - datetime.now(timezone.utc)
            # Usar math.ceil para que cualquier hora extra cuente como un día completo
            remaining_days = math.ceil(diff.total_seconds() / 86400)
        except:
            pass

    if request.args.get('format') == 'json':
        # JSON handling for complex objects in config
        safe_cfg = cfg.copy()
        if 'membership_expires_at' in safe_cfg and isinstance(safe_cfg['membership_expires_at'], datetime):
            safe_cfg['membership_expires_at'] = safe_cfg['membership_expires_at'].isoformat()
        if 'first_seen_at' in safe_cfg and isinstance(safe_cfg['first_seen_at'], datetime):
            safe_cfg['first_seen_at'] = safe_cfg['first_seen_at'].isoformat()

        if 'first_seen_at' in safe_cfg and isinstance(safe_cfg['first_seen_at'], datetime):
            safe_cfg['first_seen_at'] = safe_cfg['first_seen_at'].isoformat()

        # Inyectar validación de membresía en la respuesta para que la app la lea al iniciar
        safe_cfg['membership_info'] = membership
        if not membership.get('can_operate', True):
            # Asegura la efectividad apagando visualmente el bot (la tarjeta)
            safe_cfg['show_info_card'] = False
            safe_cfg['audio_enabled'] = False

        # print(f"📤 CONFIG RESPONSE: {json.dumps(safe_cfg, default=str)}")
        return jsonify(safe_cfg)


    # Read extenal HTML file
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, 'config_dashboard.html')
        with open(file_path, 'r', encoding='utf-8') as f:
            template_content = f.read()

        return render_template_string(template_content, config=cfg, user_id=user_id, lat=lat, lon=lon, membership=membership, remaining_days=remaining_days, contact_phone=contact_phone, version_display=version_display, logo_url=logo_url, show_distributor=show_distributor, page_title=page_title, version_key=version_key)
    except Exception as e:
        print(f"❌ Error loading dashboard template: {e}")
        return f"Error loading Dashboard: {e}"

@app.route('/blocked-zones', methods=['GET', 'POST', 'DELETE'])
def manage_blocked_zones():
    if not db: return jsonify({"error": "Firebase not configured"}), 500

    user_id = request.args.get('user_id')
    if request.method == 'POST': user_id = request.json.get('user_id')
    if not user_id: return jsonify({"error": "Missing user_id"}), 400

    zones_ref = None

    # 1. Obtener active_profile_id
    root_ref = db.collection('bot_configs').document(user_id)
    root_doc = root_ref.get()
    active_profile_id = 'Mi Perfil'
    if root_doc.exists:
        active_profile_id = root_doc.to_dict().get('active_profile_id', 'Mi Perfil')

    # Migración legacy eliminada por optimización
    zones_ref = root_ref.collection('profile_config').document(active_profile_id).collection('blocked_zones')



    if request.method == 'GET':
        docs = zones_ref.stream()
        return jsonify([{**doc.to_dict(), 'id': doc.id} for doc in docs])

    if request.method == 'POST':
        data = request.json
        data['user_id'] = user_id
        if data.get('id'):
            zones_ref.document(data['id']).set(data, merge=True)
            invalidate_cache(user_id)
            bump_signal(user_id)
            return jsonify({"status": "updated", "id": data['id']})
        else:
            if 'id' in data: del data['id']
            _, ref = zones_ref.add(data)
            invalidate_cache(user_id)
            bump_signal(user_id)
            return jsonify({"status": "created", "id": ref.id})

    if request.method == 'DELETE':
        zid = request.args.get('id')
        if not zid: return jsonify({"error": "Missing id"}), 400
        zones_ref.document(zid).delete()
        invalidate_cache(user_id)
        bump_signal(user_id)
        return jsonify({"status": "deleted"})

@app.route('/blocked-zones-ui')
def blocked_zones_ui():
    # Deprecated/Redirect to unified manager
    return zones_manager_ui()

@app.route('/zones-popup')
def zones_popup_ui():
    user_id = request.args.get('user_id', 'default_user')
    version_key = request.args.get('version', 'v1')
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, 'zones_popup.html')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                template = f.read()
            return render_template_string(template, user_id=user_id, version_key=version_key)
        else:
            return f"Error: zones_popup.html not found."
    except Exception as e:
        return f"Error loading UI: {e}"

@app.route('/zones-manager')
def zones_manager_ui():
    user_id = request.args.get('user_id', 'default_user')
    version_key = request.args.get('version', 'v1')
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, 'zones_manager.html')
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                template = f.read()
            return render_template_string(template, user_id=user_id, version_key=version_key)
        else:
            return f"Error: zones_manager.html not found. Bot User ID: {user_id}"
    except Exception as e:
        return f"Error loading UI: {e}"

@app.route('/favorite-zones', methods=['GET', 'POST', 'DELETE'])
def manage_favorite_zones():
    if not db: return jsonify({"error": "Firebase not configured"}), 500

    user_id = request.args.get('user_id')
    if request.method == 'POST': user_id = request.json.get('user_id')
    if not user_id: return jsonify({"error": "Missing user_id"}), 400

    zones_ref = None

    # 1. Obtener active_profile_id
    root_ref = db.collection('bot_configs').document(user_id)
    root_doc = root_ref.get()
    active_profile_id = 'Mi Perfil'
    if root_doc.exists:
        active_profile_id = root_doc.to_dict().get('active_profile_id', 'Mi Perfil')

    # Migración legacy eliminada por optimización
    zones_ref = root_ref.collection('profile_config').document(active_profile_id).collection('favorite_zones')


    if request.method == 'GET':
        docs = zones_ref.stream()
        return jsonify([{**doc.to_dict(), 'id': doc.id} for doc in docs])

    if request.method == 'POST':
        data = request.json
        data['user_id'] = user_id
        if data.get('id'):
            zones_ref.document(data['id']).set(data, merge=True)
            invalidate_cache(user_id)
            bump_signal(user_id)
            return jsonify({"status": "updated", "id": data['id']})
        else:
            if 'id' in data: del data['id']
            _, ref = zones_ref.add(data)
            invalidate_cache(user_id)
            bump_signal(user_id)
            return jsonify({"status": "created", "id": ref.id})

    if request.method == 'DELETE':
        zid = request.args.get('id')
        if not zid: return jsonify({"error": "Missing id"}), 400
        zones_ref.document(zid).delete()
        invalidate_cache(user_id)
        bump_signal(user_id)
        return jsonify({"status": "deleted"})



def admin_suggested_ui():
    try:
        # Robust path finding
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, 'suggested_manager.html')
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Error loading UI: {e}", 500

@app.route('/admin/suggested/save', methods=['POST'])
def admin_suggested_save():
    if not db: return jsonify({"status": "error", "message": "No DB connection"}), 500
    try:
        data = request.json
        pid = data.get('id')
        meta = data.get('metadata', {})
        config = data.get('config', {})

        if not pid: return jsonify({"status": "error", "message": "Missing ID"}), 400

        # Merge metadata into the root of the document for storage (as per our design)
        # doc = { ...config_fields..., "metadata": {name, desc} }
        doc_data = config.copy()
        doc_data['metadata'] = meta

        db.collection('suggested_configs').document(pid).set(doc_data)

        return jsonify({"status": "success"})
    except Exception as e:
        print(f"Error saving suggestion: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/admin/suggested/delete', methods=['POST'])
def admin_suggested_delete():
    if not db: return jsonify({"status": "error", "message": "No DB connection"}), 500
    try:
        data = request.json
        pid = data.get('id')
        if not pid: return jsonify({"status": "error", "message": "Missing ID"}), 400

        db.collection('suggested_configs').document(pid).delete()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- UI TEMPLATE (HTML) ---

if __name__ == '__main__':
    PORT = int(os.environ.get('PORT', 10000)) # Seed suggestions on startup
    if db:
        # Run in background to not block startup
        pass
        # Actually, let's just run it synchronously, it's fast (3 docs)
        seed_suggested_configs()

    print(f"🚀 Server running on port {PORT}...")
    # serve(app, host='0.0.0.0', port=PORT) # Waitress (Production)
    app.run(host='0.0.0.0', port=PORT,)      # Flask Dev (Debug)c