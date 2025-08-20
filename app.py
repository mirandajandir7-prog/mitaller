from __future__ import annotations
import os, threading, atexit
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from flask import (
    Flask, render_template, request, redirect, url_for, flash, jsonify
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user, login_required, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash

from tinydb import TinyDB, Query
from tinydb.storages import JSONStorage

# ─────────────────────────────────────────────
# Config / App
# ─────────────────────────────────────────────
APP_NAME = "APSMOTORSPORTS"
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'cambia-esto')
DATA_DIR = os.path.join(os.getcwd(), 'data')
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'taller.json')

_db_lock = threading.Lock()
_DB: Optional[TinyDB] = None
def db() -> TinyDB:
    """Única instancia TinyDB (JSONStorage, sin caché)."""
    global _DB
    if _DB is None:
        _DB = TinyDB(DB_PATH, storage=JSONStorage)
    return _DB

# Tablas
TABLES = ['users', 'clients', 'vehicles', 'jobs', 'notes', 'timelogs', 'seq', 'quotes']
with _db_lock:
    _ = db()
    for t in TABLES:
        _ = db().table(t)

# IDs incrementales
SEQ_KEY = 'global_seq'
def _ensure_seq():
    T = db().table('seq')
    if not T.get(Query().key == SEQ_KEY):
        T.insert({'key': SEQ_KEY, 'value': 1})
_ensure_seq()

def _next_id() -> int:
    with _db_lock:
        T = db().table('seq')
        row = T.get(Query().key == SEQ_KEY)
        if not row:
            T.insert({'key': SEQ_KEY, 'value': 1})
            row = T.get(Query().key == SEQ_KEY)
        nid = int(row['value'])
        T.update({'value': nid + 1}, Query().key == SEQ_KEY)
        return nid

# CRUD helpers
def insert(table: str, doc: Dict[str, Any]) -> int:
    doc['id'] = _next_id()
    with _db_lock:
        db().table(table).insert(doc)
    return doc['id']

def update(table: str, doc_id: int, patch: Dict[str, Any]):
    with _db_lock:
        db().table(table).update(patch, Query().id == doc_id)

def remove(table: str, doc_id: int):
    with _db_lock:
        db().table(table).remove(Query().id == doc_id)

def get_(table: str, doc_id: int) -> Optional[Dict[str, Any]]:
    return db().table(table).get(Query().id == doc_id)

def all_(table: str) -> List[Dict[str, Any]]:
    return list(db().table(table).all())

def find(table: str, **where) -> List[Dict[str, Any]]:
    q = Query()
    expr = None
    for k, v in where.items():
        e = (getattr(q, k) == v)
        expr = e if expr is None else (expr & e)
    return db().table(table).search(expr) if expr is not None else []

# Cerrar DB al salir
@atexit.register
def _close_db_on_exit():
    try:
        if '_DB' in globals() and _DB is not None:
            _DB.close()
    except Exception:
        pass

# ─────────────────────────────────────────────
# Login
# ─────────────────────────────────────────────
login_manager = LoginManager(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, d: Dict[str, Any]):
        self.d = d
    @property
    def id(self) -> str: return str(self.d['id'])
    @property
    def username(self) -> str: return self.d.get('username', '')
    @property
    def role(self) -> str: return self.d.get('role', 'mecanico')
    @property
    def full_name(self) -> str: return self.d.get('full_name', self.username)

@login_manager.user_loader
def load_user(uid: str):
    u = get_('users', int(uid))
    return User(u) if u else None

# Seed admin
if not find('users', username='admin'):
    insert('users', {
        'username': 'admin',
        'full_name': 'Administrador',
        'role': 'admin',
        'password_hash': generate_password_hash('admin123'),
        'created_at': datetime.now(timezone.utc).isoformat()
    })

# ─────────────────────────────────────────────
# Filtros Jinja y helpers
# ─────────────────────────────────────────────
def _fmt_dt(iso: str, with_time: bool = True) -> str:
    if not iso: return ""
    try:
        dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except Exception:
        return iso
    return dt.astimezone().strftime('%d/%m/%Y %H:%M' if with_time else '%d/%m/%Y')

@app.template_filter('fecha')
def fecha_f(iso: str) -> str:
    return _fmt_dt(iso, with_time=False)

@app.template_filter('fechahora')
def fechahora_f(iso: str) -> str:
    return _fmt_dt(iso, with_time=True)

STATUS_BADGES = {
    'abierto':  ('Abierto',  'bg-secondary'),
    'en_proceso': ('En proceso', 'bg-warning'),
    'listo':   ('Listo',    'bg-info'),
    'entregado': ('Entregado', 'bg-success'),
    'pausado': ('Pausado', 'bg-dark'),
}
@app.template_filter('status_badge')
def status_badge(st: str) -> str:
    label, cls = STATUS_BADGES.get(st, (st or "—", 'bg-secondary'))
    return f'<span class="badge {cls}">{label}</span>'

@app.context_processor
def inject_globals():
    return dict(
        APP_NAME=APP_NAME,
        total_clients=len(all_('clients')),
        total_vehicles=len(all_('vehicles')),
        total_jobs=len(all_('jobs'))
    )

# ─────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────
@app.get('/login')
def login():
    return render_template('login.html')

@app.post('/login')
def login_post():
    username = (request.form.get('username') or '').strip()
    password = request.form.get('password') or ''
    rows = find('users', username=username)
    u = rows[0] if rows else None
    if not u or not check_password_hash(u.get('password_hash', ''), password):
        flash('Credenciales inválidas', 'danger')
        return redirect(url_for('login'))
    login_user(User(u))
    flash(f'¡Bienvenido, {u.get("full_name", username)}!', 'success')
    return redirect(url_for('dashboard'))

@app.get('/logout')
@login_required
def logout():
    logout_user()
    flash('Sesión cerrada', 'info')
    return redirect(url_for('login'))

# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────
@app.get('/')
@login_required
def dashboard():
    jobs = all_('jobs')
    jobs.sort(key=lambda j: j.get('intake_date',''), reverse=True)
    jobs = jobs[:5]
    jobs_fmt = []
    for j in jobs:
        v = get_('vehicles', j.get('vehicle_id')) or {}
        c = get_('clients', v.get('client_id')) or {}
        jj = j.copy()
        jj['vehicle'] = v
        jj['client'] = c
        jobs_fmt.append(jj)

    vehicles = all_('vehicles')[-5:]
    vehicles_fmt = []
    for v in vehicles:
        c = get_('clients', v.get('client_id')) or {}
        vv = v.copy()
        vv['client_name'] = c.get('full_name','')
        vehicles_fmt.append(vv)

    return render_template('dashboard.html', jobs=jobs_fmt, vehicles=vehicles_fmt)

# ─────────────────────────────────────────────
# CLIENTES
# ─────────────────────────────────────────────
@app.get('/clients')
@login_required
def clients_list():
    q = (request.args.get('q') or '').strip().lower()
    clients = all_('clients')
    if q:
        clients = [c for c in clients if q in c.get('full_name','').lower()
                   or q in (c.get('phone','') or '').lower()
                   or q in (c.get('email','') or '').lower()]
    vids = all_('vehicles')
    vehicles_by_client = {}
    for v in vids:
        vehicles_by_client[v.get('client_id')] = vehicles_by_client.get(v.get('client_id'), 0) + 1
    for c in clients:
        c['vehicles_count'] = vehicles_by_client.get(c['id'], 0)
    clients.sort(key=lambda c: c.get('full_name','').lower())
    return render_template('clients_list.html', clients=clients, q=q)

@app.route('/clients/new', methods=['GET','POST'])
@login_required
def clients_new():
    if request.method == 'POST':
        full_name = request.form['full_name'].strip()
        phone     = (request.form.get('phone') or '').strip()
        email     = (request.form.get('email') or '').strip()
        brand = (request.form.get('brand') or '').strip()
        model = (request.form.get('model') or '').strip()
        plate = (request.form.get('plate') or '').strip().upper()
        color = (request.form.get('color') or '').strip()

        if not full_name or not plate:
            flash('Nombre completo y Placa son obligatorios.', 'danger')
            return redirect(url_for('clients_new'))

        if any(v for v in all_('vehicles') if v.get('plate') == plate):
            flash('Ya existe un vehículo con esa placa.', 'danger')
            return redirect(url_for('clients_new'))

        client_id = insert('clients', {
            'full_name': full_name,
            'phone': phone,
            'email': email,
            'created_at': datetime.now(timezone.utc).isoformat(),
        })

        insert('vehicles', {
            'client_id': client_id,
            'brand': brand,
            'model': model,
            'plate': plate,
            'color': color,
            'year': None,
            'vin': ''
        })

        flash('Cliente y vehículo creados', 'success')
        return redirect(url_for('clients_list'))

    return render_template('clients_form.html', client=None, action='Crear')

@app.route('/clients/<int:client_id>/edit', methods=['GET','POST'])
@login_required
def clients_edit(client_id):
    cli = get_('clients', client_id)
    if not cli:
        flash('Cliente no encontrado', 'danger')
        return redirect(url_for('clients_list'))
    if request.method == 'POST':
        patch = {
            'full_name': request.form['full_name'].strip(),
            'phone': (request.form.get('phone') or '').strip(),
            'email': (request.form.get('email') or '').strip(),
        }
        update('clients', client_id, patch)
        flash('Cliente actualizado', 'success')
        return redirect(url_for('clients_list'))
    return render_template('clients_form.html', client=cli, action='Editar')

@app.post('/clients/<int:client_id>/delete')
@login_required
def clients_delete(client_id):
    remove('clients', client_id)
    for v in [v for v in all_('vehicles') if v.get('client_id') == client_id]:
        for j in [j for j in all_('jobs') if j.get('vehicle_id') == v['id']]:
            remove('jobs', j['id'])
        remove('vehicles', v['id'])
    flash('Cliente eliminado', 'info')
    return redirect(url_for('clients_list'))

# ─────────────────────────────────────────────
# VEHÍCULOS
# ─────────────────────────────────────────────
@app.get('/vehicles')
@login_required
def vehicles_list():
    q = (request.args.get('q') or '').strip().lower()
    vehicles = all_('vehicles')
    for v in vehicles:
        c = get_('clients', v.get('client_id')) or {}
        v['client_name'] = c.get('full_name','')
    if q:
        vehicles = [v for v in vehicles if q in v.get('plate','').lower()
                    or q in v.get('client_name','').lower()
                    or q in v.get('brand','').lower()
                    or q in v.get('model','').lower()]
    vehicles.sort(key=lambda v: (v.get('plate',''), v.get('client_name','')))
    return render_template('vehicles_list.html', vehicles=vehicles, q=q)

@app.route('/vehicles/new', methods=['GET','POST'])
@login_required
def vehicles_new():
    clients = sorted(all_('clients'), key=lambda c: c.get('full_name','').lower())
    if request.method == 'POST':
        plate = request.form['plate'].upper().strip()
        if any(v for v in all_('vehicles') if v.get('plate') == plate):
            flash('Ya existe un vehículo con esa placa', 'danger')
            return redirect(url_for('vehicles_new'))
        insert('vehicles', {
            'client_id': int(request.form['client_id']),
            'plate': plate,
            'brand': request.form.get('brand','').strip(),
            'model': request.form.get('model','').strip(),
            'year': int(request.form.get('year') or 0) or None,
            'vin': request.form.get('vin','').strip(),
            'color': request.form.get('color','').strip(),
        })
        flash('Vehículo creado', 'success')
        return redirect(url_for('vehicles_list'))
    return render_template('vehicles_form.html', vehicle=None, clients=clients, action='Crear')

@app.route('/vehicles/<int:vehicle_id>/edit', methods=['GET','POST'])
@login_required
def vehicles_edit(vehicle_id):
    v = get_('vehicles', vehicle_id)
    if not v:
        flash('Vehículo no encontrado', 'danger')
        return redirect(url_for('vehicles_list'))
    clients = sorted(all_('clients'), key=lambda c: c.get('full_name','').lower())
    if request.method == 'POST':
        plate = request.form['plate'].upper().strip()
        clash = [x for x in all_('vehicles') if x.get('plate') == plate and x['id'] != vehicle_id]
        if clash:
            flash('Otra unidad ya tiene esa placa', 'danger')
            return redirect(url_for('vehicles_edit', vehicle_id=vehicle_id))
        patch = {
            'client_id': int(request.form['client_id']),
            'plate': plate,
            'brand': request.form.get('brand','').strip(),
            'model': request.form.get('model','').strip(),
            'year': int(request.form.get('year') or 0) or None,
            'vin': request.form.get('vin','').strip(),
            'color': request.form.get('color','').strip(),
        }
        update('vehicles', vehicle_id, patch)
        flash('Vehículo actualizado', 'success')
        return redirect(url_for('vehicles_list'))
    return render_template('vehicles_form.html', vehicle=v, clients=clients, action='Editar')

@app.post('/vehicles/<int:vehicle_id>/delete')
@login_required
def vehicles_delete(vehicle_id):
    remove('vehicles', vehicle_id)
    for j in [j for j in all_('jobs') if j.get('vehicle_id') == vehicle_id]:
        remove('jobs', j['id'])
    flash('Vehículo eliminado', 'info')
    return redirect(url_for('vehicles_list'))

# API: info de vehículo
@app.get('/api/vehicle/<int:vehicle_id>/info')
@login_required
def api_vehicle_info(vehicle_id):
    v = get_('vehicles', vehicle_id)
    if not v:
        return jsonify({'ok': False}), 404
    c = get_('clients', v.get('client_id')) or {}
    jobs = [j for j in all_('jobs') if j.get('vehicle_id') == vehicle_id]
    jobs.sort(key=lambda j: j.get('intake_date',''), reverse=True)
    last = jobs[0] if jobs else None
    return jsonify({
        'ok': True,
        'client_name': c.get('full_name',''),
        'last_job_date': last.get('intake_date','') if last else ''
    })

# ─────────────────────────────────────────────
# ÓRDENES DE TRABAJO
# ─────────────────────────────────────────────
@app.get('/jobs')
@login_required
def jobs_list():
    q = (request.args.get('q') or '').strip().lower()
    jobs = all_('jobs')
    out = []
    for j in jobs:
        v = get_('vehicles', j['vehicle_id']) or {}
        c = get_('clients', v.get('client_id')) or {}
        j2 = j.copy()
        j2['vehicle'] = v
        j2['client'] = c
        out.append(j2)
    if q:
        out = [j for j in out if
               q in (j.get('reason','') or '').lower()
               or q in (j.get('description','') or '').lower()
               or q in (j.get('status','') or '').lower()
               or q in (j['vehicle'].get('plate','') or '').lower()
               or q in (j['client'].get('full_name','') or '').lower()
               ]
    out.sort(key=lambda j: j.get('intake_date',''), reverse=True)
    return render_template('jobs_list.html', jobs=out, q=q)

@app.route('/jobs/new', methods=['GET','POST'])
@login_required
def jobs_new():
    vehicles = sorted(all_('vehicles'), key=lambda v: v.get('plate',''))
    if request.method == 'POST':
        def cb(name): return 1 if request.form.get(name) == 'on' else 0
        job = {
            'vehicle_id': int(request.form['vehicle_id']),
            'mechanic_id': int(current_user.id),
            'reason': request.form.get('reason','').strip(),
            'description': request.form.get('description','').strip(),
            'status': request.form.get('status','abierto'),
            'intake_date': datetime.now(timezone.utc).isoformat(),
            'delivery_date': None,
            'odometer_km': int(request.form.get('odometer_km') or 0) or None,
            'fuel_level': request.form.get('fuel_level',''),
            'checklist': {
                'antenas': cb('chk_antenas'),
                'botiquin': cb('chk_botiquin'),
                'documentos': cb('chk_documentos'),
                'encendedor': cb('chk_encendedor'),
                'extintor': cb('chk_extintor'),
                'gato': cb('chk_gato'),
                'herramientas': cb('chk_herramientas'),
                'llave1': cb('chk_llave1'),
                'llave2': cb('chk_llave2'),
                'llave_rueda': cb('chk_llave_rueda'),
                'pisos': cb('chk_pisos'),
                'rueda_repuesto': cb('chk_rueda_repuesto'),
                'tag': cb('chk_tag'),
                'tapas': cb('chk_tapas'),
                'triangulos': cb('chk_triangulos'),
            }
        }
        jid = insert('jobs', job)
        flash('OT creada', 'success')
        return redirect(url_for('job_detail', job_id=jid))
    return render_template('jobs_form.html', vehicles=vehicles)

@app.get('/jobs/<int:job_id>')
@login_required
def job_detail(job_id):
    j = get_('jobs', job_id)
    if not j:
        flash('OT no encontrada', 'danger')
        return redirect(url_for('jobs_list'))
    v = get_('vehicles', j['vehicle_id']) or {}
    c = get_('clients', v.get('client_id')) or {}
    j2 = j.copy()
    j2['vehicle'] = v
    j2['client'] = c
    notes = [n for n in all_('notes') if n.get('job_id') == job_id]
    notes.sort(key=lambda n: n.get('created_at',''), reverse=True)
    j2['notes'] = notes
    return render_template('job_detail.html', job=j2, vehicle=v, client=c)

@app.post('/jobs/<int:job_id>/status')
@login_required
def job_change_status(job_id):
    st = request.form.get('status','abierto')
    patch = {'status': st}
    if st == 'entregado':
        patch['delivery_date'] = datetime.now(timezone.utc).isoformat()
    update('jobs', job_id, patch)
    flash('Estado actualizado', 'success')
    return redirect(url_for('job_detail', job_id=job_id))

@app.post('/jobs/<int:job_id>/notes')
@login_required
def job_add_note(job_id):
    content = (request.form.get('content') or '').strip()
    if content:
        insert('notes', {
            'job_id': job_id,
            'user_id': int(current_user.id),
            'content': content,
            'created_at': datetime.now(timezone.utc).isoformat()
        })
        flash('Nota agregada', 'success')
    return redirect(url_for('job_detail', job_id=job_id))

# Cotización más reciente asociada a una OT
def _latest_quote_for_job(job_id: int) -> Optional[Dict[str, Any]]:
    qs = [q for q in all_('quotes') if q.get('job_id') == job_id]
    qs.sort(key=lambda x: x.get('created_at',''), reverse=True)
    return qs[0] if qs else None

# Boleta de OT
@app.get('/jobs/<int:job_id>/print')
@login_required
def job_print(job_id):
    j = get_('jobs', job_id)
    if not j:
        flash('OT no encontrada', 'danger'); return redirect(url_for('jobs_list'))
    v = get_('vehicles', j['vehicle_id']) or {}
    c = get_('clients', v.get('client_id')) or {}

    tasks: List[str] = []
    if j.get('description'):
        tasks.extend([x.strip() for x in j.get('description','').splitlines() if x.strip()])
    if not tasks:
        notes = [n for n in all_('notes') if n.get('job_id') == job_id]
        notes.sort(key=lambda n: n.get('created_at',''))
        tasks = [n.get('content','') for n in notes if n.get('content')]

    q = _latest_quote_for_job(job_id)
    subtotal = q.get('subtotal', 0.0) if q else 0.0
    igv      = q.get('igv', 0.0) if q else 0.0
    total    = q.get('total', 0.0) if q else 0.0

    ctx = {
        'id': j['id'],
        'client_name': c.get('full_name',''),
        'vehicle_brand': v.get('brand',''),
        'vehicle_model': v.get('model',''),
        'vehicle_plate': v.get('plate',''),
        'date': _fmt_dt(j.get('delivery_date') or j.get('intake_date'), with_time=False),
        'tasks': tasks,
        'subtotal': subtotal,
        'igv': igv,
        'total': total,
    }
    return render_template('job_print.html', job=ctx)

# ─────────────────────────────────────────────
# COTIZACIONES
# ─────────────────────────────────────────────

def _parse_items_from_form(frm):
    descs = frm.getlist('item_desc[]')
    qtys  = frm.getlist('item_qty[]')
    prices= frm.getlist('item_price[]')
    items = []
    for d, q, p in zip(descs, qtys, prices):
        d = (d or '').strip()
        if not d: continue
        try:
            qf = float(q or 0); pf = float(p or 0)
        except: qf = 0.0; pf = 0.0
        items.append({'desc': d, 'qty': qf, 'unit_price': pf, 'total': round(qf*pf, 2)})
    return items

def _totals(items, require_invoice: bool, igv_rate: float = 0.18):
    subtotal = round(sum(i['total'] for i in items), 2)
    igv = round(subtotal * (igv_rate if require_invoice else 0.0), 2)
    total = round(subtotal + igv, 2)
    return subtotal, igv, total

def _meta_from_form(frm):
    keys = [
        'version','fecha','ruc','contacto','conductor',
        'marca','modelo','serie','serie_motor','combustible',
        'kilometraje','entrega_tecnica'
    ]
    meta = {}
    for k in keys:
        v = (frm.get(f'meta_{k}') or '').strip()
        if v:
            meta[k] = v
    return meta

@app.get('/quotes')
@login_required
def quotes_list():
    """Listado de cotizaciones (endpoint que faltaba)."""
    quotes = all_('quotes')
    # Asegura totales por si alguna cotización vieja no los trae calculados
    for q in quotes:
        items = q.get('items', [])
        st = round(sum((i.get('qty',0) or 0) * (i.get('unit_price',0) or 0) for i in items), 2)
        igv_rate = 0.18 if q.get('require_invoice') else 0.0
        q['subtotal'] = q.get('subtotal', st)
        q['igv']      = q.get('igv', round(q['subtotal'] * igv_rate, 2))
        q['total']    = q.get('total', round(q['subtotal'] + q['igv'], 2))
    quotes.sort(key=lambda x: x.get('created_at',''), reverse=True)
    return render_template('quotes_list.html', quotes=quotes)

@app.get('/quotes/new')
@login_required
def quotes_new():
    job_id = request.args.get('job_id', type=int)
    return render_template('quotes_form.html', job_id=job_id)

@app.post('/quotes/new')
@login_required
def quotes_create():
    # Ítems dinámicos
    items = _parse_items_from_form(request.form)
    require_invoice = 1 if request.form.get('need_invoice') in ('1','on','true','yes') else 0

    # Compatibilidad modo simple (por si lo usas)
    if not items:
        client_name = (request.form.get('client_name') or '').strip()
        vehicle_lbl = (request.form.get('vehicle') or '').strip()
        services_txt= (request.form.get('services') or '').strip()
        amount      = float(request.form.get('amount') or 0)
        if amount:
            items = [{'desc': 'Servicios', 'qty': 1.0, 'unit_price': amount, 'total': round(amount, 2)}]
        client_id = None
        vehicle_id = None
    else:
        client_name = (request.form.get('client_name') or '').strip()
        vehicle_lbl = (request.form.get('vehicle') or '').strip()
        services_txt= (request.form.get('services') or '').strip()
        client_id   = request.form.get('client_id', type=int)
        vehicle_id  = request.form.get('vehicle_id', type=int)

    subtotal, igv, total = _totals(items, bool(require_invoice))
    job_id = request.form.get('job_id', type=int)
    meta = _meta_from_form(request.form)

    quote = {
        'job_id': job_id,
        'client_id': client_id,
        'vehicle_id': vehicle_id,
        'client_name': client_name,
        'vehicle_label': vehicle_lbl,
        'services_lines': [s.strip() for s in services_txt.splitlines() if s.strip()],
        'require_invoice': int(require_invoice),
        'igv_rate': 0.18,
        'currency': 'PEN',
        'items': items,
        'subtotal': subtotal,
        'igv': igv,
        'total': total,
        'meta': meta,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'created_by': int(current_user.id),
    }
    qid = insert('quotes', quote)
    flash('Cotización creada', 'success')
    return redirect(url_for('quote_print', quote_id=qid))

@app.get('/quotes/<int:quote_id>/print')
@login_required
def quote_print(quote_id):
    q = get_('quotes', quote_id)
    if not q:
        flash('Cotización no encontrada', 'danger')
        return redirect(url_for('dashboard'))

    items = q.get('items', [])
    subtotal, igv, total = _totals(items, bool(q.get('require_invoice')))
    q['subtotal'], q['igv'], q['total'] = subtotal, igv, total

    v = get_('vehicles', q.get('vehicle_id') or 0) or {}
    c = get_('clients', q.get('client_id') or 0) or {}
    if not c and q.get('client_name'):
        c = {'full_name': q.get('client_name')}
    if not v and q.get('vehicle_label'):
        v = {'plate': q.get('vehicle_label'), 'brand': (q.get('meta',{}) or {}).get('marca',''), 'model': (q.get('meta',{}) or {}).get('modelo','')}

    return render_template('quote_print.html', quote=q, client=c, vehicle=v)

@app.get('/quotes/<int:quote_id>/duplicate')
@login_required
def quote_duplicate(quote_id):
    q = get_('quotes', quote_id)
    if not q:
        flash('Cotización no encontrada', 'danger')
        return redirect(url_for('dashboard'))
    base = {k: q.get(k) for k in (
        'job_id','client_id','vehicle_id','client_name','vehicle_label',
        'services_lines','require_invoice','igv_rate','currency','items','meta'
    )}
    subtotal, igv, total = _totals(base.get('items', []), bool(base.get('require_invoice')))
    base.update({
        'subtotal': subtotal, 'igv': igv, 'total': total,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'created_by': int(current_user.id),
    })
    new_id = insert('quotes', base)
    flash(f'Cotización duplicada #{quote_id} → #{new_id}', 'success')
    return redirect(url_for('quote_print', quote_id=new_id))

def _quote_as_text(q: dict) -> str:
    lines = []
    if q.get('services_lines'):
        lines.append("Servicios solicitados:")
        lines += [f"- {s}" for s in q['services_lines']]
        lines.append("")
    elif q.get('items'):
        lines.append("Ítems cotizados:")
        for it in q['items']:
            lines.append(f"- {it.get('desc','')} · cant: {it.get('qty',1):.2f} · p.u.: S/ {it.get('unit_price',0):.2f} · total: S/ {it.get('total',0):.2f}")
        lines.append("")
    lines.append(f"Subtotal: S/ {q.get('subtotal', 0):.2f}")
    if q.get('require_invoice'):
        lines.append(f"IGV 18%: S/ {q.get('igv', 0):.2f}")
    else:
        lines.append("IGV: 0% (no factura)")
    lines.append(f"TOTAL: S/ {q.get('total', 0):.2f}")
    return "\n".join(lines)

@app.get('/quotes/<int:quote_id>/to-job')
@login_required
def quote_to_job(quote_id):
    q = get_('quotes', quote_id)
    if not q:
        flash('Cotización no encontrada', 'danger')
        return redirect(url_for('dashboard'))

    veh_id = q.get('vehicle_id')
    cli_id = q.get('client_id')
    if not veh_id or not cli_id:
        flash('La cotización no tiene cliente/vehículo asociado. Crea la OT manualmente.', 'warning')
        return redirect(url_for('quote_print', quote_id=quote_id))

    desc = f"Creado desde Cotización #{quote_id}\n\n" + _quote_as_text(q)
    job = {
        'vehicle_id': int(veh_id),
        'mechanic_id': int(current_user.id),
        'reason': f'Cotización #{quote_id}',
        'description': desc,
        'status': 'abierto',
        'intake_date': datetime.now(timezone.utc).isoformat(),
        'delivery_date': None,
        'odometer_km': None,
        'fuel_level': '',
        'checklist': { 'antenas':0, 'botiquin':0, 'documentos':0, 'encendedor':0, 'extintor':0,
                       'gato':0, 'herramientas':0, 'llave1':0, 'llave2':0, 'llave_rueda':0,
                       'pisos':0, 'rueda_repuesto':0, 'tag':0, 'tapas':0, 'triangulos':0 }
    }
    job_id = insert('jobs', job)

    insert('notes', {
        'job_id': job_id,
        'user_id': int(current_user.id),
        'content': f'OT creada a partir de la Cotización #{quote_id}.',
        'created_at': datetime.now(timezone.utc).isoformat()
    })

    flash(f'Cotización #{quote_id} convertida en OT #{job_id}', 'success')
    return redirect(url_for('job_detail', job_id=job_id))

# ─────────────────────────────────────────────
# Errores
# ─────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', code=404, title="No encontrado", msg="La página solicitada no existe."), 404

@app.errorhandler(500)
def server_error(e):
    return render_template('error.html', code=500, title="Error del servidor", msg="Ocurrió un error inesperado."), 500

# ─────────────────────────────────────────────
# Run (sin reloader)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
