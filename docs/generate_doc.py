"""
Genera el documento técnico-funcional de tests dbt para wh_transform.
Ejecutar: python3 generate_doc.py
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "dbt_tests_technical_doc.pdf")

# ── Paleta de colores ──────────────────────────────────────────────────────────
C_DARK       = HexColor("#1A1A2E")
C_PRIMARY    = HexColor("#16213E")
C_ACCENT     = HexColor("#0F3460")
C_HIGHLIGHT  = HexColor("#E94560")
C_GREEN      = HexColor("#2ECC71")
C_ORANGE     = HexColor("#F39C12")
C_RED        = HexColor("#E74C3C")
C_BLUE       = HexColor("#3498DB")
C_LIGHT_GRAY = HexColor("#F8F9FA")
C_MED_GRAY   = HexColor("#DEE2E6")
C_TEXT       = HexColor("#212529")
C_TEXT_LIGHT = HexColor("#6C757D")
C_CODE_BG    = HexColor("#F1F3F4")
C_CODE_TEXT  = HexColor("#D63384")

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm

# ── Estilos ────────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

def make_style(name, parent="Normal", **kwargs):
    return ParagraphStyle(name, parent=styles[parent], **kwargs)

sTitle       = make_style("sTitle",      fontSize=28, textColor=colors.white,
                           leading=34, spaceAfter=6, alignment=TA_CENTER, fontName="Helvetica-Bold")
sSubtitle    = make_style("sSubtitle",   fontSize=13, textColor=HexColor("#B0C4DE"),
                           leading=18, spaceAfter=4, alignment=TA_CENTER)
sMeta        = make_style("sMeta",       fontSize=10, textColor=HexColor("#90A4AE"),
                           leading=14, alignment=TA_CENTER)
sH1          = make_style("sH1",         fontSize=18, textColor=C_ACCENT,
                           leading=24, spaceBefore=18, spaceAfter=8, fontName="Helvetica-Bold")
sH2          = make_style("sH2",         fontSize=14, textColor=C_PRIMARY,
                           leading=20, spaceBefore=14, spaceAfter=6, fontName="Helvetica-Bold")
sH3          = make_style("sH3",         fontSize=12, textColor=C_ACCENT,
                           leading=16, spaceBefore=10, spaceAfter=4, fontName="Helvetica-Bold")
sBody        = make_style("sBody",       fontSize=10, textColor=C_TEXT,
                           leading=15, spaceAfter=4, alignment=TA_JUSTIFY)
sBodyLeft    = make_style("sBodyLeft",   fontSize=10, textColor=C_TEXT,
                           leading=15, spaceAfter=4)
sBullet      = make_style("sBullet",     fontSize=10, textColor=C_TEXT,
                           leading=14, spaceAfter=2, leftIndent=14, bulletIndent=4)
sCode        = make_style("sCode",       fontSize=8.5, textColor=HexColor("#1A1A2E"),
                           leading=13, fontName="Courier", backColor=C_CODE_BG,
                           leftIndent=10, rightIndent=10, spaceAfter=6, spaceBefore=4)
sLabel       = make_style("sLabel",      fontSize=9, textColor=C_TEXT_LIGHT,
                           leading=12, fontName="Helvetica-Bold")
sCaption     = make_style("sCaption",    fontSize=8.5, textColor=C_TEXT_LIGHT,
                           leading=12, alignment=TA_CENTER, spaceAfter=8)
sAlert       = make_style("sAlert",      fontSize=10, textColor=HexColor("#721c24"),
                           leading=14, backColor=HexColor("#F8D7DA"), leftIndent=10, rightIndent=10,
                           spaceBefore=4, spaceAfter=6)
sNote        = make_style("sNote",       fontSize=10, textColor=HexColor("#0C5460"),
                           leading=14, backColor=HexColor("#D1ECF1"), leftIndent=10, rightIndent=10,
                           spaceBefore=4, spaceAfter=6)
sSuccess     = make_style("sSuccess",    fontSize=10, textColor=HexColor("#155724"),
                           leading=14, backColor=HexColor("#D4EDDA"), leftIndent=10, rightIndent=10,
                           spaceBefore=4, spaceAfter=6)

# ── Helpers ────────────────────────────────────────────────────────────────────

def hr(color=C_MED_GRAY, thickness=0.5):
    return HRFlowable(width="100%", thickness=thickness, color=color, spaceAfter=6, spaceBefore=6)

def sp(h=0.3):
    return Spacer(1, h * cm)

def h1(text):
    return Paragraph(text, sH1)

def h2(text):
    return Paragraph(text, sH2)

def h3(text):
    return Paragraph(text, sH3)

def body(text):
    return Paragraph(text, sBody)

def body_left(text):
    return Paragraph(text, sBodyLeft)

def bullet(text):
    return Paragraph(f"• {text}", sBullet)

def code(text):
    # Escape < > &
    t = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return Paragraph(t, sCode)

def label(text):
    return Paragraph(text, sLabel)

def note(text):
    return Paragraph(f"ℹ  {text}", sNote)

def alert(text):
    return Paragraph(f"⚠  {text}", sAlert)

def success(text):
    return Paragraph(f"✓  {text}", sSuccess)

def section_box(title, color=C_ACCENT):
    data = [[Paragraph(title, make_style("bx", fontSize=13, textColor=colors.white,
                                          fontName="Helvetica-Bold", leading=18))]]
    t = Table(data, colWidths=[PAGE_W - 2*MARGIN])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), color),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LEFTPADDING",   (0,0), (-1,-1), 12),
        ("RIGHTPADDING",  (0,0), (-1,-1), 12),
    ]))
    return t

def status_badge(text, color):
    st = make_style("badge", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold",
                    leading=12, backColor=color, leftIndent=4, rightIndent=4)
    return Paragraph(f" {text} ", st)

def styled_table(headers, rows, col_widths=None):
    data = [headers] + rows
    if col_widths is None:
        col_widths = [(PAGE_W - 2*MARGIN) / len(headers)] * len(headers)
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  C_PRIMARY),
        ("TEXTCOLOR",     (0,0), (-1,0),  colors.white),
        ("FONTNAME",      (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0), (-1,0),  9),
        ("ALIGN",         (0,0), (-1,0),  "CENTER"),
        ("TOPPADDING",    (0,0), (-1,0),  7),
        ("BOTTOMPADDING", (0,0), (-1,0),  7),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [C_LIGHT_GRAY, colors.white]),
        ("FONTSIZE",      (0,1), (-1,-1), 9),
        ("TOPPADDING",    (0,1), (-1,-1), 5),
        ("BOTTOMPADDING", (0,1), (-1,-1), 5),
        ("LEFTPADDING",   (0,0), (-1,-1), 7),
        ("RIGHTPADDING",  (0,0), (-1,-1), 7),
        ("GRID",          (0,0), (-1,-1), 0.4, C_MED_GRAY),
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
        ("WORDWRAP",      (0,0), (-1,-1), True),
    ]))
    return t

# ── Page template con header/footer ───────────────────────────────────────────

class DocCanvas:
    def __init__(self):
        self.page_num = 0

    def on_page(self, canv, doc):
        self.page_num += 1
        canv.saveState()
        # Header bar
        canv.setFillColor(C_PRIMARY)
        canv.rect(0, PAGE_H - 1.2*cm, PAGE_W, 1.2*cm, fill=1, stroke=0)
        canv.setFillColor(colors.white)
        canv.setFont("Helvetica", 8)
        canv.drawString(MARGIN, PAGE_H - 0.75*cm, "wh_transform — Documento Técnico de Tests dbt")
        canv.drawRightString(PAGE_W - MARGIN, PAGE_H - 0.75*cm, "CONFIDENCIAL")
        # Footer
        canv.setFillColor(C_MED_GRAY)
        canv.rect(0, 0, PAGE_W, 0.9*cm, fill=1, stroke=0)
        canv.setFillColor(C_TEXT_LIGHT)
        canv.setFont("Helvetica", 8)
        canv.drawString(MARGIN, 0.32*cm, "© 2026 — wh_transform Data Team")
        canv.drawRightString(PAGE_W - MARGIN, 0.32*cm, f"Página {self.page_num}")
        canv.restoreState()


# ── CONTENIDO ──────────────────────────────────────────────────────────────────

def build_content():
    story = []
    dc = DocCanvas()

    # ════════════════════════════════════════════════════════════
    # PORTADA
    # ════════════════════════════════════════════════════════════
    cover_data = [[
        Paragraph("", sTitle)
    ]]

    # Cover box
    cover = Table([[
        Paragraph("wh_transform", make_style("ct1", fontSize=11, textColor=HexColor("#90CAF9"),
                                              fontName="Helvetica-Bold", leading=14, alignment=TA_CENTER)),
        Spacer(1, 0.2*cm),
    ]], colWidths=[PAGE_W - 2*MARGIN])

    # Full cover block
    cover_block = Table([[
        Paragraph("Documento Técnico-Funcional", make_style("cvt", fontSize=9,
                   textColor=HexColor("#90CAF9"), fontName="Helvetica-Bold",
                   leading=12, alignment=TA_CENTER, spaceAfter=8)),
        Paragraph("Tests de Calidad de Datos dbt", sTitle),
        Spacer(1, 0.3*cm),
        Paragraph("Proyecto: wh_transform — Azure Synapse Analytics", sSubtitle),
        Spacer(1, 0.5*cm),
        HRFlowable(width="60%", thickness=1, color=HexColor("#E94560"),
                   spaceAfter=10, spaceBefore=10, hAlign="CENTER"),
        Spacer(1, 0.4*cm),
        Paragraph("Fecha de ejecución: 11 de junio de 2026", sMeta),
        Paragraph("Versión: 1.0  ·  Branch: feature/dbt-tests-data-quality", sMeta),
        Paragraph("Target: ati_datawarehouse — ati-cdw-synapse.sql.azuresynapse.net", sMeta),
        Spacer(1, 1.5*cm),
        Paragraph("Resultado de la última ejecución", make_style("rl", fontSize=11,
                   textColor=HexColor("#B0C4DE"), fontName="Helvetica-Bold",
                   leading=14, alignment=TA_CENTER)),
        Spacer(1, 0.3*cm),
    ]], colWidths=[PAGE_W - 2*MARGIN])
    cover_block.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), C_DARK),
        ("TOPPADDING",    (0,0), (-1,-1), 0),
        ("BOTTOMPADDING", (0,0), (-1,-1), 0),
        ("LEFTPADDING",   (0,0), (-1,-1), 2*cm),
        ("RIGHTPADDING",  (0,0), (-1,-1), 2*cm),
        ("ALIGN",         (0,0), (-1,-1), "CENTER"),
    ]))

    # KPI row on cover
    kpi_data = [[
        Paragraph("148\nTests Totales", make_style("kpi", fontSize=22, textColor=colors.white,
                   fontName="Helvetica-Bold", leading=28, alignment=TA_CENTER)),
        Paragraph("126\nPASS ✓", make_style("kpig", fontSize=22, textColor=C_GREEN,
                   fontName="Helvetica-Bold", leading=28, alignment=TA_CENTER)),
        Paragraph("22\nERROR ✗", make_style("kpir", fontSize=22, textColor=C_RED,
                   fontName="Helvetica-Bold", leading=28, alignment=TA_CENTER)),
        Paragraph("85%\nCobertura", make_style("kpib", fontSize=22, textColor=C_BLUE,
                   fontName="Helvetica-Bold", leading=28, alignment=TA_CENTER)),
    ]]
    kpi_table = Table(kpi_data, colWidths=[(PAGE_W - 2*MARGIN)/4]*4)
    kpi_table.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), HexColor("#16213E")),
        ("TOPPADDING",    (0,0), (-1,-1), 14),
        ("BOTTOMPADDING", (0,0), (-1,-1), 14),
        ("GRID",          (0,0), (-1,-1), 0.5, HexColor("#2C3E50")),
    ]))

    story.append(cover_block)
    story.append(kpi_table)
    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 1. INTRODUCCIÓN
    # ════════════════════════════════════════════════════════════
    story.append(h1("1. Introducción"))
    story.append(hr())
    story.append(body(
        "Este documento describe el conjunto de tests de calidad de datos implementados en el proyecto "
        "<b>wh_transform</b>, una capa de transformación dbt Core que opera sobre <b>Azure Synapse Analytics "
        "(Dedicated SQL Pool, dialecto T-SQL)</b>. El objetivo de los tests es garantizar la integridad, "
        "consistencia y corrección de los datos en todos los dominios del data warehouse."
    ))
    story.append(sp(0.3))
    story.append(body(
        "Los tests cubren cuatro dominios productivos: <b>ATI Datawarehouse</b> (migración Korbato), "
        "<b>Sonnell</b> (pipeline de facturación), <b>AMA</b> (dashboard de flota Geotab) y "
        "<b>Monitoring</b> (salud de pipelines de ingesta). Existen dos tipos de tests: "
        "<b>schema tests</b> declarados en archivos YAML y <b>tests singulares</b> escritos en SQL."
    ))
    story.append(sp(0.5))

    # Contexto técnico
    story.append(h2("1.1 Contexto técnico"))
    ctx_data = [
        ["Parámetro", "Valor"],
        ["Herramienta", "dbt Core"],
        ["Adapter", "dbt-synapse (T-SQL / Azure Synapse Analytics)"],
        ["Data Warehouse", "ati_datawarehouse — ati-cdw-synapse.sql.azuresynapse.net"],
        ["Package adicional", "dbt-labs/dbt_utils (unique_combination_of_columns)"],
        ["Rama del repositorio", "feature/dbt-tests-data-quality"],
        ["Fecha de implementación", "10 de junio de 2026"],
        ["Última ejecución", "11 de junio de 2026 — 10:27:47 UTC"],
    ]
    t = styled_table(
        [Paragraph("<b>Parámetro</b>", sBodyLeft), Paragraph("<b>Valor</b>", sBodyLeft)],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1], sBodyLeft)] for r in ctx_data[1:]],
        col_widths=[5*cm, PAGE_W - 2*MARGIN - 5*cm]
    )
    story.append(t)
    story.append(sp(0.5))

    # Tipos de tests
    story.append(h2("1.2 Tipos de tests en dbt"))
    story.append(body(
        "<b>Schema tests (genéricos)</b>: declarados en archivos <code>schema.yml</code>. dbt genera "
        "automáticamente el SQL de validación. Tipos disponibles: <i>unique</i>, <i>not_null</i>, "
        "<i>accepted_values</i>, <i>relationships</i>, y tests de <i>dbt_utils</i> como "
        "<i>unique_combination_of_columns</i>."
    ))
    story.append(sp(0.2))
    story.append(body(
        "<b>Tests singulares</b>: archivos <code>.sql</code> en la carpeta <code>tests/</code>. Contienen "
        "una query SQL que retorna las filas que <i>violan</i> la regla de negocio. "
        "Si el SELECT devuelve 0 filas → test PASA. Si devuelve filas → test FALLA con el detalle exacto."
    ))
    story.append(sp(0.2))
    story.append(note(
        "Todos los tests singulares usan ref() para referenciar los modelos, garantizando que dbt "
        "resuelva correctamente el schema y el alias de cada tabla."
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 2. SCHEMA TESTS — ATI DATAWAREHOUSE
    # ════════════════════════════════════════════════════════════
    story.append(section_box("2. Schema Tests — ATI Datawarehouse", C_ACCENT))
    story.append(sp(0.3))
    story.append(body(
        "Archivo: <code>models/ati_datawarehouse/schema.yml</code>. Cubre los 7 modelos del dominio "
        "Korbato migrado al data warehouse. Los modelos son incrementales (append + DELETE pre_hook) "
        "con ventana de 1 día. Los tests validan integridad estructural: claves primarias únicas y "
        "relaciones FK entre entidades."
    ))
    story.append(sp(0.4))

    # Modelo: fleet
    story.append(h2("2.1 fleet"))
    story.append(label("Materialización: table  ·  Distribución: HASH(fleet_key)  ·  Índice: CCI"))
    story.append(body("Grain: una fila por flota (fleet_key único). Deduplicado por rn=1 en staging_fleet."))
    ati_fleet = [
        ["Columna", "Test", "Propósito"],
        ["fleet_key", "unique + not_null", "PK — identificador único de flota. Duplicados romperían todos los JOINs downstream."],
        ["fleet_name", "not_null", "Nombre obligatorio. Un NULL indica ingesta incompleta del archivo fuente."],
        ["operator_id", "not_null", "FK lógica al operador. NULL indica registro huérfano."],
        ["_md_processed_at", "not_null", "Metadata de ingesta obligatoria para trazabilidad."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_fleet[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_fleet[1:]],
        col_widths=[3.5*cm, 4*cm, PAGE_W - 2*MARGIN - 7.5*cm]
    ))

    story.append(h2("2.2 vehicle_day"))
    story.append(label("Materialización: incremental (append)  ·  Distribución: HASH(svc_date)"))
    story.append(body("Grain: vehicle_day_key único (vehículo × día de servicio)."))
    ati_vd = [
        ["Columna", "Test", "Propósito"],
        ["vehicle_day_key", "unique + not_null", "PK de la entidad. Duplicados causarían conteo incorrecto de días operativos."],
        ["svc_date", "not_null", "Fecha de servicio. Necesaria para el filtro incremental."],
        ["fleet_key", "not_null + relationships → fleet", "FK a fleet. Valida que cada vehículo-día pertenezca a una flota registrada."],
        ["vehicle_id", "not_null", "Identificador del vehículo físico."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th2", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_vd[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_vd[1:]],
        col_widths=[3.5*cm, 4.5*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))

    story.append(h2("2.3 trip"))
    story.append(label("Materialización: incremental (append)  ·  Distribución: HASH(trip_key)"))
    story.append(body("Grain: trip_key único (viaje individual)."))
    ati_trip = [
        ["Columna", "Test", "Propósito"],
        ["trip_key", "unique + not_null", "PK del viaje. Duplicados distorsionarían métricas de cantidad de viajes."],
        ["svc_date", "not_null", "Fecha de servicio para filtro incremental."],
        ["vehicle_day_key", "not_null + relationships → vehicle_day", "FK a vehicle_day. Un trip sin vehicle_day es un viaje sin vehículo asignado."],
        ["route_id", "not_null", "Ruta operada. NULL implica viaje sin ruta — inválido para cualquier análisis."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th3", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_trip[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_trip[1:]],
        col_widths=[3.5*cm, 4.5*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))
    story.append(alert(
        "ESTADO ACTUAL: not_null en trip_key y vehicle_day_key están FALLANDO. "
        "Hay registros con NULLs en la tabla trip, originados en la fuente external.trip (data lake)."
    ))

    story.append(h2("2.4 visit"))
    story.append(label("Materialización: incremental (append)  ·  Distribución: HASH(svc_date)"))
    story.append(body("Grain: visit_key único (visita individual a una parada)."))
    ati_visit = [
        ["Columna", "Test", "Propósito"],
        ["visit_key", "unique + not_null", "PK de la visita. Duplicados inflarían conteos de pasajeros."],
        ["svc_date", "not_null", "Fecha de servicio."],
        ["trip_key", "not_null + relationships → trip", "FK a trip. Una visita sin viaje es un registro huérfano."],
        ["stop_id", "not_null", "Parada visitada. NULL invalida análisis de OTP y APC."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th4", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_visit[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_visit[1:]],
        col_widths=[3.5*cm, 4.5*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))
    story.append(alert(
        "ESTADO ACTUAL: unique en visit_key y relationships visit → trip están FALLANDO. "
        "Causa probable: visit_keys duplicados y trip_keys huérfanos derivados de los NULLs en trip."
    ))

    story.append(h2("2.5 pattern"))
    story.append(label("Materialización: table  ·  Distribución: HASH(pattern_id)"))
    ati_pat = [
        ["Columna", "Test", "Propósito"],
        ["pattern_id", "unique + not_null", "PK del patrón de ruta. Unicidad garantiza integridad de pattern_stop."],
        ["route_id", "not_null", "Ruta a la que pertenece el patrón."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th5", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_pat[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_pat[1:]],
        col_widths=[3.5*cm, 4*cm, PAGE_W - 2*MARGIN - 7.5*cm]
    ))

    story.append(h2("2.6 pattern_stop"))
    story.append(label("Materialización: table  ·  Distribución: HASH(pattern_id)"))
    story.append(body("Grain: (pattern_id, stop_seq) — cada parada dentro de un patrón."))
    ati_ps = [
        ["Test", "Tipo", "Propósito"],
        ["unique_combination_of_columns\n(pattern_id, stop_seq)", "dbt_utils", "PK compuesta. Garantiza que cada posición dentro de un patrón sea única."],
        ["pattern_id: not_null + relationships → pattern", "generic", "FK a pattern. Una parada sin patrón padre es un registro inválido."],
        ["stop_seq: not_null", "generic", "Secuencia de parada obligatoria para ordenar recorridos."],
        ["stop_id: not_null", "generic", "Identificador de la parada física."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th6", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_ps[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_ps[1:]],
        col_widths=[5.5*cm, 2.5*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))

    story.append(h2("2.7 Trip_Sch_Match"))
    story.append(label("Materialización: incremental (append)  ·  Distribución: ROUND_ROBIN  ·  Índice: HEAP"))
    story.append(body("Grain: (svc_date, trip_key) — matcheo de viaje real contra trip programado (GTFS)."))
    ati_tsm = [
        ["Test", "Tipo", "Propósito"],
        ["unique_combination_of_columns\n(svc_date, trip_key)", "dbt_utils", "PK compuesta. Un viaje solo puede matchear una vez por día."],
        ["svc_date: not_null", "generic", "Fecha de servicio requerida para el matcheo temporal."],
        ["trip_key: not_null + relationships → trip", "generic", "FK a trip. El matcheo debe apuntar a un viaje real existente."],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("th7", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in ati_tsm[0]],
        [[Paragraph(r[0], sCode), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in ati_tsm[1:]],
        col_widths=[5.5*cm, 2.5*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))
    story.append(alert(
        "ESTADO ACTUAL: relationships Trip_Sch_Match → trip está FALLANDO. "
        "Hay trip_keys en Trip_Sch_Match que no tienen registro padre en trip. "
        "Consecuencia directa de los NULLs en trip."
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 3. TESTS SINGULARES
    # ════════════════════════════════════════════════════════════
    story.append(section_box("3. Tests Singulares — Reglas de Negocio", C_HIGHLIGHT))
    story.append(sp(0.3))
    story.append(body(
        "Los tests singulares validan lógica de negocio que los tests genéricos no pueden capturar. "
        "Cada archivo <code>.sql</code> en <code>tests/</code> implementa una regla específica del dominio. "
        "En todos los casos: <b>0 filas = PASS, filas retornadas = FAIL</b> con los registros problemáticos."
    ))
    story.append(sp(0.5))

    # --- Test 1
    story.append(KeepTogether([
        section_box("Test 1: assert_sonnell_invoice_non_negative", HexColor("#2C3E50")),
        sp(0.2),
        label("Archivo: tests/assert_sonnell_invoice_non_negative.sql  ·  Dominio: Sonnell  ·  Fase: P0"),
        sp(0.15),
        h3("Propósito"),
        body(
            "Garantiza que ninguna línea de factura de tipo <b>Regular</b> tenga un total monetario negativo. "
            "En el modelo de negocio de Sonnell, los cobros al cliente siempre son positivos. "
            "Los créditos o penalidades se registran <i>exclusivamente</i> en líneas de tipo <b>Offset</b>. "
            "Un <code>Total &lt; 0</code> en una línea Regular indica un error de cálculo en el pipeline "
            "de facturación."
        ),
        sp(0.2),
        h3("Modelo validado"),
        body("<code>fct_sonnell_invoice_totals</code> (alias: <i>SonnellInvoiceTotals</i>, schema <i>dbo</i>)"),
        sp(0.2),
        h3("Lógica SQL"),
        code("""SELECT
    [Year],
    [Month],
    Subsystem,
    [Type],
    Total,
    Version
FROM {{ ref('fct_sonnell_invoice_totals') }}
WHERE CurrentVersion = CAST(1 AS BIT)  -- solo versión activa
  AND [Type] = 'Regular'               -- líneas de factura normales (MB/TC/MU)
  AND Total < 0                         -- esto no debería existir"""),
        sp(0.2),
        h3("Condición de fallo"),
        body("Retorna filas cuando existe algún total negativo en líneas activas de tipo Regular. "
             "El reporte de fallo incluye Year, Month, Subsystem y el Total incorrecto para facilitar "
             "la investigación."),
        success("ESTADO ACTUAL: PASS — no se encontraron totales negativos en líneas Regular activas."),
        sp(0.4),
    ]))

    # --- Test 2
    story.append(KeepTogether([
        section_box("Test 2: assert_ama_no_future_trips", HexColor("#2C3E50")),
        sp(0.2),
        label("Archivo: tests/assert_ama_no_future_trips.sql  ·  Dominio: AMA  ·  Fase: P0"),
        sp(0.15),
        h3("Propósito"),
        body(
            "Detecta viajes de flota AMA con <code>actual_departure</code> en el futuro. "
            "Un timestamp futuro en datos históricos de Geotab indica corrupción en la fuente "
            "<code>geotab_trip</code>, habitualmente causada por una zona horaria mal configurada "
            "en el dispositivo GPS o un error en el proceso de ingesta. Estos registros distorsionan "
            "métricas de puntualidad y cuentas de viajes del día."
        ),
        sp(0.2),
        h3("Modelo validado"),
        body("<code>AMA_Geotab_Viajes</code> (schema <i>dbo</i>, incremental con ventana de 1 día)"),
        sp(0.2),
        h3("Lógica SQL"),
        code("""SELECT
    trip_id,
    vehicle_name,
    actual_departure,
    GETDATE()                                          AS current_datetime,
    DATEDIFF(MINUTE, GETDATE(), actual_departure)      AS minutes_ahead
FROM {{ ref('AMA_Geotab_Viajes') }}
WHERE actual_departure > GETDATE()"""),
        sp(0.2),
        h3("Condición de fallo"),
        body("Retorna filas cuando algún viaje tiene <code>actual_departure</code> posterior a la fecha/hora actual. "
             "El campo <code>minutes_ahead</code> permite cuantificar el desvío temporal."),
        success("ESTADO ACTUAL: PASS — no se detectaron viajes con timestamp futuro."),
        sp(0.4),
    ]))

    story.append(PageBreak())

    # --- Test 3
    story.append(KeepTogether([
        section_box("Test 3: assert_sonnell_offset_cap_4_percent", HexColor("#2C3E50")),
        sp(0.2),
        label("Archivo: tests/assert_sonnell_offset_cap_4_percent.sql  ·  Dominio: Sonnell  ·  Fase: P1"),
        sp(0.15),
        h3("Propósito"),
        body(
            "Audita la regla de negocio del <b>cap del 4%</b>: el offset (incentivo/penalidad por OTP) "
            "de cualquier subsistema no debe superar el 4% del costo total de ese subsistema en el período. "
            "Según la documentación del proyecto, esta regla es <i>informacional</i> — no bloquea la factura "
            "pero debe registrarse en <code>SonnellOffsetApplied</code>. Este test detecta cuando se supera "
            "el umbral para alertar al equipo de operaciones antes del cierre mensual."
        ),
        sp(0.2),
        h3("Modelo validado"),
        body("<code>fct_sonnell_subsystem_offset</code> (alias: <i>SonnellSubsystemOffset</i>)"),
        sp(0.2),
        h3("Lógica SQL"),
        code("""SELECT
    [Year],
    [Month],
    Subsystem,
    TotalSubsystemCost,
    TotalOffset,
    ROUND(ABS(TotalOffset) / NULLIF(TotalSubsystemCost, 0) * 100, 2) AS offset_pct
FROM {{ ref('fct_sonnell_subsystem_offset') }}
WHERE CurrentVersion = CAST(1 AS BIT)
  AND TotalSubsystemCost > 0
  AND ABS(TotalOffset) / NULLIF(TotalSubsystemCost, 0) > 0.04"""),
        sp(0.2),
        h3("Notas técnicas"),
        bullet("Se usa <code>NULLIF(TotalSubsystemCost, 0)</code> para evitar división por cero en meses sin servicio."),
        bullet("<code>ABS(TotalOffset)</code> cubre tanto incentivos positivos como penalidades negativas."),
        bullet("El campo <code>offset_pct</code> en el resultado permite al equipo ver exactamente cuánto supera el 4%."),
        success("ESTADO ACTUAL: PASS — ningún subsistema activo supera el cap del 4%."),
        sp(0.4),
    ]))

    # --- Test 4
    story.append(KeepTogether([
        section_box("Test 4: assert_sonnell_one_active_version_per_month", HexColor("#2C3E50")),
        sp(0.2),
        label("Archivo: tests/assert_sonnell_one_active_version_per_month.sql  ·  Dominio: Sonnell  ·  Fase: P1"),
        sp(0.15),
        h3("Propósito"),
        body(
            "Valida la integridad del patrón SCD Type 2 implementado en <code>fct_sonnell_invoice_totals</code>. "
            "Para cada combinación de <code>(Year, Month)</code>, debe existir exactamente <b>una sola versión "
            "activa</b> (<code>CurrentVersion = 1</code>). Si existen dos versiones activas para el mismo período, "
            "significa que el <code>pre_hook</code> de SCD Type 2 falló y no marcó la versión anterior como "
            "<code>CurrentVersion = 0</code>, lo que causaría que la factura se calcule doblemente."
        ),
        sp(0.2),
        h3("Modelo validado"),
        body("<code>fct_sonnell_invoice_totals</code>"),
        sp(0.2),
        h3("Lógica SQL"),
        code("""SELECT
    [Year],
    [Month],
    COUNT(DISTINCT Version) AS versiones_activas
FROM {{ ref('fct_sonnell_invoice_totals') }}
WHERE CurrentVersion = CAST(1 AS BIT)
GROUP BY [Year], [Month]
HAVING COUNT(DISTINCT Version) > 1"""),
        sp(0.2),
        h3("Condición de fallo"),
        body("Retorna filas con los períodos donde coexisten múltiples versiones activas. "
             "El campo <code>versiones_activas</code> indica cuántas versiones activas hay para ese mes."),
        success("ESTADO ACTUAL: PASS — cada período tiene exactamente una versión activa."),
        sp(0.4),
    ]))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 4. FRESHNESS
    # ════════════════════════════════════════════════════════════
    story.append(section_box("4. Freshness de Sources — Sonnell", HexColor("#0F3460")))
    story.append(sp(0.3))
    story.append(body(
        "La configuración de <b>freshness</b> en dbt permite detectar automáticamente cuando las tablas "
        "fuente dejaron de recibir datos. A diferencia de los tests, la freshness se ejecuta con "
        "<code>dbt source freshness</code> y consulta directamente las tablas raw del data lake."
    ))
    story.append(sp(0.3))
    story.append(note(
        "La freshness NO corre con 'dbt test'. Comando específico: "
        "dbt source freshness --profiles-dir . --select source:external"
    ))
    story.append(sp(0.4))

    story.append(h2("4.1 Tablas configuradas"))
    story.append(body(
        "Archivo modificado: <code>models/sources/external.yml</code>. "
        "Las tres tablas fuente del pipeline Sonnell ahora tienen freshness habilitada."
    ))
    story.append(sp(0.3))

    fresh_data = [
        ["Tabla fuente", "Modelo consumidor", "Alias dbt", "Campo", "WARN", "ERROR"],
        ["external.sonnell_trip", "stg_sonnell_trip", "SonnellTrips", "_md_processed_at", "2 días", "3 días"],
        ["external.Sonnell_checkpoin", "stg_sonnell_checkpoints", "SonnellCheckpoints", "_md_processed_at", "2 días", "3 días"],
        ["external.sonnell_subsystem", "stg_sonnell_daily_summary", "SonnellDailySummary", "_md_processed_at", "2 días", "3 días"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("thf", fontSize=8.5, textColor=colors.white, fontName="Helvetica-Bold")) for h in fresh_data[0]],
        [[Paragraph(r[i], sCode if i in (0,3) else sBodyLeft) for i in range(6)] for r in fresh_data[1:]],
        col_widths=[3.8*cm, 3.5*cm, 3.2*cm, 3.2*cm, 1.5*cm, 1.5*cm]
    ))
    story.append(sp(0.4))

    story.append(h2("4.2 Lógica de umbrales"))
    story.append(body(
        "El campo <code>_md_processed_at</code> es un DATETIME que registra cuándo el archivo fue "
        "procesado en el data lake. Si el pipeline corre diariamente, este valor tendrá típicamente "
        "entre 12 y 36 horas de antigüedad. Los umbrales se definieron así:"
    ))
    story.append(sp(0.2))

    thresh_data = [
        ["Escenario", "Antigüedad de _md_processed_at", "Resultado"],
        ["Pipeline corrió ayer (normal)", "< 36 horas", "✓ PASS"],
        ["Pipeline no corrió ayer", "36–48 horas (aprox.)", "⚠ WARN — dato de ayer faltante"],
        ["Pipeline no corrió en 2 días", "> 48 horas", "⚠ WARN"],
        ["Pipeline no corrió en 3 días", "> 72 horas", "✗ ERROR — escalamiento requerido"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("tht", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in thresh_data[0]],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft)] for r in thresh_data[1:]],
        col_widths=[5.5*cm, 5*cm, PAGE_W - 2*MARGIN - 10.5*cm]
    ))
    story.append(sp(0.3))
    story.append(note(
        "El umbral warn=2 días (en lugar de 1) existe porque dbt compara "
        "GETDATE() con MAX(_md_processed_at). Si el pipeline corre a las 3am y se ejecuta el "
        "freshness check a las 10am, la diferencia ya es ~31 horas. Con warn=1 día (24h) se "
        "generarían falsos positivos en ejecuciones normales."
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 5. REPORTE DE ESTADO ACTUAL
    # ════════════════════════════════════════════════════════════
    story.append(section_box("5. Reporte de Estado Actual — Ejecución 11/06/2026", C_HIGHLIGHT))
    story.append(sp(0.3))
    story.append(body(
        "Resultado de <code>dbt test --profiles-dir .</code> ejecutado el 11 de junio de 2026 a las 10:27:47 UTC. "
        "Duración total: 3 minutos 9 segundos. 6 threads en paralelo."
    ))
    story.append(sp(0.4))

    # KPI table
    kpi_rep = [
        ["Métrica", "Valor"],
        ["Total de tests", "148"],
        ["Tests pasados (PASS)", "126"],
        ["Tests fallidos (ERROR)", "22"],
        ["Warnings", "0"],
        ["Tasa de éxito", "85.1%"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("thkpi", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in kpi_rep[0]],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1],
          make_style("kpiv", fontSize=10, textColor=C_GREEN if "126" in r[1] or "85" in r[1] else (C_RED if "22" in r[1] else C_TEXT), fontName="Helvetica-Bold")
        )] for r in kpi_rep[1:]],
        col_widths=[8*cm, PAGE_W - 2*MARGIN - 8*cm]
    ))
    story.append(sp(0.5))

    # Errores por dominio
    story.append(h2("5.1 Errores por dominio"))
    err_data = [
        ["Dominio", "Tests fallidos", "Causa", "Impacto", "Acción"],
        ["Monitoring\n(fct_pipeline_health_daily)", "13", "Incompatibilidad técnica:\ndbt_utils.expression_is_true\nno genera T-SQL válido en Synapse\n(Error 8155: sin alias de columna)", "Falso negativo — los datos\nprobablemente están bien,\nno es un problema de datos", "Reemplazar con tests\nsingulares .sql equivalentes"],
        ["ATI Datawarehouse\n(trip, visit, Trip_Sch_Match)", "6", "Datos sucios en fuente\nexternal.trip: NULLs en\ntrip_key y vehicle_day_key.\nCausa duplicados en visit y\nFK huérfanos en cascada.", "Crítico — métricas de viajes,\nvisitas y matcheo GTFS\nno son confiables", "Investigar y limpiar\nexternal.trip en el\ndata lake"],
        ["Sonnell\n(fct_sonnell_subsystem_cost)", "1", "DailySummaryId no es único:\nhay registros duplicados en\nla tabla de costos", "Medio — puede distorsionar\ncálculos de facturación\nsi hay CurrentVersion=1\nduplicados", "Verificar si duplicados\nson de versiones\nhistóricas (CV=0)"],
        ["HMS\n(fct_hms_monthly_trips)", "1", "Columna OTP tiene valores\nno contemplados en\naccepted_values\n['ON_TIME', 'DELAYED']", "Bajo — hay estados de\nviaje no mapeados en\nel modelo", "Consultar valores reales\nde OTP y actualizar\nel test"],
        ["Ejemplo dbt\n(my_first_dbt_model)", "1", "Modelo de ejemplo sin datos", "Ninguno — no es productivo", "Eliminar o ignorar\nel modelo de ejemplo"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("the", fontSize=8.5, textColor=colors.white, fontName="Helvetica-Bold")) for h in err_data[0]],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1], make_style("ec", fontSize=10, textColor=C_RED, fontName="Helvetica-Bold", alignment=TA_CENTER)),
          Paragraph(r[2], sBodyLeft), Paragraph(r[3], sBodyLeft), Paragraph(r[4], sBodyLeft)] for r in err_data[1:]],
        col_widths=[3.5*cm, 1.5*cm, 4*cm, 3.5*cm, 3.7*cm]
    ))

    story.append(sp(0.5))
    story.append(h2("5.2 Tests fallidos — detalle completo"))
    detail_data = [
        ["Test ID (resumido)", "Modelo", "Estado"],
        ["expression_is_true: baseline_7d_stddev >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: error_count >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: error_rate_pct 0-100", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: event_date <= GETDATE()", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: executions_failed >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: executions_skipped >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: executions_success >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: executions_total >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: executions_total = sum partes", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: expected_frequency_hrs >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: health_score 0-100", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: records_failed >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["expression_is_true: records_processed >= 0", "fct_pipeline_health_daily", "ERROR"],
        ["not_null: trip_key", "trip", "ERROR"],
        ["not_null: vehicle_day_key", "trip", "ERROR"],
        ["unique: visit_key", "visit", "ERROR"],
        ["relationships: trip → vehicle_day (vehicle_day_key)", "trip", "ERROR"],
        ["relationships: visit → trip (trip_key)", "visit", "ERROR"],
        ["relationships: Trip_Sch_Match → trip (trip_key)", "Trip_Sch_Match", "ERROR"],
        ["unique: DailySummaryId", "fct_sonnell_subsystem_cost", "ERROR"],
        ["accepted_values: OTP in [ON_TIME, DELAYED]", "fct_hms_monthly_trips", "ERROR"],
        ["not_null: id (ejemplo dbt)", "my_first_dbt_model", "ERROR"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("thd", fontSize=8.5, textColor=colors.white, fontName="Helvetica-Bold")) for h in detail_data[0]],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1], sCode),
          Paragraph(r[2], make_style("es", fontSize=9, textColor=C_RED, fontName="Helvetica-Bold"))] for r in detail_data[1:]],
        col_widths=[8.5*cm, 5*cm, 2.7*cm]
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 6. INSIGHTS Y RECOMENDACIONES
    # ════════════════════════════════════════════════════════════
    story.append(section_box("6. Insights y Recomendaciones", HexColor("#1B4F72")))
    story.append(sp(0.3))

    story.append(h2("6.1 Problema raíz en ATI Datawarehouse (CRÍTICO)"))
    story.append(body(
        "Los 6 errores del dominio ATI tienen un <b>único origen</b>: la tabla fuente "
        "<code>external.trip</code> en el schema <code>raw</code> contiene registros con "
        "<code>trip_key = NULL</code> y <code>vehicle_day_key = NULL</code>. "
        "El modelo <code>trip.sql</code> usa <code>WHERE rn = 1</code> para deduplicar, "
        "pero no filtra NULLs en las claves. Esto genera una cascada de fallos:"
    ))
    story.append(sp(0.2))
    cascade_data = [
        ["Paso", "Entidad", "Problema derivado"],
        ["1", "external.trip (raw)", "Registros con trip_key = NULL ingresan al pipeline"],
        ["2", "trip (mart)", "not_null falla — trip_key y vehicle_day_key tienen NULLs"],
        ["3", "visit (mart)", "FK huérfanos — visits apuntan a trip_keys que no existen como PK válida"],
        ["4", "visit (mart)", "unique falla — NULL en trip_key puede generar visit_keys duplicados"],
        ["5", "Trip_Sch_Match (mart)", "FK huérfanos — matcheo apunta a trip_keys inválidos"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("thc", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold")) for h in cascade_data[0]],
        [[Paragraph(r[0], sBodyLeft), Paragraph(r[1], sCode), Paragraph(r[2], sBodyLeft)] for r in cascade_data[1:]],
        col_widths=[1.5*cm, 4.5*cm, PAGE_W - 2*MARGIN - 6*cm]
    ))
    story.append(sp(0.3))
    story.append(body("<b>Acción recomendada:</b> agregar filtro en el modelo <code>trip.sql</code>:"))
    story.append(code("-- En trip.sql, agregar al WHERE:\nAND trip_key IS NOT NULL\nAND vehicle_day_key IS NOT NULL"))
    story.append(sp(0.2))
    story.append(alert(
        "Investigar también la fuente en el data lake: ¿por qué external.trip tiene registros sin clave? "
        "Puede ser un problema en el proceso de ingesta o en el origen Korbato."
    ))

    story.append(sp(0.4))
    story.append(h2("6.2 Incompatibilidad dbt_utils.expression_is_true con Synapse"))
    story.append(body(
        "El error <i>'No column name was specified for column 1 of dbt_internal_test'</i> (SQL Error 8155) "
        "afecta exclusivamente a los tests <code>dbt_utils.expression_is_true</code>. El macro genera "
        "SQL con una subquery derivada sin alias de columna, lo cual es inválido en SQL Server/Synapse "
        "pero válido en PostgreSQL/Snowflake."
    ))
    story.append(sp(0.2))
    story.append(body("<b>Solución A (recomendada):</b> reemplazar con tests singulares equivalentes. Ejemplo:"))
    story.append(code("""-- En lugar de expression_is_true: health_score >= 0 AND health_score <= 100
-- Crear tests/assert_monitoring_health_score_range.sql:
SELECT event_date, entity_name, health_score
FROM {{ ref('fct_pipeline_health_daily') }}
WHERE health_score < 0 OR health_score > 100"""))
    story.append(sp(0.2))
    story.append(body(
        "<b>Solución B:</b> verificar si una versión más reciente de <code>dbt_utils</code> "
        "corrige la compatibilidad con Synapse y actualizar <code>packages.yml</code>."
    ))

    story.append(sp(0.4))
    story.append(h2("6.3 DailySummaryId duplicado en Sonnell"))
    story.append(body(
        "El test <code>unique: DailySummaryId</code> falla en <code>fct_sonnell_subsystem_cost</code>. "
        "Este modelo usa <code>incremental_strategy='append'</code> con un guard <code>NOT EXISTS</code>. "
        "Los duplicados pueden ser de versiones históricas (<code>CurrentVersion = 0</code>) o, más grave, "
        "de la versión activa."
    ))
    story.append(sp(0.2))
    story.append(body("<b>Diagnóstico recomendado:</b>"))
    story.append(code("""-- Verificar si los duplicados son de versión activa o histórica:
SELECT DailySummaryId, COUNT(*) as cnt, MAX(CurrentVersion) as max_cv
FROM dbo.dbt_SonnellSubsystemCost
GROUP BY DailySummaryId
HAVING COUNT(*) > 1
ORDER BY max_cv DESC"""))
    story.append(sp(0.2))
    story.append(note(
        "Si max_cv = 0 para todos los duplicados: son versiones históricas — el test puede ajustarse "
        "con WHERE CurrentVersion = 1. "
        "Si max_cv = 1: hay duplicados activos — investigar si el pre_hook de SCD Type 2 falló."
    ))

    story.append(sp(0.4))
    story.append(h2("6.4 Valores OTP no contemplados en HMS"))
    story.append(body(
        "El test <code>accepted_values: OTP in ['ON_TIME', 'DELAYED']</code> falla en "
        "<code>fct_hms_monthly_trips</code>. La fuente <code>raw.hms_trips</code> tiene estados "
        "de viaje adicionales que no fueron mapeados en el modelo."
    ))
    story.append(body("<b>Diagnóstico recomendado:</b>"))
    story.append(code("""-- Ver todos los valores reales de OTP:
SELECT OTP, COUNT(*) AS cnt
FROM dbo.HMS_MonthlyDataTrip
GROUP BY OTP
ORDER BY cnt DESC"""))
    story.append(body(
        "Con los valores reales, actualizar el test en <code>models/hms/schema.yml</code> "
        "para incluir todos los valores válidos, o actualizar el modelo para normalizar los estados."
    ))

    story.append(sp(0.5))
    story.append(h2("6.5 Resumen de acciones por prioridad"))
    actions = [
        ["Prioridad", "Acción", "Dominio", "Esfuerzo"],
        ["P0 — URGENTE", "Filtrar NULLs en trip.sql (trip_key IS NOT NULL, vehicle_day_key IS NOT NULL)", "ATI Datawarehouse", "1h"],
        ["P0 — URGENTE", "Investigar origen de NULLs en external.trip en el data lake", "ATI Datawarehouse", "2-4h"],
        ["P1 — IMPORTANTE", "Reemplazar 13 expression_is_true por tests singulares T-SQL", "Monitoring", "3h"],
        ["P1 — IMPORTANTE", "Diagnosticar y corregir DailySummaryId duplicados en Sonnell", "Sonnell", "1-2h"],
        ["P2 — NORMAL", "Actualizar accepted_values de OTP con valores reales de HMS", "HMS", "30min"],
        ["P2 — NORMAL", "Eliminar o ignorar modelo de ejemplo my_first_dbt_model", "General", "15min"],
    ]
    story.append(styled_table(
        [Paragraph(h, make_style("tha", fontSize=8.5, textColor=colors.white, fontName="Helvetica-Bold")) for h in actions[0]],
        [[Paragraph(r[0], make_style("pr", fontSize=9, textColor=C_RED if "P0" in r[0] else (C_ORANGE if "P1" in r[0] else C_BLUE), fontName="Helvetica-Bold")),
          Paragraph(r[1], sBodyLeft), Paragraph(r[2], sBodyLeft), Paragraph(r[3], sBodyLeft)] for r in actions[1:]],
        col_widths=[3*cm, 7.5*cm, 3.5*cm, 2.2*cm]
    ))

    story.append(PageBreak())

    # ════════════════════════════════════════════════════════════
    # 7. COMANDOS DE REFERENCIA
    # ════════════════════════════════════════════════════════════
    story.append(section_box("7. Comandos de Referencia", HexColor("#1A5276")))
    story.append(sp(0.4))

    story.append(h2("Correr todos los tests"))
    story.append(code("dbt test --profiles-dir ."))

    story.append(h2("Por dominio"))
    story.append(code(
        "dbt test --profiles-dir . --select tag:ati_datawarehouse\n"
        "dbt test --profiles-dir . --select tag:sonnell\n"
        "dbt test --profiles-dir . --select tag:dashboard_AMA\n"
        "dbt test --profiles-dir . --select tag:monitoring"
    ))

    story.append(h2("Por tipo de test"))
    story.append(code(
        "dbt test --profiles-dir . --select test_type:singular   # solo tests .sql\n"
        "dbt test --profiles-dir . --select test_type:generic    # solo schema tests"
    ))

    story.append(h2("Modelo específico"))
    story.append(code(
        "dbt test --profiles-dir . --select fleet\n"
        "dbt test --profiles-dir . --select fct_sonnell_invoice_totals\n"
        "dbt test --profiles-dir . --select AMA_Geotab_Viajes"
    ))

    story.append(h2("Freshness (comando separado)"))
    story.append(code(
        "dbt source freshness --profiles-dir . --select source:external  # Sonnell (3 tablas)\n"
        "dbt source freshness --profiles-dir .                           # todas las fuentes"
    ))

    story.append(sp(0.8))
    story.append(hr(C_MED_GRAY, 1))
    story.append(sp(0.2))
    story.append(Paragraph(
        "Documento generado automáticamente por el equipo de Data Engineering — wh_transform v1.0",
        make_style("footer_text", fontSize=8.5, textColor=C_TEXT_LIGHT, alignment=TA_CENTER)
    ))
    story.append(Paragraph(
        "11 de junio de 2026  ·  Azure Synapse Analytics  ·  dbt Core",
        make_style("footer_text2", fontSize=8.5, textColor=C_TEXT_LIGHT, alignment=TA_CENTER)
    ))

    return story, dc


# ── Build PDF ──────────────────────────────────────────────────────────────────

def main():
    story, dc = build_content()
    doc = SimpleDocTemplate(
        OUTPUT,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=1.8*cm,
        bottomMargin=1.4*cm,
        title="Tests dbt — Documento Técnico-Funcional",
        author="wh_transform Data Team",
        subject="Calidad de Datos — dbt Tests",
    )
    doc.build(story, onFirstPage=dc.on_page, onLaterPages=dc.on_page)
    print(f"✓ PDF generado: {OUTPUT}")


if __name__ == "__main__":
    main()
